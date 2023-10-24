import * as core from '@actions/core'
import {HttpClient} from '@actions/http-client'
import {BearerCredentialHandler} from '@actions/http-client/lib/auth'
import {
  RequestOptions,
  TypedResponse
} from '@actions/http-client/lib/interfaces'
import * as crypto from 'crypto'
import * as fs from 'fs'

import * as utils from './cacheUtils'
import {CompressionMethod} from './constants'
import {
  ArtifactCacheEntry,
  InternalCacheOptions,
  CommitCacheRequest,
  ReserveCacheRequest,
  ReserveCacheResponse,
  ITypedResponseWithError,
  ArtifactCacheList
} from './contracts'
import {downloadCacheHttpClient, downloadCacheConcurrent} from './downloadUtils'
import {
  DownloadOptions,
  UploadOptions,
  getDownloadOptions,
  getUploadOptions
} from '../options'
import {
  isSuccessStatusCode,
  retryHttpClientResponse,
  retryTypedResponse
} from './requestUtils'

const versionSalt = '1.0'

function getCacheApiUrl(resource: string): string {
  const baseUrl: string =
    process.env['NAMESPACE_CACHE_URL'] ||
    'https://cache.github-services.fra2.nscluster.cloud/'
  if (!baseUrl) {
    throw new Error('Cache Service Url not found, unable to restore cache.')
  }

  const url = `${baseUrl}_apis/artifactcache/${resource}`
  core.debug(`Resource Url: ${url}`)
  return url
}

function createAcceptHeader(type: string, apiVersion: string): string {
  return `${type};api-version=${apiVersion}`
}

function getRequestOptions(): RequestOptions {
  const requestOptions: RequestOptions = {
    headers: {
      Accept: createAcceptHeader('application/json', '6.0-preview.1')
    }
  }

  return requestOptions
}

function createHttpClient(): HttpClient {
  if (process.env['NSC_TOKEN_FILE'] == null) {
    throw new Error(`Missing $NSC_TOKEN_FILE`)
  }

  const tokenJSON = fs.readFileSync(process.env['NSC_TOKEN_FILE'], 'utf8')
  const token = JSON.parse(tokenJSON).bearer_token

  const bearerCredentialHandler = new BearerCredentialHandler(token)

  return new HttpClient(
    'actions/cache',
    [bearerCredentialHandler],
    getRequestOptions()
  )
}

export function getCacheVersion(
  paths: string[],
  compressionMethod?: CompressionMethod,
  enableCrossOsArchive = false
): string {
  const components = paths

  // Add compression method to cache version to restore
  // compressed cache as per compression method
  if (compressionMethod) {
    components.push(compressionMethod)
  }

  // Only check for windows platforms if enableCrossOsArchive is false
  if (process.platform === 'win32' && !enableCrossOsArchive) {
    components.push('windows-only')
  }

  // Add salt to cache version to support breaking changes in cache entry
  components.push(versionSalt)

  return crypto
    .createHash('sha256')
    .update(components.join('|'))
    .digest('hex')
}

export async function getCacheEntry(
  keys: string[],
  paths: string[],
  options?: InternalCacheOptions
): Promise<ArtifactCacheEntry | null> {
  const httpClient = createHttpClient()
  const version = getCacheVersion(
    paths,
    options?.compressionMethod,
    options?.enableCrossOsArchive
  )
  const resource = `cache?keys=${encodeURIComponent(
    keys.join(',')
  )}&version=${version}`

  const response = await retryTypedResponse('getCacheEntry', async () =>
    httpClient.getJson<ArtifactCacheEntry>(getCacheApiUrl(resource))
  )
  // Cache not found
  if (response.statusCode === 204) {
    // List cache for primary key only if cache miss occurs
    if (core.isDebug()) {
      await printCachesListForDiagnostics(keys[0], httpClient, version)
    }
    return null
  }
  if (!isSuccessStatusCode(response.statusCode)) {
    throw new Error(`Cache service responded with ${response.statusCode}`)
  }

  const cacheResult = response.result
  const cacheDownloadUrl = cacheResult?.archiveLocation
  if (!cacheDownloadUrl) {
    // Cache achiveLocation not found. This should never happen, and hence bail out.
    throw new Error('Cache not found.')
  }
  core.setSecret(cacheDownloadUrl)
  core.debug(`Cache Result:`)
  core.debug(JSON.stringify(cacheResult))

  return cacheResult
}

async function printCachesListForDiagnostics(
  key: string,
  httpClient: HttpClient,
  version: string
): Promise<void> {
  const resource = `caches?key=${encodeURIComponent(key)}`
  const response = await retryTypedResponse('listCache', async () =>
    httpClient.getJson<ArtifactCacheList>(getCacheApiUrl(resource))
  )
  if (response.statusCode === 200) {
    const cacheListResult = response.result
    const totalCount = cacheListResult?.totalCount
    if (totalCount && totalCount > 0) {
      core.debug(
        `No matching cache found for cache key '${key}', version '${version} and scope ${process.env['GITHUB_REF']}. There exist one or more cache(s) with similar key but they have different version or scope. See more info on cache matching here: https://docs.github.com/en/actions/using-workflows/caching-dependencies-to-speed-up-workflows#matching-a-cache-key \nOther caches with similar key:`
      )
      for (const cacheEntry of cacheListResult?.artifactCaches || []) {
        core.debug(
          `Cache Key: ${cacheEntry?.cacheKey}, Cache Version: ${cacheEntry?.cacheVersion}, Cache Scope: ${cacheEntry?.scope}, Cache Created: ${cacheEntry?.creationTime}`
        )
      }
    }
  }
}

export async function downloadCache(
  archiveLocation: string,
  archivePath: string,
  archiveSize: number,
  options?: DownloadOptions
): Promise<void> {
  const downloadOptions = getDownloadOptions(options)

  if (downloadOptions.downloadConcurrency! > 1) {
    await downloadCacheConcurrent(
      archiveLocation,
      archivePath,
      archiveSize,
      downloadOptions.downloadConcurrency!
    )
  } else {
    // Otherwise, download using the Actions http-client.
    await downloadCacheHttpClient(archiveLocation, archivePath)
  }
}

// Reserve Cache
export async function reserveCache(
  key: string,
  paths: string[],
  options?: InternalCacheOptions
): Promise<ITypedResponseWithError<ReserveCacheResponse>> {
  const httpClient = createHttpClient()
  const version = getCacheVersion(
    paths,
    options?.compressionMethod,
    options?.enableCrossOsArchive
  )

  const reserveCacheRequest: ReserveCacheRequest = {
    key,
    version,
    cacheSize: options?.cacheSize
  }
  const response = await retryTypedResponse('reserveCache', async () =>
    httpClient.postJson<ReserveCacheResponse>(
      getCacheApiUrl('caches'),
      reserveCacheRequest
    )
  )
  return response
}

function getContentRange(start: number, end: number): string {
  // Format: `bytes start-end/filesize
  // start and end are inclusive
  // filesize can be *
  // For a 200 byte chunk starting at byte 0:
  // Content-Range: bytes 0-199/*
  return `bytes ${start}-${end}/*`
}

async function uploadChunk(
  httpClient: HttpClient,
  resourceUrl: string,
  openStream: () => NodeJS.ReadableStream,
  start: number,
  end: number,
  chunkNo: number,
  signal: AbortSignal
): Promise<string> {
  core.debug(
    `Uploading chunk of size ${end -
      start +
      1} bytes at offset ${start} with content range: ${getContentRange(
      start,
      end
    )}`
  )
  const additionalHeaders = {
    'Content-Type': 'application/octet-stream',
    'Content-Length': end - start + 1, // end is inclusive
    'Content-Range': getContentRange(start, end),
    'NS-Chunk-No': chunkNo.toString()
  }

  const uploadChunkResponse = await retryHttpClientResponse(
    `uploadChunk (start: ${start}, end: ${end})`,
    async () =>
      httpClient.sendStream(
        'PATCH',
        resourceUrl,
        openStream(),
        additionalHeaders
      ),
    {signal}
  )
  // Avoid leaking the response handle.
  // Otherwise this connection keeps the process hanging after completion.
  await uploadChunkResponse.readBody()

  if (!isSuccessStatusCode(uploadChunkResponse.message.statusCode)) {
    throw new Error(
      `Cache service responded with ${uploadChunkResponse.message.statusCode} during upload chunk.`
    )
  }
  if (uploadChunkResponse.message.headers.etag == null) {
    throw new Error(`Cache service response misses ETag during upload chunk.`)
  }
  return uploadChunkResponse.message.headers.etag
}

async function uploadFile(
  httpClient: HttpClient,
  cacheId: string,
  archivePath: string,
  options?: UploadOptions
): Promise<string[]> {
  // Upload Chunks
  const fileSize = utils.getArchiveFileSizeInBytes(archivePath)
  const resourceUrl = getCacheApiUrl(`caches/${cacheId.toString()}`)
  const fd = fs.openSync(archivePath, 'r')
  const uploadOptions = getUploadOptions(options)

  const concurrency = utils.assertDefined(
    'uploadConcurrency',
    uploadOptions.uploadConcurrency
  )
  const maxChunkSize = utils.assertDefined(
    'uploadChunkSize',
    uploadOptions.uploadChunkSize
  )

  const abort = new AbortController()
  const parallelUploads = [...new Array(concurrency).keys()]
  core.debug('Awaiting all uploads')
  let offset = 0,
    nextChunk = 0
  const etags: string[] = []

  const uploads = parallelUploads.map(async () => {
    while (offset < fileSize) {
      const chunkSize = Math.min(fileSize - offset, maxChunkSize)
      const start = offset
      const end = offset + chunkSize - 1
      const chunkNo = nextChunk
      offset += maxChunkSize
      nextChunk += 1

      const etag = await uploadChunk(
        httpClient,
        resourceUrl,
        () =>
          fs
            .createReadStream(archivePath, {
              fd,
              start,
              end,
              autoClose: false
            })
            .on('error', error => {
              throw new Error(
                `Cache upload failed because file read failed with ${error.message}`
              )
            }),
        start,
        end,
        chunkNo,
        abort.signal
      )
      etags[chunkNo] = etag
    }
  })

  // Only close fd once HTTP communication finishes.
  // Closing the file confuses httpClient (it doesn't abort the requests and they hang).
  Promise.allSettled(uploads).then(() => {
    fs.closeSync(fd)
  })

  try {
    await Promise.all(uploads)
  } finally {
    abort.abort()
  }
  return etags
}

async function uploadChunkToUrl(
  httpClient: HttpClient,
  resourceUrl: string,
  chunkNo: number,
  length: number,
  openStream: () => NodeJS.ReadableStream,
  signal: AbortSignal
): Promise<string> {
  const additionalHeaders = {
    'Content-Type': 'application/octet-stream',
    'Content-Length': length
  }

  const uploadChunkResponse = await retryHttpClientResponse(
    `uploadChunk #${chunkNo}`,
    async () =>
      httpClient.sendStream(
        'PUT',
        resourceUrl,
        openStream(),
        additionalHeaders
      ),
    {signal}
  )
  // Avoid leaking the response handle.
  // Otherwise this connection keeps the process hanging after completion.
  await uploadChunkResponse.readBody()

  if (!isSuccessStatusCode(uploadChunkResponse.message.statusCode)) {
    throw new Error(
      `Cache service responded with ${uploadChunkResponse.message.statusCode} during upload chunk.`
    )
  }
  if (uploadChunkResponse.message.headers.etag == null) {
    throw new Error(`Cache service response misses ETag during upload chunk.`)
  }
  return uploadChunkResponse.message.headers.etag
}

async function uploadFileToUrls(
  uploadUrls: string[],
  archivePath: string,
  options?: UploadOptions
): Promise<string[]> {
  const httpClient = new HttpClient('actions/cache')
  const uploadOptions = getUploadOptions(options)
  const concurrency = utils.assertDefined(
    'uploadConcurrency',
    uploadOptions.uploadConcurrency
  )
  const preferredChunkSize = utils.assertDefined(
    'uploadChunkSize',
    uploadOptions.uploadChunkSize
  )

  const fileSize = utils.getArchiveFileSizeInBytes(archivePath)
  const fd = fs.openSync(archivePath, 'r')

  const parallelUploads = [...new Array(concurrency).keys()]
  core.debug('Awaiting all uploads')
  const maxChunkSize = Math.max(
    preferredChunkSize,
    Math.ceil(fileSize / uploadUrls.length)
  )

  const abort = new AbortController()
  let offset = 0,
    nextChunk = 0
  const etags: string[] = []
  const uploads = parallelUploads.map(async () => {
    while (offset < fileSize) {
      const chunkNo = nextChunk
      const chunkUrl = uploadUrls[chunkNo]
      const chunkSize = Math.min(fileSize - offset, maxChunkSize)
      const start = offset
      const end = offset + chunkSize - 1
      offset += maxChunkSize
      nextChunk += 1

      core.debug(`Uploading chunk ${chunkNo} (${start}-${end}) to ${chunkUrl}`)

      const etag = await uploadChunkToUrl(
        httpClient,
        chunkUrl,
        chunkNo,
        chunkSize,
        () =>
          fs
            .createReadStream(archivePath, {
              fd,
              start,
              end,
              autoClose: false
            })
            .on('error', error => {
              throw new Error(
                `Cache upload failed because file read failed with ${error.message} ${error}`
              )
            }),
        abort.signal
      )
      etags[chunkNo] = etag
    }
  })

  Promise.allSettled(uploads).then(() => {
    fs.closeSync(fd)
  })
  try {
    await Promise.all(uploads)
  } finally {
    // Store retrying. HTTP client is not abortable.
    abort.abort()
  }
  return etags
}

async function commitCache(
  httpClient: HttpClient,
  cacheId: string,
  filesize: number,
  etags: string[]
): Promise<TypedResponse<null>> {
  const commitCacheRequest: CommitCacheRequest = {size: filesize, etags}
  return await retryTypedResponse('commitCache', async () =>
    httpClient.postJson<null>(
      getCacheApiUrl(`caches/${cacheId.toString()}`),
      commitCacheRequest
    )
  )
}

export async function saveCache(
  cacheId: string,
  archivePath: string,
  uploadUrls?: string[],
  options?: UploadOptions
): Promise<void> {
  const httpClient = createHttpClient()

  core.debug('Upload cache')
  const etags = uploadUrls?.length
    ? await uploadFileToUrls(uploadUrls, archivePath, options)
    : await uploadFile(httpClient, cacheId, archivePath, options)

  // Commit Cache
  core.debug('Commiting cache')
  const cacheSize = utils.getArchiveFileSizeInBytes(archivePath)
  core.info(
    `Cache Size: ~${Math.round(cacheSize / (1024 * 1024))} MB (${cacheSize} B)`
  )

  const commitCacheResponse = await commitCache(
    httpClient,
    cacheId,
    cacheSize,
    etags
  )
  if (!isSuccessStatusCode(commitCacheResponse.statusCode)) {
    throw new Error(
      `Cache service responded with ${commitCacheResponse.statusCode} during commit cache.`
    )
  }

  core.info('Cache saved successfully')
}
