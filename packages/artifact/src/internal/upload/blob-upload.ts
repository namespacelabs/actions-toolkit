import {BlobClient, BlockBlobUploadStreamOptions} from '@azure/storage-blob'
import {TransferProgressEvent} from '@azure/core-http'
import {HttpClient} from '@actions/http-client'
import {ZipUploadStream} from './zip'
import {getUploadChunkSize, getConcurrency, directZipUpload} from '../shared/config'
import * as core from '@actions/core'
import * as crypto from 'crypto'
import * as stream from 'stream'
import * as streamPromises from 'stream/promises'
import * as fs from 'fs'
import {NetworkError} from '../shared/errors'

export interface BlobUploadResponse {
  /**
   * The total reported upload size in bytes. Empty if the upload failed
   */
  uploadSize?: number

  /**
   * The SHA256 hash of the uploaded file. Empty if the upload failed
   */
  sha256Hash?: string

  /**
   * Etag header returned from upload service.
   */
  uploadEtag?: string
}

export async function uploadZipToBlobStorage(
  authenticatedUploadURL: string,
  zipUploadStream: ZipUploadStream
): Promise<BlobUploadResponse> {
  let uploadByteCount = 0
  let uploadEtag: string | undefined

  const maxConcurrency = getConcurrency()
  const bufferSize = getUploadChunkSize()
  const blobClient = new BlobClient(authenticatedUploadURL)
  const blockBlobClient = blobClient.getBlockBlobClient()

  core.debug(
    `Uploading artifact zip to blob storage with maxConcurrency: ${maxConcurrency}, bufferSize: ${bufferSize}`
  )

  const uploadCallback = (progress: TransferProgressEvent): void => {
    core.info(`Uploaded bytes ${progress.loadedBytes}`)
    uploadByteCount = progress.loadedBytes
  }

  const options: BlockBlobUploadStreamOptions = {
    blobHTTPHeaders: {blobContentType: 'zip'},
    onProgress: uploadCallback
  }

  let sha256Hash: string | undefined = undefined
  const uploadStream = new stream.PassThrough()
  const hashStream = crypto.createHash('sha256')

  zipUploadStream.pipe(uploadStream) // This stream is used for the upload
  zipUploadStream.pipe(hashStream).setEncoding('hex') // This stream is used to compute a hash of the zip content that gets used. Integrity check

  core.info('Beginning upload of artifact content to blob storage')

  if (directZipUpload()) {
    const tempDirectory = process.env['RUNNER_TEMP']
    if (!tempDirectory) {
      throw new Error('Unable to get the RUNNER_TEMP env variable')
    }
  
    const dir = fs.mkdtempSync(tempDirectory)
    const tempFile = dir + '/data.zip'

    await streamPromises.pipeline(uploadStream, fs.createWriteStream(tempFile))

    const stat = fs.statSync(tempFile)
    uploadByteCount = stat.size

    const httpClient = new HttpClient(
      'actions/artifact',
    )

    const res = await httpClient.sendStream(
      'PUT',
      authenticatedUploadURL,
      fs.createReadStream(tempFile),
      {
        "Content-Length": stat.size
      },
    )

    if (!res.message.statusCode || res.message.statusCode < 200 || res.message.statusCode >= 300) {
      throw new Error(
        `Service responded with ${res.message.statusCode} during artifact upload.`
      )
    }
    uploadEtag = res.message.headers['etag']

    // Make sure to relese resources.
    await res.readBody()
    httpClient.dispose()
  } else {
    try {
      await blockBlobClient.uploadStream(
        uploadStream,
        bufferSize,
        maxConcurrency,
        options
      )
    } catch (error) {
      if (NetworkError.isNetworkErrorCode(error?.code)) {
        throw new NetworkError(error?.code)
      }

      throw error
    }
  }

  core.info('Finished uploading artifact content to blob storage!')

  hashStream.end()
  sha256Hash = hashStream.read() as string
  core.info(`SHA256 hash of uploaded artifact zip is ${sha256Hash}`)

  if (uploadByteCount === 0) {
    core.warning(
      `No data was uploaded to blob storage. Reported upload byte count is 0.`
    )
  }
  return {
    uploadSize: uploadByteCount,
    sha256Hash,
    uploadEtag
  }
}
