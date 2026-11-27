import * as fs from 'fs'
import * as path from 'path'
import {PassThrough, Readable} from 'stream'
import {pipeline} from 'stream/promises'
import * as core from '@actions/core'
import * as yauzl from 'yauzl'
import {getUserAgentString} from '../shared/user-agent'

const TAIL_PROBE_BYTES = 65536
const CHUNK_TIMEOUT_MS = 30 * 1000
const MAX_CHUNK_ATTEMPTS = 3

export const DEFAULT_CONCURRENCY = 8
export const DEFAULT_CHUNK_SIZE = 16 * 1024 * 1024 // 16 MiB
export const DEFAULT_WRITE_BUFFER_SIZE = 2 * 1024 * 1024 // 2 MiB per worker
export const DEFAULT_MIN_ARCHIVE_SIZE = 16 * 1024 * 1024 // 16 MiB

export interface RangeExtractOptions {
  concurrency?: number
  chunkSize?: number
  writeBufferSize?: number
  minArchiveSize?: number
}

export function parseContentRange(
  header: string | null
): {start: number; end: number; total: number} | null {
  if (!header) return null
  const m = /^bytes\s+(\d+)-(\d+)\/(\d+)$/i.exec(header.trim())
  if (!m) return null
  const start = Number(m[1])
  const end = Number(m[2])
  const total = Number(m[3])
  if (
    !Number.isSafeInteger(start) ||
    !Number.isSafeInteger(end) ||
    !Number.isSafeInteger(total) ||
    end < start ||
    end >= total
  ) {
    return null
  }
  return {start, end, total}
}

export function resolveSafePath(destDir: string, entryName: string): string {
  const root = path.resolve(destDir)
  const normalized = entryName.replace(/\\/g, '/')
  if (normalized.startsWith('/') || normalized.split('/').includes('..')) {
    throw new Error(`Unsafe zip entry path: ${entryName}`)
  }
  const destPath = path.resolve(root, normalized)
  if (destPath !== root && !destPath.startsWith(`${root}${path.sep}`)) {
    throw new Error(`Unsafe zip entry path: ${entryName}`)
  }
  return destPath
}

function hasProxyEnv(url: string): boolean {
  const proxy = url.startsWith('https:')
    ? process.env['HTTPS_PROXY'] || process.env['https_proxy']
    : process.env['HTTP_PROXY'] || process.env['http_proxy']
  return Boolean(proxy && proxy.trim().length > 0)
}

class HttpRangeReader extends yauzl.RandomAccessReader {
  private readonly url: string
  private readonly tailStart: number
  private readonly tailBuf: Buffer
  private readonly userAgent: string

  constructor(url: string, tailStart: number, tailBuf: Buffer) {
    super()
    this.url = url
    this.tailStart = tailStart
    this.tailBuf = tailBuf
    this.userAgent = getUserAgentString()
  }

  _readStreamForRange(start: number, end: number): Readable {
    if (
      start >= this.tailStart &&
      end <= this.tailStart + this.tailBuf.length
    ) {
      return Readable.from([
        this.tailBuf.subarray(start - this.tailStart, end - this.tailStart)
      ])
    }
    const out = new PassThrough()
    void (async () => {
      try {
        const res = await fetch(this.url, {
          headers: {
            Range: `bytes=${start}-${end - 1}`,
            'User-Agent': this.userAgent
          },
          signal: AbortSignal.timeout(CHUNK_TIMEOUT_MS)
        })
        if (res.status !== 206 && !(start === 0 && res.status === 200)) {
          throw new Error(
            `Unexpected HTTP ${res.status} for range ${start}-${end - 1}`
          )
        }
        if (!res.body) throw new Error('Empty response body')
        const reader = res.body.getReader()
        let remaining = end - start
        while (remaining > 0) {
          const {done, value} = await reader.read()
          if (done) break
          const chunk =
            value.byteLength > remaining ? value.subarray(0, remaining) : value
          remaining -= chunk.byteLength
          if (!out.write(chunk)) {
            await new Promise<void>(resolve => out.once('drain', resolve))
          }
        }
        out.end()
      } catch (err) {
        out.destroy(err instanceof Error ? err : new Error(String(err)))
      }
    })()
    return out
  }

  close(callback: (err: Error | null) => void): void {
    callback(null)
  }
}

async function writeFull(
  fh: fs.promises.FileHandle,
  buf: Buffer,
  len: number,
  fileOffset: number
): Promise<void> {
  let written = 0
  while (written < len) {
    const {bytesWritten} = await fh.write(
      buf,
      written,
      len - written,
      fileOffset + written
    )
    if (bytesWritten <= 0) throw new Error('Short write')
    written += bytesWritten
  }
}

async function downloadSlice(
  url: string,
  fh: fs.promises.FileHandle,
  srcStart: number,
  dstOffset: number,
  len: number,
  buf: Buffer,
  userAgent: string
): Promise<void> {
  const range = `bytes=${srcStart}-${srcStart + len - 1}`
  for (let attempt = 1; attempt <= MAX_CHUNK_ATTEMPTS; attempt++) {
    try {
      const res = await fetch(url, {
        headers: {Range: range, 'User-Agent': userAgent},
        signal: AbortSignal.timeout(CHUNK_TIMEOUT_MS)
      })
      if (res.status !== 206 && !(srcStart === 0 && res.status === 200)) {
        await res.body?.cancel()
        throw new Error(`Unexpected HTTP ${res.status} for range ${range}`)
      }
      if (!res.body) throw new Error(`Empty body for range ${range}`)

      const reader = res.body.getReader()
      let received = 0
      let pos = 0
      let writeOff = dstOffset
      while (true) {
        const {done, value} = await reader.read()
        if (done) break
        const rem = len - received
        if (rem <= 0) {
          await reader.cancel()
          break
        }
        const chunk = value.byteLength > rem ? value.subarray(0, rem) : value
        received += chunk.byteLength
        let srcOff = 0
        while (srcOff < chunk.byteLength) {
          const take = Math.min(buf.length - pos, chunk.byteLength - srcOff)
          buf.set(chunk.subarray(srcOff, srcOff + take), pos)
          pos += take
          srcOff += take
          if (pos === buf.length) {
            await writeFull(fh, buf, pos, writeOff)
            writeOff += pos
            pos = 0
          }
        }
      }
      if (pos > 0) await writeFull(fh, buf, pos, writeOff)
      if (received !== len) {
        throw new Error(`Short range read: got ${received}, expected ${len}`)
      }
      return
    } catch (err) {
      if (attempt >= MAX_CHUNK_ATTEMPTS) throw err
      core.debug(`Range ${range} attempt ${attempt} failed: ${err.message}`)
      await new Promise(resolve => setTimeout(resolve, 250 * attempt))
    }
  }
}

async function extractStoreParallel(
  url: string,
  dataStart: number,
  totalSize: number,
  destPath: string,
  concurrency: number,
  chunkSize: number,
  writeBufferSize: number
): Promise<void> {
  const fh = await fs.promises.open(destPath, 'w+')
  try {
    if (totalSize > 0) await fh.truncate(totalSize)
    if (totalSize === 0) return

    const tasks: {srcStart: number; dstOffset: number; len: number}[] = []
    for (let off = 0; off < totalSize; off += chunkSize) {
      tasks.push({
        srcStart: dataStart + off,
        dstOffset: off,
        len: Math.min(chunkSize, totalSize - off)
      })
    }

    const userAgent = getUserAgentString()
    const bufSize = Math.min(writeBufferSize, chunkSize)
    let nextIdx = 0
    const worker = async (): Promise<void> => {
      const buf = Buffer.allocUnsafe(bufSize)
      while (nextIdx < tasks.length) {
        const t = tasks[nextIdx++]
        await downloadSlice(
          url,
          fh,
          t.srcStart,
          t.dstOffset,
          t.len,
          buf,
          userAgent
        )
      }
    }
    const lanes = Math.min(concurrency, tasks.length)
    await Promise.all(Array.from({length: lanes}, worker))
  } finally {
    await fh.close()
  }
}

/**
 * Attempts fast parallel HTTP Range extraction via a single suffix Range GET
 * and `yauzl` Central Directory parsing. Returns `false` if the server or
 * archive does not qualify so the caller falls back to streaming extraction.
 */
export async function tryRangeExtract(
  url: string,
  directory: string,
  options: RangeExtractOptions = {}
): Promise<boolean> {
  if (hasProxyEnv(url)) return false

  const concurrency = Math.max(1, options.concurrency ?? DEFAULT_CONCURRENCY)
  const chunkSize = Math.max(64 * 1024, options.chunkSize ?? DEFAULT_CHUNK_SIZE)
  const writeBufferSize = Math.max(
    64 * 1024,
    options.writeBufferSize ?? DEFAULT_WRITE_BUFFER_SIZE
  )
  const minArchiveSize = options.minArchiveSize ?? DEFAULT_MIN_ARCHIVE_SIZE

  // Single suffix Range GET (no HEAD — presigned GET URLs often reject HEAD)
  let tailRes: Response
  try {
    tailRes = await fetch(url, {
      headers: {
        Range: `bytes=-${TAIL_PROBE_BYTES}`,
        'User-Agent': getUserAgentString()
      },
      signal: AbortSignal.timeout(CHUNK_TIMEOUT_MS)
    })
  } catch (err) {
    core.debug(`Range suffix probe failed (${err.message}); falling back`)
    return false
  }

  if (tailRes.status !== 206) {
    await tailRes.body?.cancel()
    return false
  }
  const cr = parseContentRange(tailRes.headers.get('content-range'))
  if (!cr || cr.total < minArchiveSize) {
    await tailRes.body?.cancel()
    return false
  }
  const resolvedUrl = tailRes.url || url
  if (url.startsWith('https:') && !resolvedUrl.startsWith('https:')) {
    await tailRes.body?.cancel()
    return false
  }
  const tailBuf = Buffer.from(await tailRes.arrayBuffer())

  const reader = new HttpRangeReader(resolvedUrl, cr.start, tailBuf)
  let zipfile: yauzl.ZipFile
  try {
    zipfile = await yauzl.fromRandomAccessReaderPromise(reader, cr.total, {
      lazyEntries: true,
      autoClose: false
    })
  } catch (err) {
    core.debug(`yauzl failed to parse ZIP CD (${err.message}); falling back`)
    return false
  }

  try {
    const entries: yauzl.Entry[] = []
    for await (const entry of zipfile.eachEntry()) {
      entries.push(entry)
    }

    // Validate entries before touching disk
    for (const entry of entries) {
      if (entry.isEncrypted() || !entry.canDecodeFileData()) {
        return false
      }
      resolveSafePath(directory, entry.fileName)
    }

    core.debug(
      `Using parallel range extraction (${entries.length} entries, ${cr.total} bytes, concurrency=${concurrency})`
    )

    for (const entry of entries) {
      const destPath = resolveSafePath(directory, entry.fileName)
      if (entry.fileName.endsWith('/')) {
        await fs.promises.mkdir(destPath, {recursive: true})
        continue
      }
      await fs.promises.mkdir(path.dirname(destPath), {recursive: true})

      if (
        entry.compressionMethod === 0 &&
        entry.uncompressedSize >= minArchiveSize
      ) {
        const {fileDataStart} = await zipfile.readLocalFileHeaderPromise(
          entry,
          {minimal: true}
        )
        await extractStoreParallel(
          resolvedUrl,
          fileDataStart,
          entry.uncompressedSize,
          destPath,
          concurrency,
          chunkSize,
          writeBufferSize
        )
      } else {
        const readStream = await zipfile.openReadStreamPromise(entry)
        await pipeline(readStream, fs.createWriteStream(destPath))
      }
    }
    return true
  } finally {
    zipfile.close()
  }
}
