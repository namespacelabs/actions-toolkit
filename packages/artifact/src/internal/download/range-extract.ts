import * as fs from 'fs'
import * as path from 'path'
import {Readable, Transform} from 'stream'
import {pipeline} from 'stream/promises'
import * as zlib from 'zlib'
import * as core from '@actions/core'
import * as yauzl from 'yauzl'
import {getUserAgentString} from '../shared/user-agent'

// yauzl looks for the end of central directory record, its comment and the
// ZIP64 locator in the last 22 + 0xffff + 20 bytes. Probing exactly that much
// serves its first read from memory.
const TAIL_PROBE_BYTES = 22 + 0xffff + 20
const MAX_REDIRECTS = 10
const MAX_SLICE_ATTEMPTS = 3

export const DEFAULT_CONCURRENCY = 8
export const DEFAULT_CHUNK_SIZE = 16 * 1024 * 1024 // 16 MiB
export const DEFAULT_WRITE_BUFFER_SIZE = 2 * 1024 * 1024 // 2 MiB per worker
export const DEFAULT_MIN_ARCHIVE_SIZE = 16 * 1024 * 1024 // 16 MiB
// Each entry costs serial requests for its central directory record, local
// header and data. Above a few dozen entries one stream is faster.
export const DEFAULT_MAX_ENTRIES = 64
export const DEFAULT_IDLE_TIMEOUT_MS = 30 * 1000

export interface RangeExtractOptions {
  concurrency?: number
  chunkSize?: number
  writeBufferSize?: number
  minArchiveSize?: number
  maxEntries?: number
  idleTimeoutMs?: number
}

type Crc32 = (data: Uint8Array, value?: number) => number
// Node 20.15 and 22.2 added zlib.crc32. @types/node here predates it.
const crc32 = (zlib as unknown as {crc32?: Crc32}).crc32

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
  const prefix = root.endsWith(path.sep) ? root : `${root}${path.sep}`
  if (destPath !== root && !destPath.startsWith(prefix)) {
    throw new Error(`Unsafe zip entry path: ${entryName}`)
  }
  return destPath
}

// The URL a redirect leads to, or null when it would leave HTTPS.
export function redirectTarget(from: string, location: string): string | null {
  const next = new URL(location, from)
  if (new URL(from).protocol === 'https:' && next.protocol !== 'https:') {
    return null
  }
  return next.href
}

// CRC-32 of A || B from crc(A), crc(B) and len(B), as zlib's crc32_combine.
const CRC_POLY = 0xedb88320

function multModP(a: number, b: number): number {
  let m = 0x80000000
  let p = 0
  for (;;) {
    if (a & m) {
      p ^= b
      if ((a & (m - 1)) === 0) break
    }
    m = m >>> 1
    b = b & 1 ? (b >>> 1) ^ CRC_POLY : b >>> 1
  }
  return p >>> 0
}

const X2N_TABLE: number[] = (() => {
  const table: number[] = []
  let p = 1 << 30 // x^1
  table.push(p)
  for (let n = 1; n < 32; n++) {
    p = multModP(p, p)
    table.push(p)
  }
  return table
})()

export function crc32Combine(crc1: number, crc2: number, len2: number): number {
  // x^(8 * len2) mod P
  let p = 0x80000000 // x^0
  let k = 3
  for (let n = len2; n > 0; n = Math.floor(n / 2), k++) {
    if (n % 2) p = multModP(X2N_TABLE[k & 31], p)
  }
  return (multModP(p, crc1) ^ crc2) >>> 0
}

function hasProxyEnv(url: string): boolean {
  const proxy = url.startsWith('https:')
    ? process.env['HTTPS_PROXY'] || process.env['https_proxy']
    : process.env['HTTP_PROXY'] || process.env['http_proxy']
  return Boolean(proxy && proxy.trim().length > 0)
}

interface RangeRequest {
  url: string
  start: number
  end: number // exclusive
  total: number
  idleTimeoutMs: number
  signal?: AbortSignal
}

// Yields the bytes of one range. It fails unless the server answers 206 with
// exactly the requested Content-Range of an object of the probed size, and
// it aborts when no data arrives for idleTimeoutMs.
async function* rangeBody(req: RangeRequest): AsyncGenerator<Buffer> {
  const range = `bytes=${req.start}-${req.end - 1}`
  const controller = new AbortController()
  const onAbort = (): void => controller.abort(req.signal?.reason)
  if (req.signal?.aborted) throw req.signal.reason
  req.signal?.addEventListener('abort', onAbort, {once: true})
  const timer = setTimeout(
    () =>
      controller.abort(
        new Error(`No data for ${req.idleTimeoutMs} ms on range ${range}`)
      ),
    req.idleTimeoutMs
  )
  let reader: ReadableStreamDefaultReader<Uint8Array> | undefined
  try {
    const res = await fetch(req.url, {
      headers: {Range: range, 'User-Agent': getUserAgentString()},
      redirect: 'error',
      signal: controller.signal
    })
    reader = res.body?.getReader()
    const header = res.headers.get('content-range')
    const cr = parseContentRange(header)
    if (
      res.status !== 206 ||
      !cr ||
      cr.start !== req.start ||
      cr.end !== req.end - 1 ||
      cr.total !== req.total
    ) {
      throw new Error(
        `Unexpected response for range ${range}: HTTP ${res.status}, Content-Range ${header}`
      )
    }
    if (!reader) throw new Error(`Empty body for range ${range}`)

    const want = req.end - req.start
    let received = 0
    for (;;) {
      timer.refresh()
      const {done, value} = await reader.read()
      if (done) break
      received += value.byteLength
      if (received > want) {
        throw new Error(`Range ${range} returned more than ${want} bytes`)
      }
      yield Buffer.from(value.buffer, value.byteOffset, value.byteLength)
    }
    if (received !== want) {
      throw new Error(`Range ${range} returned ${received} of ${want} bytes`)
    }
    reader = undefined
  } finally {
    clearTimeout(timer)
    req.signal?.removeEventListener('abort', onAbort)
    if (reader) await reader.cancel().catch(() => {})
  }
}

class HttpRangeReader extends yauzl.RandomAccessReader {
  constructor(
    private readonly url: string,
    private readonly total: number,
    private readonly tailStart: number,
    private readonly tail: Buffer,
    private readonly idleTimeoutMs: number
  ) {
    super()
  }

  _readStreamForRange(start: number, end: number): Readable {
    if (start >= this.tailStart) {
      return Readable.from([
        this.tail.subarray(start - this.tailStart, end - this.tailStart)
      ])
    }
    return Readable.from(
      rangeBody({
        url: this.url,
        start,
        end,
        total: this.total,
        idleTimeoutMs: this.idleTimeoutMs
      }),
      {objectMode: false}
    )
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

interface Slice {
  srcStart: number
  dstOffset: number
  len: number
}

// Downloads one slice into fh through buf and returns its CRC-32.
async function downloadSlice(
  url: string,
  total: number,
  fh: fs.promises.FileHandle,
  slice: Slice,
  buf: Buffer,
  idleTimeoutMs: number,
  signal: AbortSignal
): Promise<number> {
  for (let attempt = 1; ; attempt++) {
    try {
      let crc = 0
      let pos = 0
      let writeOff = slice.dstOffset
      for await (const chunk of rangeBody({
        url,
        start: slice.srcStart,
        end: slice.srcStart + slice.len,
        total,
        idleTimeoutMs,
        signal
      })) {
        crc = (crc32 as Crc32)(chunk, crc)
        let off = 0
        while (off < chunk.length) {
          const take = Math.min(buf.length - pos, chunk.length - off)
          chunk.copy(buf, pos, off, off + take)
          pos += take
          off += take
          if (pos === buf.length) {
            await writeFull(fh, buf, pos, writeOff)
            writeOff += pos
            pos = 0
          }
        }
      }
      if (pos > 0) await writeFull(fh, buf, pos, writeOff)
      return crc
    } catch (err) {
      if (signal.aborted || attempt >= MAX_SLICE_ATTEMPTS) throw err
      core.debug(
        `Range at ${slice.srcStart} attempt ${attempt} failed: ${err.message}`
      )
      await new Promise(resolve => setTimeout(resolve, 250 * attempt))
    }
  }
}

interface StoreEntryJob {
  url: string
  total: number
  dataStart: number
  size: number
  expectedCrc: number
  destPath: string
}

async function extractStoreParallel(
  job: StoreEntryJob,
  concurrency: number,
  chunkSize: number,
  writeBufferSize: number,
  idleTimeoutMs: number
): Promise<void> {
  const slices: Slice[] = []
  for (let off = 0; off < job.size; off += chunkSize) {
    slices.push({
      srcStart: job.dataStart + off,
      dstOffset: off,
      len: Math.min(chunkSize, job.size - off)
    })
  }
  const crcs = new Array<number>(slices.length)
  // The first failure stops every other lane before the file is closed.
  const controller = new AbortController()

  const fh = await fs.promises.open(job.destPath, 'w')
  try {
    await fh.truncate(job.size)
    let next = 0
    const worker = async (): Promise<void> => {
      const buf = Buffer.allocUnsafe(Math.min(writeBufferSize, chunkSize))
      while (!controller.signal.aborted && next < slices.length) {
        const i = next++
        try {
          crcs[i] = await downloadSlice(
            job.url,
            job.total,
            fh,
            slices[i],
            buf,
            idleTimeoutMs,
            controller.signal
          )
        } catch (err) {
          controller.abort(err)
          throw err
        }
      }
    }
    const lanes = Math.min(concurrency, slices.length)
    const results = await Promise.allSettled(
      Array.from({length: lanes}, worker)
    )
    for (const r of results) {
      if (r.status === 'rejected') throw controller.signal.reason ?? r.reason
    }
  } finally {
    await fh.close()
  }

  let crc = 0
  for (let i = 0; i < slices.length; i++) {
    crc = crc32Combine(crc, crcs[i], slices[i].len)
  }
  if (crc !== job.expectedCrc) {
    throw new Error(`CRC-32 mismatch for ${job.destPath}`)
  }
}

async function extractStreamed(
  zipfile: yauzl.ZipFile,
  entry: yauzl.Entry,
  destPath: string
): Promise<void> {
  let crc = 0
  const tap = new Transform({
    transform(chunk: Buffer, _encoding, callback) {
      crc = (crc32 as Crc32)(chunk, crc)
      callback(null, chunk)
    }
  })
  const readStream = await zipfile.openReadStreamPromise(entry)
  await pipeline(readStream, tap, fs.createWriteStream(destPath))
  if (crc >>> 0 !== entry.crc32) {
    throw new Error(`CRC-32 mismatch for ${destPath}`)
  }
}

// Fetches the archive tail with one suffix Range GET. It follows redirects
// itself so that a downgrade from HTTPS is refused before it is requested.
async function probeTail(
  url: string,
  idleTimeoutMs: number
): Promise<{url: string; start: number; total: number; tail: Buffer} | null> {
  let current = url
  for (let i = 0; i <= MAX_REDIRECTS; i++) {
    const res = await fetch(current, {
      headers: {
        Range: `bytes=-${TAIL_PROBE_BYTES}`,
        'User-Agent': getUserAgentString()
      },
      redirect: 'manual',
      signal: AbortSignal.timeout(idleTimeoutMs)
    })
    if ([301, 302, 303, 307, 308].includes(res.status)) {
      await res.body?.cancel()
      const location = res.headers.get('location')
      const next = location ? redirectTarget(current, location) : null
      if (!next) return null
      current = next
      continue
    }
    const cr = parseContentRange(res.headers.get('content-range'))
    if (res.status !== 206 || !cr || cr.end !== cr.total - 1) {
      await res.body?.cancel()
      return null
    }
    const tail = Buffer.from(await res.arrayBuffer())
    if (tail.length !== cr.end - cr.start + 1) return null
    return {url: current, start: cr.start, total: cr.total, tail}
  }
  return null
}

/**
 * Extracts a zip artifact with parallel HTTP Range requests when it holds a
 * few entries and at least one large stored one, such as a disk image.
 * Returns false, having written nothing, when the server or the archive does
 * not qualify, so the caller streams it instead. Throws when a download
 * that already started fails; the caller then streams it as well.
 */
export async function tryRangeExtract(
  url: string,
  directory: string,
  options: RangeExtractOptions = {}
): Promise<boolean> {
  if (hasProxyEnv(url) || !crc32) return false

  const concurrency = Math.max(1, options.concurrency ?? DEFAULT_CONCURRENCY)
  const chunkSize = Math.max(64 * 1024, options.chunkSize ?? DEFAULT_CHUNK_SIZE)
  const writeBufferSize = Math.max(
    64 * 1024,
    options.writeBufferSize ?? DEFAULT_WRITE_BUFFER_SIZE
  )
  const minArchiveSize = options.minArchiveSize ?? DEFAULT_MIN_ARCHIVE_SIZE
  const maxEntries = options.maxEntries ?? DEFAULT_MAX_ENTRIES
  const idleTimeoutMs = options.idleTimeoutMs ?? DEFAULT_IDLE_TIMEOUT_MS
  const isLargeStore = (entry: yauzl.Entry): boolean =>
    entry.compressionMethod === 0 && entry.uncompressedSize >= minArchiveSize

  let zipfile: yauzl.ZipFile | undefined
  try {
    // Decide. A failure here only means the archive does not qualify.
    let probe: Awaited<ReturnType<typeof probeTail>>
    const entries: yauzl.Entry[] = []
    try {
      probe = await probeTail(url, idleTimeoutMs)
      if (!probe || probe.total < minArchiveSize) return false

      const reader = new HttpRangeReader(
        probe.url,
        probe.total,
        probe.start,
        probe.tail,
        idleTimeoutMs
      )
      zipfile = await yauzl.fromRandomAccessReaderPromise(reader, probe.total, {
        lazyEntries: true,
        autoClose: false,
        // Reject backslashes instead of turning them into directories,
        // which the streaming extractor does not do.
        strictFileNames: true
      })
      if (zipfile.entryCount > maxEntries) return false

      for await (const entry of zipfile.eachEntry()) {
        if (entry.isEncrypted() || !entry.canDecodeFileData()) return false
        resolveSafePath(directory, entry.fileName)
        entries.push(entry)
      }
      if (!entries.some(isLargeStore)) return false
    } catch (err) {
      core.debug(`Parallel range download not used: ${err.message}`)
      return false
    }

    // Extract. A failure here is an error the caller recovers from.
    core.debug(
      `Using parallel range extraction (${entries.length} entries, ${probe.total} bytes, concurrency=${concurrency})`
    )
    for (const entry of entries) {
      const destPath = resolveSafePath(directory, entry.fileName)
      if (entry.fileName.endsWith('/')) {
        await fs.promises.mkdir(destPath, {recursive: true})
        continue
      }
      await fs.promises.mkdir(path.dirname(destPath), {recursive: true})

      if (isLargeStore(entry)) {
        const {fileDataStart} = await zipfile.readLocalFileHeaderPromise(
          entry,
          {minimal: true}
        )
        await extractStoreParallel(
          {
            url: probe.url,
            total: probe.total,
            dataStart: fileDataStart,
            size: entry.uncompressedSize,
            expectedCrc: entry.crc32,
            destPath
          },
          concurrency,
          chunkSize,
          writeBufferSize,
          idleTimeoutMs
        )
      } else {
        await extractStreamed(zipfile, entry, destPath)
      }
    }
    return true
  } finally {
    zipfile?.close()
  }
}
