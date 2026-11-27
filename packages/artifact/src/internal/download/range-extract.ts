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
const REDIRECT_STATUSES = [301, 302, 303, 307, 308]
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

// The downloaded bytes do not match the archive's own CRC-32. Streaming the
// same object again would write the same bytes without checking them.
export class ArtifactIntegrityError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'ArtifactIntegrityError'
  }
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

// True when unzip-stream would write this entry to the same path. It maps
// CP437 differently, so only ASCII or UTF-8-flagged names without a Unicode
// Path field (0x7075) decode the same. It also rewrites every segment of
// two or more dots to a single dot.
function namedLikeStreaming(entry: yauzl.Entry): boolean {
  const utf8 = (entry.generalPurposeBitFlag & 0x800) !== 0
  const ascii = entry.fileNameRaw.every(b => b < 0x80)
  return (
    (utf8 || ascii) &&
    !entry.extraFields.some(f => f.id === 0x7075) &&
    !entry.fileName.split('/').some(segment => /^\.{2,}$/.test(segment))
  )
}

function hasProxyEnv(url: string): boolean {
  const proxy = url.startsWith('https:')
    ? process.env['HTTPS_PROXY'] || process.env['https_proxy']
    : process.env['HTTP_PROXY'] || process.env['http_proxy']
  return Boolean(proxy && proxy.trim().length > 0)
}

// Node runs a timer of 2^31 ms or more after 1 ms.
const MAX_TIMER_MS = 2 ** 31 - 1

function positive(value: number | undefined, fallback: number): number {
  return value !== undefined && Number.isFinite(value) && value >= 1
    ? Math.floor(value)
    : fallback
}

// An abort signal that also fires when no data arrives for idleMs. The
// clock runs only while waiting on the network, so time the consumer
// spends writing does not count against it. touch() and pause() only set
// fields: one timer re-arms itself about once per idleMs instead of
// once per chunk.
class IdleAbort {
  private readonly controller = new AbortController()
  private last = Date.now()
  private paused = false
  private timer: ReturnType<typeof setTimeout>
  private readonly onParentAbort = (): void =>
    this.controller.abort(this.parent?.reason)

  constructor(
    private readonly idleMs: number,
    private readonly what: string,
    private readonly parent?: AbortSignal
  ) {
    if (parent?.aborted) this.controller.abort(parent.reason)
    parent?.addEventListener('abort', this.onParentAbort, {once: true})
    this.timer = setTimeout(this.check, idleMs)
  }

  private readonly check = (): void => {
    const idle = this.paused ? 0 : Date.now() - this.last
    if (idle >= this.idleMs) {
      this.controller.abort(
        new Error(`No data for ${this.idleMs} ms on ${this.what}`)
      )
      return
    }
    this.timer = setTimeout(this.check, this.idleMs - idle)
  }

  get signal(): AbortSignal {
    return this.controller.signal
  }

  touch(): void {
    this.last = Date.now()
    this.paused = false
  }

  pause(): void {
    this.paused = true
  }

  dispose(): void {
    clearTimeout(this.timer)
    this.parent?.removeEventListener('abort', this.onParentAbort)
  }
}

// GET with a Range header. It follows redirects itself so that a downgrade
// from HTTPS is refused before anything is sent, and returns the URL it
// ended at. Errors name the range, never the URL: it carries a signature.
async function fetchRange(
  url: string,
  range: string,
  idle: IdleAbort
): Promise<{res: Response; url: string}> {
  let current = url
  for (let hops = 0; ; hops++) {
    idle.touch()
    const res = await fetch(current, {
      headers: {Range: range, 'User-Agent': getUserAgentString()},
      redirect: 'manual',
      signal: idle.signal
    })
    if (!REDIRECT_STATUSES.includes(res.status)) return {res, url: current}
    await res.body?.cancel()
    const location = res.headers.get('location')
    const next = location ? redirectTarget(current, location) : null
    if (!next || hops >= MAX_REDIRECTS) {
      throw new Error(`Refused redirect for range ${range}`)
    }
    current = next
  }
}

// Yields exactly `want` bytes of a response body and fails on more or less.
async function* exactBody(
  res: Response,
  want: number,
  idle: IdleAbort,
  what: string
): AsyncGenerator<Buffer> {
  if (!res.body) throw new Error(`Empty body for ${what}`)
  const reader = res.body.getReader()
  let finished = false
  try {
    let received = 0
    for (;;) {
      idle.touch()
      const {done, value} = await reader.read()
      idle.pause()
      if (done) break
      received += value.byteLength
      if (received > want) {
        throw new Error(`${what} returned more than ${want} bytes`)
      }
      yield Buffer.from(value.buffer, value.byteOffset, value.byteLength)
    }
    if (received !== want) {
      throw new Error(`${what} returned ${received} of ${want} bytes`)
    }
    finished = true
  } finally {
    if (!finished) await reader.cancel().catch(() => {})
  }
}

// The object the probe saw. Every later range must come from the same one.
interface ArchiveObject {
  url: string
  total: number
  etag: string | null
}

// Yields the bytes [start, end) of the archive. It fails unless the server
// answers 206 with exactly that Content-Range of the probed object.
async function* rangeBody(
  obj: ArchiveObject,
  start: number,
  end: number,
  idleTimeoutMs: number,
  signal?: AbortSignal
): AsyncGenerator<Buffer> {
  const range = `bytes=${start}-${end - 1}`
  const idle = new IdleAbort(idleTimeoutMs, `range ${range}`, signal)
  try {
    const {res} = await fetchRange(obj.url, range, idle)
    const header = res.headers.get('content-range')
    const cr = parseContentRange(header)
    if (
      res.status !== 206 ||
      !cr ||
      cr.start !== start ||
      cr.end !== end - 1 ||
      cr.total !== obj.total
    ) {
      await res.body?.cancel()
      throw new Error(
        `Unexpected response for range ${range}: HTTP ${res.status}, Content-Range ${header}`
      )
    }
    if (obj.etag !== null && res.headers.get('etag') !== obj.etag) {
      await res.body?.cancel()
      throw new Error(`ETag changed for range ${range}`)
    }
    yield* exactBody(res, end - start, idle, `range ${range}`)
  } finally {
    idle.dispose()
  }
}

class HttpRangeReader extends yauzl.RandomAccessReader {
  constructor(
    private readonly obj: ArchiveObject,
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
    // Readable.from would wait for a pending read before cleaning up, so
    // destroying the stream aborts the request first.
    const stop = new AbortController()
    const body = rangeBody(
      this.obj,
      start,
      end,
      this.idleTimeoutMs,
      stop.signal
    )
    return new Readable({
      read() {
        body.next().then(
          ({done, value}) => this.push(done ? null : value),
          err => this.destroy(err)
        )
      },
      destroy(err, callback) {
        stop.abort(err ?? new Error('Range stream closed'))
        body.return(undefined).then(
          () => callback(err),
          () => callback(err)
        )
      }
    })
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
  obj: ArchiveObject,
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
      for await (const chunk of rangeBody(
        obj,
        slice.srcStart,
        slice.srcStart + slice.len,
        idleTimeoutMs,
        signal
      )) {
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
  obj: ArchiveObject
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
  const crcs: (number | undefined)[] = slices.map(() => undefined)
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
            job.obj,
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
  for (const [i, sliceCrc] of crcs.entries()) {
    if (sliceCrc === undefined) throw new Error(`Slice ${i} was not fetched`)
    crc = crc32Combine(crc, sliceCrc, slices[i].len)
  }
  if (crc !== job.expectedCrc) {
    throw new ArtifactIntegrityError(`CRC-32 mismatch for ${job.destPath}`)
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
    throw new ArtifactIntegrityError(`CRC-32 mismatch for ${destPath}`)
  }
}

// Fetches the archive tail with one suffix Range GET. A response that is
// not exactly that suffix is refused before its body is read.
async function probeTail(
  url: string,
  idleTimeoutMs: number
): Promise<{obj: ArchiveObject; start: number; tail: Buffer} | null> {
  const what = 'the archive tail'
  const idle = new IdleAbort(idleTimeoutMs, what)
  try {
    const {res, url: finalUrl} = await fetchRange(
      url,
      `bytes=-${TAIL_PROBE_BYTES}`,
      idle
    )
    const cr = parseContentRange(res.headers.get('content-range'))
    const want = cr ? cr.end - cr.start + 1 : 0
    if (
      res.status !== 206 ||
      !cr ||
      cr.end !== cr.total - 1 ||
      want !== Math.min(TAIL_PROBE_BYTES, cr.total)
    ) {
      await res.body?.cancel()
      return null
    }
    const chunks: Buffer[] = []
    for await (const chunk of exactBody(res, want, idle, what)) {
      chunks.push(chunk)
    }
    return {
      obj: {url: finalUrl, total: cr.total, etag: res.headers.get('etag')},
      start: cr.start,
      tail: Buffer.concat(chunks)
    }
  } finally {
    idle.dispose()
  }
}

/**
 * Extracts a zip artifact with parallel HTTP Range requests when it holds a
 * few entries and at least one large stored one, such as a disk image.
 * Returns false, having written nothing, when the server or the archive does
 * not qualify, so the caller streams it instead. Throws when a download that
 * already started fails. The caller then streams it as well, except after
 * an ArtifactIntegrityError.
 */
export async function tryRangeExtract(
  url: string,
  directory: string,
  options: RangeExtractOptions = {}
): Promise<boolean> {
  if (hasProxyEnv(url) || !crc32) return false

  const concurrency = positive(options.concurrency, DEFAULT_CONCURRENCY)
  const chunkSize = Math.max(
    64 * 1024,
    positive(options.chunkSize, DEFAULT_CHUNK_SIZE)
  )
  const writeBufferSize = Math.max(
    64 * 1024,
    positive(options.writeBufferSize, DEFAULT_WRITE_BUFFER_SIZE)
  )
  const minArchiveSize =
    options.minArchiveSize === 0
      ? 0
      : positive(options.minArchiveSize, DEFAULT_MIN_ARCHIVE_SIZE)
  const maxEntries = positive(options.maxEntries, DEFAULT_MAX_ENTRIES)
  const idleTimeoutMs = Math.min(
    MAX_TIMER_MS,
    positive(options.idleTimeoutMs, DEFAULT_IDLE_TIMEOUT_MS)
  )
  const isLargeStore = (entry: yauzl.Entry): boolean =>
    entry.compressionMethod === 0 && entry.uncompressedSize >= minArchiveSize

  let zipfile: yauzl.ZipFile | undefined
  try {
    // Decide. A failure here only means the archive does not qualify.
    let probe: Awaited<ReturnType<typeof probeTail>>
    const entries: yauzl.Entry[] = []
    try {
      probe = await probeTail(url, idleTimeoutMs)
      if (!probe || probe.obj.total < minArchiveSize) return false

      const reader = new HttpRangeReader(
        probe.obj,
        probe.start,
        probe.tail,
        idleTimeoutMs
      )
      zipfile = await yauzl.fromRandomAccessReaderPromise(
        reader,
        probe.obj.total,
        {
          lazyEntries: true,
          autoClose: false,
          // Reject backslashes instead of turning them into directories,
          // which the streaming extractor does not do.
          strictFileNames: true
        }
      )
      if (zipfile.entryCount > maxEntries) return false

      for await (const entry of zipfile.eachEntry()) {
        if (
          entry.isEncrypted() ||
          !entry.canDecodeFileData() ||
          !namedLikeStreaming(entry)
        ) {
          return false
        }
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
      `Using parallel range extraction (${entries.length} entries, ${probe.obj.total} bytes, concurrency=${concurrency})`
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
            obj: probe.obj,
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
