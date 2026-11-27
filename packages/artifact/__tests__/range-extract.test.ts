import * as crypto from 'crypto'
import * as fs from 'fs'
import * as http from 'http'
import {AddressInfo} from 'net'
import * as path from 'path'
import * as zlib from 'zlib'
import archiver from 'archiver'
import {
  crc32Combine,
  parseContentRange,
  redirectTarget,
  resolveSafePath,
  tryRangeExtract
} from '../src/internal/download/range-extract'
import {streamExtractExternal} from '../src/internal/download/download-artifact'
import {noopLogs} from './common'

const testDir = path.join(__dirname, '_temp', 'range-extract')
const CHUNK = 512 * 1024

interface TestServerHandle {
  url: string
  requests: {method: string; range?: string; path: string}[]
  close: () => Promise<void>
}

interface TestServerOptions {
  supportRange?: boolean
  redirectOnce?: boolean
  // Called for every satisfiable range request. Return true when it wrote
  // the response itself.
  onRange?: (
    start: number,
    end: number,
    res: http.ServerResponse,
    zip: Buffer
  ) => boolean | void
}

function sendRange(
  res: http.ServerResponse,
  zip: Buffer,
  start: number,
  end: number,
  body?: Buffer
): void {
  const slice = body ?? zip.subarray(start, end + 1)
  res.writeHead(206, {
    'Content-Type': 'application/zip',
    'Accept-Ranges': 'bytes',
    'Content-Range': `bytes ${start}-${end}/${zip.length}`,
    'Content-Length': String(slice.length)
  })
  res.end(slice)
}

async function startZipServer(
  zip: Buffer,
  options: TestServerOptions = {}
): Promise<TestServerHandle> {
  const supportRange = options.supportRange ?? true
  const redirectOnce = options.redirectOnce ?? false
  const requests: {method: string; range?: string; path: string}[] = []

  const server = http.createServer((req, res) => {
    const reqPath = req.url || '/'
    const rangeHdr = req.headers['range']
    requests.push({
      method: req.method || 'GET',
      range: typeof rangeHdr === 'string' ? rangeHdr : undefined,
      path: reqPath
    })

    if (redirectOnce && reqPath === '/redirect') {
      res.writeHead(302, {Location: '/artifact.zip'})
      res.end()
      return
    }

    if (supportRange && typeof rangeHdr === 'string') {
      const m = /^bytes=(\d*)-(\d*)$/.exec(rangeHdr.trim())
      if (m) {
        let start = 0
        let end = zip.length - 1
        if (m[1] === '' && m[2] !== '') {
          start = Math.max(0, zip.length - Number(m[2]))
        } else {
          start = Number(m[1])
          if (m[2] !== '') end = Math.min(zip.length - 1, Number(m[2]))
        }
        if (options.onRange?.(start, end, res, zip)) return
        sendRange(res, zip, start, end)
        return
      }
    }

    res.writeHead(200, {
      'Content-Type': 'application/zip',
      'Content-Length': String(zip.length)
    })
    res.end(zip)
  })

  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve))
  const port = (server.address() as AddressInfo).port
  return {
    url: `http://127.0.0.1:${port}${
      redirectOnce ? '/redirect' : '/artifact.zip'
    }`,
    requests,
    close: async () =>
      new Promise<void>(resolve => {
        server.closeAllConnections()
        server.close(() => resolve())
      })
  }
}

async function createStreamingArchiverZip(
  files: {name: string; content: Buffer}[],
  compressionLevel: number
): Promise<Buffer> {
  const srcDir = path.join(testDir, `src-${Date.now()}-${Math.random()}`)
  await fs.promises.mkdir(srcDir, {recursive: true})

  const zip = archiver.create('zip', {zlib: {level: compressionLevel}})
  const chunks: Buffer[] = []
  zip.on('data', (c: Buffer) => chunks.push(c))

  for (const [i, f] of files.entries()) {
    const srcPath = path.join(srcDir, `${i}.bin`)
    await fs.promises.writeFile(srcPath, f.content)
    zip.append(fs.createReadStream(srcPath), {name: f.name})
  }

  await new Promise<void>((resolve, reject) => {
    zip.on('end', resolve)
    zip.on('error', reject)
    zip.finalize()
  })
  return Buffer.concat(chunks)
}

// A stored zip written by hand, for names, flags and CRCs archiver will not
// produce. No data descriptors.
function rawStoreZip(
  entries: {name: Buffer; data: Buffer; flags?: number; crc?: number}[]
): Buffer {
  const crc32 = (
    zlib as unknown as {crc32: (d: Uint8Array, v?: number) => number}
  ).crc32
  const parts: Buffer[] = []
  const cds: Buffer[] = []
  let off = 0
  for (const e of entries) {
    const crc = e.crc ?? crc32(e.data)
    const flags = e.flags ?? 0
    const lfh = Buffer.alloc(30)
    lfh.writeUInt32LE(0x04034b50, 0)
    lfh.writeUInt16LE(20, 4)
    lfh.writeUInt16LE(flags, 6)
    lfh.writeUInt32LE(crc, 14)
    lfh.writeUInt32LE(e.data.length, 18)
    lfh.writeUInt32LE(e.data.length, 22)
    lfh.writeUInt16LE(e.name.length, 26)
    const cd = Buffer.alloc(46)
    cd.writeUInt32LE(0x02014b50, 0)
    cd.writeUInt16LE(20, 4)
    cd.writeUInt16LE(20, 6)
    cd.writeUInt16LE(flags, 8)
    cd.writeUInt32LE(crc, 16)
    cd.writeUInt32LE(e.data.length, 20)
    cd.writeUInt32LE(e.data.length, 24)
    cd.writeUInt16LE(e.name.length, 28)
    cd.writeUInt32LE(off, 42)
    parts.push(lfh, e.name, e.data)
    cds.push(cd, e.name)
    off += 30 + e.name.length + e.data.length
  }
  const cdBuf = Buffer.concat(cds)
  const eocd = Buffer.alloc(22)
  eocd.writeUInt32LE(0x06054b50, 0)
  eocd.writeUInt16LE(entries.length, 8)
  eocd.writeUInt16LE(entries.length, 10)
  eocd.writeUInt32LE(cdBuf.length, 12)
  eocd.writeUInt32LE(off, 16)
  return Buffer.concat([...parts, cdBuf, eocd])
}

// One stored entry of four slices plus a small one, served with ranges.
async function bigStoreZip(): Promise<{zip: Buffer; payload: Buffer}> {
  const payload = crypto.randomBytes(4 * CHUNK)
  const zip = await createStreamingArchiverZip(
    [
      {name: 'release/test.iso', content: payload},
      {name: 'release/SHA256SUMS', content: Buffer.from('sums\n')}
    ],
    0
  )
  return {zip, payload}
}

const fastOptions = {
  minArchiveSize: CHUNK,
  chunkSize: CHUNK,
  writeBufferSize: 128 * 1024,
  concurrency: 4,
  idleTimeoutMs: 1000
}

async function newOutDir(name: string): Promise<string> {
  const dir = path.join(testDir, name)
  await fs.promises.mkdir(dir, {recursive: true})
  return dir
}

describe('range-extract', () => {
  beforeEach(async () => {
    noopLogs()
    await fs.promises.rm(testDir, {recursive: true, force: true})
    await fs.promises.mkdir(testDir, {recursive: true})
  })

  afterEach(async () => {
    await fs.promises.rm(testDir, {recursive: true, force: true})
  })

  it('extracts streaming bit-3 STORE archive in parallel without HEAD', async () => {
    const bigPayload = crypto.randomBytes(3 * 1024 * 1024 + 12345)
    const smallPayload = Buffer.from('1234')
    const zipBuf = await createStreamingArchiverZip(
      [
        {name: 'release/test.iso', content: bigPayload},
        {name: 'test-runs/build-seconds', content: smallPayload}
      ],
      0
    )

    expect(zipBuf.readUInt16LE(6) & 0x08).toBe(0x08)
    expect(zipBuf.readUInt16LE(8)).toBe(0)

    const srv = await startZipServer(zipBuf)
    const outDir = await newOutDir('out-store')

    try {
      const handled = await tryRangeExtract(srv.url, outDir, fastOptions)
      expect(handled).toBe(true)
      expect(srv.requests.some(r => r.method === 'HEAD')).toBe(false)
      expect(srv.requests[0].range).toBe('bytes=-65577')

      expect(
        (
          await fs.promises.readFile(path.join(outDir, 'release/test.iso'))
        ).equals(bigPayload)
      ).toBe(true)
      expect(
        (
          await fs.promises.readFile(
            path.join(outDir, 'test-runs/build-seconds')
          )
        ).equals(smallPayload)
      ).toBe(true)
    } finally {
      await srv.close()
    }
  })

  it('follows 302 redirects on STORE archives and falls back for DEFLATE-only archives', async () => {
    const storePayload = Buffer.from('omarchy-redirect-data-'.repeat(40000))
    const storeZip = await createStreamingArchiverZip(
      [{name: 'nested/a.bin', content: storePayload}],
      0
    )
    const redirectSrv = await startZipServer(storeZip, {redirectOnce: true})
    const outStore = await newOutDir('out-redirect')

    try {
      const handled = await tryRangeExtract(redirectSrv.url, outStore, {
        ...fastOptions,
        concurrency: 2
      })
      expect(handled).toBe(true)
      expect(
        (
          await fs.promises.readFile(path.join(outStore, 'nested/a.bin'))
        ).equals(storePayload)
      ).toBe(true)
    } finally {
      await redirectSrv.close()
    }

    const deflatePayload = Buffer.from('omarchy-deflate-data-'.repeat(10000))
    const deflateZip = await createStreamingArchiverZip(
      [{name: 'nested/b.txt', content: deflatePayload}],
      6
    )
    const deflateSrv = await startZipServer(deflateZip)
    const outDeflate = await newOutDir('out-deflate')

    try {
      expect(
        await tryRangeExtract(deflateSrv.url, outDeflate, {minArchiveSize: 0})
      ).toBe(false)
      await streamExtractExternal(deflateSrv.url, outDeflate)
      expect(
        (
          await fs.promises.readFile(path.join(outDeflate, 'nested/b.txt'))
        ).equals(deflatePayload)
      ).toBe(true)
    } finally {
      await deflateSrv.close()
    }
  })

  it('leaves Zip Slip paths to the streaming extractor, which neutralises them', async () => {
    expect(() => resolveSafePath('/tmp/dest', '../etc/passwd')).toThrow(
      /Unsafe zip entry path/
    )
    expect(() => resolveSafePath('/tmp/dest', 'x/../../etc/hosts')).toThrow(
      /Unsafe zip entry path/
    )
    expect(() => resolveSafePath('/tmp/dest', '/etc/passwd')).toThrow(
      /Unsafe zip entry path/
    )

    const evilZip = await fs.promises.readFile(
      path.join(__dirname, 'fixtures', 'evil.zip')
    )
    const srv = await startZipServer(evilZip)
    const outDir = await newOutDir('out-evil')

    try {
      expect(await tryRangeExtract(srv.url, outDir, {minArchiveSize: 0})).toBe(
        false
      )
      await streamExtractExternal(srv.url, outDir)
      const written = await fs.promises.readdir(outDir, {recursive: true})
      expect(written.length).toBeGreaterThan(0)
      expect(fs.existsSync(path.join(testDir, 'etc'))).toBe(false)
    } finally {
      await srv.close()
    }
  })

  it('falls back cleanly to unzip-stream when server returns 200 OK', async () => {
    const zipBuf = await createStreamingArchiverZip(
      [{name: 'hello.txt', content: Buffer.from('fallback-ok')}],
      0
    )
    const srv = await startZipServer(zipBuf, {supportRange: false})
    const outDir = await newOutDir('out-fallback')

    try {
      expect(await tryRangeExtract(srv.url, outDir, {minArchiveSize: 0})).toBe(
        false
      )
      await streamExtractExternal(srv.url, outDir)
      expect(
        await fs.promises.readFile(path.join(outDir, 'hello.txt'), 'utf8')
      ).toBe('fallback-ok')
    } finally {
      await srv.close()
    }
  })

  it('parses Content-Range headers accurately', () => {
    expect(parseContentRange('bytes 0-65535/7826651626')).toEqual({
      start: 0,
      end: 65535,
      total: 7826651626
    })
    expect(parseContentRange('bytes */100')).toBeNull()
    expect(parseContentRange(null)).toBeNull()
  })

  it('declines archives with many entries before reading their central directory', async () => {
    const files = [
      {name: 'release/test.iso', content: crypto.randomBytes(2 * CHUNK)}
    ]
    for (let i = 0; i < 2000; i++) {
      files.push({
        name: `reports/junit/suite-${String(i).padStart(6, '0')}.xml`,
        content: Buffer.from(`<testsuite id="${i}"/>\n`)
      })
    }
    const zip = await createStreamingArchiverZip(files, 0)
    const srv = await startZipServer(zip)
    const outDir = await newOutDir('out-many')

    try {
      expect(await tryRangeExtract(srv.url, outDir, fastOptions)).toBe(false)
      expect(srv.requests.length).toBe(1)
    } finally {
      await srv.close()
    }
  })

  it('rejects a slice whose Content-Range is not the one requested', async () => {
    const {zip, payload} = await bigStoreZip()
    const srv = await startZipServer(zip, {
      onRange: (start, end, res, buf) => {
        // A confused cache answers with the previous slice and its own header.
        if (end - start + 1 === CHUNK && start >= CHUNK) {
          sendRange(res, buf, start - CHUNK, end - CHUNK)
          return true
        }
      }
    })
    const outDir = await newOutDir('out-wrong-range')

    try {
      await expect(
        tryRangeExtract(srv.url, outDir, fastOptions)
      ).rejects.toThrow(/Content-Range/)
      await streamExtractExternal(srv.url, outDir, fastOptions)
      expect(
        (
          await fs.promises.readFile(path.join(outDir, 'release/test.iso'))
        ).equals(payload)
      ).toBe(true)
    } finally {
      await srv.close()
    }
  })

  it('detects corrupted slice bytes with the entry CRC-32', async () => {
    const {zip} = await bigStoreZip()
    let flipped = false
    const srv = await startZipServer(zip, {
      onRange: (start, end, res, buf) => {
        if (!flipped && end - start + 1 === CHUNK) {
          flipped = true
          const body = Buffer.from(buf.subarray(start, end + 1))
          body[100] ^= 0xff
          sendRange(res, buf, start, end, body)
          return true
        }
      }
    })
    const outDir = await newOutDir('out-crc')

    try {
      await expect(
        tryRangeExtract(srv.url, outDir, fastOptions)
      ).rejects.toThrow(/CRC-32/)
    } finally {
      await srv.close()
    }
  })

  it('falls back to one stream when a data range is answered with 200', async () => {
    // Default options, so the archive must exceed the 16 MiB defaults.
    const slice = 16 * 1024 * 1024
    const payload = crypto.randomBytes(2 * slice)
    const zip = await createStreamingArchiverZip(
      [{name: 'release/test.iso', content: payload}],
      0
    )
    const srv = await startZipServer(zip, {
      onRange: (start, end, res, buf) => {
        if (end - start + 1 === slice) {
          res.writeHead(200, {'Content-Length': String(buf.length)})
          res.end(buf)
          return true
        }
      }
    })
    const outDir = await newOutDir('out-200')

    try {
      await streamExtractExternal(srv.url, outDir)
      expect(
        (
          await fs.promises.readFile(path.join(outDir, 'release/test.iso'))
        ).equals(payload)
      ).toBe(true)
    } finally {
      await srv.close()
    }
  })

  it('times out a stalled range on idle time, not on total time', async () => {
    const {zip, payload} = await bigStoreZip()
    const trickle = await startZipServer(zip, {
      onRange: (start, end, res, buf) => {
        if (end - start + 1 !== CHUNK) return
        // 64 KiB every 150 ms: a slice takes 1.2 s, beyond the 1 s budget,
        // but data never stops for longer than 150 ms.
        res.writeHead(206, {
          'Content-Range': `bytes ${start}-${end}/${buf.length}`,
          'Content-Length': String(CHUNK)
        })
        let off = start
        const timer = setInterval(() => {
          const next = Math.min(off + 64 * 1024, end + 1)
          res.write(buf.subarray(off, next))
          off = next
          if (off > end) {
            clearInterval(timer)
            res.end()
          }
        }, 150)
        res.on('close', () => clearInterval(timer))
        return true
      }
    })
    const outTrickle = await newOutDir('out-trickle')
    try {
      expect(await tryRangeExtract(trickle.url, outTrickle, fastOptions)).toBe(
        true
      )
      expect(
        (
          await fs.promises.readFile(path.join(outTrickle, 'release/test.iso'))
        ).equals(payload)
      ).toBe(true)
    } finally {
      await trickle.close()
    }

    const stall = await startZipServer(zip, {
      onRange: (start, end, res, buf) => {
        if (end - start + 1 !== CHUNK) return
        res.writeHead(206, {
          'Content-Range': `bytes ${start}-${end}/${buf.length}`,
          'Content-Length': String(CHUNK)
        })
        res.write(buf.subarray(start, start + 1024))
        return true
      }
    })
    const outStall = await newOutDir('out-stall')
    const t0 = Date.now()
    try {
      await expect(
        tryRangeExtract(stall.url, outStall, {
          ...fastOptions,
          idleTimeoutMs: 200
        })
      ).rejects.toThrow(/No data/)
      // Three attempts with 200 ms idle time and short backoff.
      expect(Date.now() - t0).toBeLessThan(5000)
    } finally {
      await stall.close()
    }
  }, 15000)

  it('stops every request once one slice fails', async () => {
    const {zip} = await bigStoreZip()
    const srv = await startZipServer(zip, {
      onRange: (start, end, res, buf) => {
        if (end - start + 1 !== CHUNK) return
        if (start >= 2 * CHUNK) {
          res.writeHead(403)
          res.end()
          return true
        }
        // Healthy slices are still in flight when the failing one gives up.
        setTimeout(() => sendRange(res, buf, start, end), 2500)
        return true
      }
    })
    const outDir = await newOutDir('out-abort')

    try {
      await expect(
        tryRangeExtract(srv.url, outDir, {...fastOptions, concurrency: 4})
      ).rejects.toThrow(/403/)
      const settled = srv.requests.length
      await new Promise(resolve => setTimeout(resolve, 3500))
      expect(srv.requests.length).toBe(settled)
    } finally {
      await srv.close()
    }
  }, 15000)

  it('falls back when the probe body breaks', async () => {
    const {zip} = await bigStoreZip()
    const srv = await startZipServer(zip, {
      onRange: (start, end, res, buf) => {
        if (end !== buf.length - 1) return
        res.writeHead(206, {
          'Content-Range': `bytes ${start}-${end}/${buf.length}`,
          'Content-Length': String(end - start + 1)
        })
        res.write(buf.subarray(start, start + 100))
        setTimeout(() => res.destroy(), 50)
        return true
      }
    })
    const outDir = await newOutDir('out-probe-reset')

    try {
      expect(await tryRangeExtract(srv.url, outDir, fastOptions)).toBe(false)
    } finally {
      await srv.close()
    }
  })

  it('leaves CP437 names to the streaming extractor', async () => {
    // 0xff is a plain space to unzip-stream and U+00A0 to yauzl.
    const zip = rawStoreZip([
      {
        name: Buffer.from([0x72, 0x65, 0xff, 0x2e, 0x69, 0x73, 0x6f]),
        data: crypto.randomBytes(2 * CHUNK)
      }
    ])
    const srv = await startZipServer(zip)
    const outDir = await newOutDir('out-cp437')

    try {
      expect(await tryRangeExtract(srv.url, outDir, fastOptions)).toBe(false)
    } finally {
      await srv.close()
    }
  })

  it('fails without streaming again when the archive itself has a bad CRC-32', async () => {
    const zip = rawStoreZip([
      {
        name: Buffer.from('release/test.iso'),
        data: crypto.randomBytes(2 * CHUNK),
        crc: 0x12345678
      }
    ])
    const srv = await startZipServer(zip)
    const outDir = await newOutDir('out-bad-crc')

    try {
      await expect(
        streamExtractExternal(srv.url, outDir, fastOptions)
      ).rejects.toThrow(/CRC-32/)
      // One probe, one local header, two slices; no full GET afterwards.
      expect(srv.requests.every(r => r.range !== undefined)).toBe(true)
    } finally {
      await srv.close()
    }
  })

  it('follows a redirect on a data range', async () => {
    const {zip, payload} = await bigStoreZip()
    const srv = await startZipServer(zip, {
      onRange: (start, end, res) => {
        if (end - start + 1 === CHUNK && res.req.url === '/artifact.zip') {
          res.writeHead(307, {Location: '/blob'})
          res.end()
          return true
        }
      }
    })
    const outDir = await newOutDir('out-slice-redirect')

    try {
      expect(await tryRangeExtract(srv.url, outDir, fastOptions)).toBe(true)
      expect(
        (
          await fs.promises.readFile(path.join(outDir, 'release/test.iso'))
        ).equals(payload)
      ).toBe(true)
    } finally {
      await srv.close()
    }
  })

  it('rejects ranges from a different version of the object', async () => {
    const {zip, payload} = await bigStoreZip()
    const srv = await startZipServer(zip, {
      onRange: (start, end, res, buf) => {
        const etag = end === buf.length - 1 ? '"v1"' : '"v2"'
        res.setHeader('ETag', etag)
        sendRange(res, buf, start, end)
        return true
      }
    })
    const outDir = await newOutDir('out-etag')

    try {
      await expect(
        tryRangeExtract(srv.url, outDir, fastOptions)
      ).rejects.toThrow(/ETag/)
      await streamExtractExternal(srv.url, outDir, fastOptions)
      expect(
        (
          await fs.promises.readFile(path.join(outDir, 'release/test.iso'))
        ).equals(payload)
      ).toBe(true)
    } finally {
      await srv.close()
    }
  })

  it('ignores options that are not positive numbers', async () => {
    const {zip, payload} = await bigStoreZip()
    const srv = await startZipServer(zip)
    const outDir = await newOutDir('out-nan')

    try {
      expect(
        await tryRangeExtract(srv.url, outDir, {
          ...fastOptions,
          concurrency: NaN
        })
      ).toBe(true)
      expect(
        (
          await fs.promises.readFile(path.join(outDir, 'release/test.iso'))
        ).equals(payload)
      ).toBe(true)
    } finally {
      await srv.close()
    }
  })

  it('refuses a redirect from HTTPS to HTTP', () => {
    expect(
      redirectTarget('https://blob.example/a?sig=1', 'http://blob.example/b')
    ).toBeNull()
    expect(
      redirectTarget('https://blob.example/a?sig=1', 'https://cdn.example/b')
    ).toBe('https://cdn.example/b')
    expect(redirectTarget('http://blob.example/a', '/b')).toBe(
      'http://blob.example/b'
    )
  })

  it('allows entries when the destination is the filesystem root', () => {
    expect(resolveSafePath('/', 'artifact.bin')).toBe(
      path.resolve('/artifact.bin')
    )
  })

  it('combines CRC-32 values like zlib over the concatenation', () => {
    const crc32 = (
      zlib as unknown as {crc32: (d: Uint8Array, v?: number) => number}
    ).crc32
    for (const [a, b] of [
      [0, 0],
      [0, 17],
      [1, 1],
      [12345, 678901],
      [CHUNK, CHUNK + 3]
    ]) {
      const x = crypto.randomBytes(a)
      const y = crypto.randomBytes(b)
      expect(crc32Combine(crc32(x), crc32(y), y.length)).toBe(
        crc32(Buffer.concat([x, y]))
      )
    }
  })
})
