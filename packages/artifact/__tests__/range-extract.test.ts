import * as crypto from 'crypto'
import * as fs from 'fs'
import * as http from 'http'
import {AddressInfo} from 'net'
import * as path from 'path'
import archiver from 'archiver'
import {
  parseContentRange,
  resolveSafePath,
  tryRangeExtract
} from '../src/internal/download/range-extract'
import {streamExtractExternal} from '../src/internal/download/download-artifact'
import {noopLogs} from './common'

const testDir = path.join(__dirname, '_temp', 'range-extract')

interface TestServerHandle {
  url: string
  requests: {method: string; range?: string; path: string}[]
  close: () => Promise<void>
}

async function startZipServer(
  zipBuffer: Buffer,
  options: {supportRange?: boolean; redirectOnce?: boolean} = {}
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
        let end = zipBuffer.length - 1
        if (m[1] === '' && m[2] !== '') {
          start = Math.max(0, zipBuffer.length - Number(m[2]))
        } else {
          start = Number(m[1])
          if (m[2] !== '') end = Math.min(zipBuffer.length - 1, Number(m[2]))
        }
        const slice = zipBuffer.subarray(start, end + 1)
        res.writeHead(206, {
          'Content-Type': 'application/zip',
          'Accept-Ranges': 'bytes',
          'Content-Range': `bytes ${start}-${end}/${zipBuffer.length}`,
          'Content-Length': String(slice.length)
        })
        res.end(slice)
        return
      }
    }

    res.writeHead(200, {
      'Content-Type': 'application/zip',
      'Content-Length': String(zipBuffer.length)
    })
    res.end(zipBuffer)
  })

  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve))
  const port = (server.address() as AddressInfo).port
  return {
    url: `http://127.0.0.1:${port}${
      redirectOnce ? '/redirect' : '/artifact.zip'
    }`,
    requests,
    close: async () =>
      new Promise<void>(resolve => server.close(() => resolve()))
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

  for (const f of files) {
    const srcPath = path.join(srcDir, `${path.basename(f.name)}.bin`)
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

    const srv = await startZipServer(zipBuf, {supportRange: true})
    const outDir = path.join(testDir, 'out-store')
    await fs.promises.mkdir(outDir, {recursive: true})

    try {
      const handled = await tryRangeExtract(srv.url, outDir, {
        minArchiveSize: 0,
        chunkSize: 512 * 1024,
        writeBufferSize: 256 * 1024,
        concurrency: 4
      })
      expect(handled).toBe(true)
      expect(srv.requests.some(r => r.method === 'HEAD')).toBe(false)
      expect(srv.requests[0].range).toBe('bytes=-65536')

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

  it('extracts DEFLATE archive and follows 302 redirects', async () => {
    const payloadA = Buffer.from('omarchy-iso-test-data-'.repeat(100000))
    const payloadB = Buffer.from('hello world')
    const zipBuf = await createStreamingArchiverZip(
      [
        {name: 'nested/a.txt', content: payloadA},
        {name: 'b.txt', content: payloadB}
      ],
      6
    )

    const srv = await startZipServer(zipBuf, {
      supportRange: true,
      redirectOnce: true
    })
    const outDir = path.join(testDir, 'out-deflate')
    await fs.promises.mkdir(outDir, {recursive: true})

    try {
      const handled = await tryRangeExtract(srv.url, outDir, {
        minArchiveSize: 0,
        concurrency: 2
      })
      expect(handled).toBe(true)
      expect(
        (await fs.promises.readFile(path.join(outDir, 'nested/a.txt'))).equals(
          payloadA
        )
      ).toBe(true)
      expect(
        (await fs.promises.readFile(path.join(outDir, 'b.txt'))).equals(
          payloadB
        )
      ).toBe(true)
    } finally {
      await srv.close()
    }
  })

  it('rejects Zip Slip traversal paths (including evil.zip)', async () => {
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
    const srv = await startZipServer(evilZip, {supportRange: true})
    const outDir = path.join(testDir, 'out-evil')
    await fs.promises.mkdir(outDir, {recursive: true})

    try {
      await expect(
        tryRangeExtract(srv.url, outDir, {minArchiveSize: 0})
      ).rejects.toThrow()
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
    const outDir = path.join(testDir, 'out-fallback')
    await fs.promises.mkdir(outDir, {recursive: true})

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
})
