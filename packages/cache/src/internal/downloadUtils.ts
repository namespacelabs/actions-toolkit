import * as core from '@actions/core'
import {HttpClient, HttpClientResponse} from '@actions/http-client'
import * as buffer from 'buffer'
import * as fs from 'fs'
import * as fsPromises from 'fs/promises'
import * as stream from 'stream/promises'

import * as utils from './cacheUtils'
import {SocketTimeout} from './constants'
import {retryHttpClientResponse} from './requestUtils'

/**
 * Pipes the body of a HTTP response to a stream
 *
 * @param response the HTTP response
 * @param output the writable stream
 */
async function pipeResponseToStream(
  response: HttpClientResponse,
  output: NodeJS.WritableStream,
  progress: DownloadProgress
): Promise<void> {
  await stream.pipeline(
    response.message,
    async function*(source: AsyncIterable<buffer.Buffer>) {
      for await (const chunk of source) {
        progress.addReceivedBytes(chunk.byteLength)
        yield chunk
      }
    },
    output
  )
}

/**
 * Class for tracking the download state and displaying stats.
 */
export class DownloadProgress {
  contentLength: number
  receivedBytes: number
  startTime: number
  displayedComplete: boolean
  timeoutHandle?: ReturnType<typeof setTimeout>

  constructor(contentLength: number) {
    this.contentLength = contentLength
    this.receivedBytes = 0
    this.displayedComplete = false
    this.startTime = Date.now()
  }

  /**
   * Adds the number of bytes received.
   *
   * @param receivedBytes the number of bytes received
   */
  addReceivedBytes(receivedBytes: number): void {
    this.receivedBytes += receivedBytes
  }

  /**
   * Returns the total number of bytes transferred.
   */
  getTransferredBytes(): number {
    return this.receivedBytes
  }

  /**
   * Returns true if the download is complete.
   */
  isDone(): boolean {
    return this.getTransferredBytes() === this.contentLength
  }

  /**
   * Prints the current download stats. Once the download completes, this will print one
   * last line and then stop.
   */
  display(): void {
    if (this.displayedComplete) {
      return
    }

    const transferredBytes = this.receivedBytes
    const percentage = (100 * (transferredBytes / this.contentLength)).toFixed(
      1
    )
    const elapsedTime = Date.now() - this.startTime
    const downloadSpeed = (
      transferredBytes /
      (1024 * 1024) /
      (elapsedTime / 1000)
    ).toFixed(1)

    core.info(
      `Received ${transferredBytes} of ${this.contentLength} (${percentage}%), ${downloadSpeed} MBs/sec`
    )

    if (this.isDone()) {
      this.displayedComplete = true
    }
  }

  /**
   * Starts the timer that displays the stats.
   *
   * @param delayInMs the delay between each write
   */
  startDisplayTimer(delayInMs = 1000): void {
    const displayCallback = (): void => {
      this.display()

      if (!this.isDone()) {
        this.timeoutHandle = setTimeout(displayCallback, delayInMs)
      }
    }

    this.timeoutHandle = setTimeout(displayCallback, delayInMs)
  }

  /**
   * Stops the timer that displays the stats. As this typically indicates the download
   * is complete, this will display one last line, unless the last line has already
   * been written.
   */
  stopDisplayTimer(): void {
    if (this.timeoutHandle) {
      clearTimeout(this.timeoutHandle)
      this.timeoutHandle = undefined
    }

    this.display()
  }
}

/**
 * Download the cache using the Actions toolkit http-client
 *
 * @param archiveLocation the URL for the cache
 * @param archivePath the local path where the cache is saved
 */
export async function downloadCacheHttpClient(
  archiveLocation: string,
  archivePath: string
): Promise<void> {
  const writeStream = fs.createWriteStream(archivePath)
  const httpClient = new HttpClient('actions/cache')
  const downloadResponse = await retryHttpClientResponse(
    'downloadCache',
    async () => httpClient.get(archiveLocation)
  )

  // Abort download if no traffic received over the socket.
  downloadResponse.message.socket.setTimeout(SocketTimeout, () => {
    downloadResponse.message.destroy()
    core.debug(`Aborting download, socket timed out after ${SocketTimeout} ms`)
  })

  const contentLength = Number(
    downloadResponse.message.headers['content-length']
  )
  if (!contentLength) {
    throw new Error(`Missing Content-Length header on response`)
  }
  const downloadProgress = new DownloadProgress(contentLength)
  downloadProgress.startDisplayTimer()
  try {
    await pipeResponseToStream(downloadResponse, writeStream, downloadProgress)
  } finally {
    downloadProgress.stopDisplayTimer()
  }

  const actualLength = utils.getArchiveFileSizeInBytes(archivePath)
  if (actualLength !== contentLength) {
    throw new Error(
      `Incomplete download. Expected file size: ${contentLength}, actual file size: ${actualLength}`
    )
  }
}

export async function downloadCacheConcurrent(
  archiveLocation: string,
  archivePath: string,
  archiveSize: number,
  concurrency: number
): Promise<void> {
  const fd = await fsPromises.open(archivePath, 'w')
  await fd.truncate(archiveSize)

  const httpClient = new HttpClient('actions/cache')
  const downloadProgress = new DownloadProgress(archiveSize)

  const chunkSize = Math.ceil(archiveSize / concurrency)
  const results = []
  for (let offset = 0; offset < archiveSize; offset += chunkSize) {
    const limit = Math.min(offset + chunkSize, archiveSize) - 1 // inclusive range
    core.debug(`Downloading chunk bytes=${offset}-${limit}`)
    results.push(
      (async () => {
        const downloadResponse = await retryHttpClientResponse(
          'downloadCache',
          async () =>
            httpClient.get(archiveLocation, {
              Range: `bytes=${offset}-${limit}`
            })
        )

        const w = fd.createWriteStream({autoClose: false, start: offset})
        await pipeResponseToStream(downloadResponse, w, downloadProgress)
      })()
    )
  }

  downloadProgress.startDisplayTimer()
  try {
    await Promise.all(results)
  } finally {
    downloadProgress.stopDisplayTimer()
    fd.close()
  }
}
