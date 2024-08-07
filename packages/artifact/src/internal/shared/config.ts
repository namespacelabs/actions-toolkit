import os from 'os'
import * as fs from 'fs'

// Used for controlling the highWaterMark value of the zip that is being streamed
// The same value is used as the chunk size that is use during upload to blob storage
export function getUploadChunkSize(): number {
  return 8 * 1024 * 1024 // 8 MB Chunks
}

export function getRuntimeToken(): string {
  const token = process.env['ACTIONS_RUNTIME_TOKEN']
  if (!token) {
    throw new Error('Unable to get the ACTIONS_RUNTIME_TOKEN env variable')
  }
  return token
}

export function getNamespaceToken(): string {
  const tokenFile = process.env['NSC_TOKEN_FILE']
  if (!tokenFile) {
    throw new Error('Unable to get the NSC_TOKEN_FILE env variable')
  }
  const tokenJSON = fs.readFileSync(tokenFile, 'utf8')
  return JSON.parse(tokenJSON).bearer_token
}

export function getResultsServiceUrl(): string {
  const resultsUrl = process.env['ACTIONS_RESULTS_URL']
  if (!resultsUrl) {
    throw new Error('Unable to get the ACTIONS_RESULTS_URL env variable')
  }

  return new URL(resultsUrl).origin
}

export function getNamespaceResultsServiceUrl(): string {
  const resultsUrl = process.env['NAMESPACE_ACTIONS_RESULTS_URL']
  if (!resultsUrl) {
    throw new Error('Unable to get the NAMESPACE_ACTIONS_RESULTS_URL env variable')
  }

  return new URL(resultsUrl).origin
}

export function directZipUpload(): boolean {
  const set = process.env['NAMESPACE_ACTIONS_DIRECT_ZIP_UPLOAD']
  if (!set) {
    return false
  }

  return JSON.parse(set)
}

export function getNamespaceResultsService(): string | undefined {
  return process.env['NAMESPACE_ACTIONS_RESULTS_SERVICE']
}

export function isGhes(): boolean {
  const ghUrl = new URL(
    process.env['GITHUB_SERVER_URL'] || 'https://github.com'
  )

  const hostname = ghUrl.hostname.trimEnd().toUpperCase()
  const isGitHubHost = hostname === 'GITHUB.COM'
  const isGheHost = hostname.endsWith('.GHE.COM')
  const isLocalHost = hostname.endsWith('.LOCALHOST')

  return !isGitHubHost && !isGheHost && !isLocalHost
}

export function getGitHubWorkspaceDir(): string {
  const ghWorkspaceDir = process.env['GITHUB_WORKSPACE']
  if (!ghWorkspaceDir) {
    throw new Error('Unable to get the GITHUB_WORKSPACE env variable')
  }
  return ghWorkspaceDir
}

// Mimics behavior of azcopy: https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azcopy-optimize
// If your machine has fewer than 5 CPUs, then the value of this variable is set to 32.
// Otherwise, the default value is equal to 16 multiplied by the number of CPUs. The maximum value of this variable is 300.
export function getConcurrency(): number {
  const numCPUs = os.cpus().length

  if (numCPUs <= 4) {
    return 32
  }

  const concurrency = 16 * numCPUs
  return concurrency > 300 ? 300 : concurrency
}
