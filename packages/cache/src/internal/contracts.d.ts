import {CompressionMethod} from './constants'
import {TypedResponse} from '@actions/http-client/lib/interfaces'
import {HttpClientError} from '@actions/http-client'

export interface ITypedResponseWithError<T> extends TypedResponse<T> {
  error?: HttpClientError
}

export interface ArtifactCacheEntry {
  cacheKey?: string
  scope?: string
  cacheVersion?: string
  creationTime?: string
  archiveLocation?: string
  size?: number
}

export interface ArtifactCacheList {
  totalCount: number
  artifactCaches?: ArtifactCacheEntry[]
}

export interface CommitCacheRequest {
  size: number;
  etags: string[];
}

export interface ReserveCacheRequest {
  key: string
  version?: string
  cacheSize?: number
}

export interface ReserveCacheResponse {
  cacheId: string
}

export interface InternalCacheOptions {
  compressionMethod?: CompressionMethod
  enableCrossOsArchive?: boolean
  cacheSize?: number
}

export interface ArchiveTool {
  path: string
  type: string
}
