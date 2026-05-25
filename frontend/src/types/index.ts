/**
 * Shared TypeScript type definitions for the MirDB Homepage.
 */

export interface ServerConfig {
  listen_address: string;
  max_lsm_levels: number;
  work_directory: string;
  sstable_size_mb: number;
  memtable_size_mb: number;
}

export interface Metrics {
  total_keys_in_memory: number;
  total_disk_storage_mb: number;
  active_compactions: number;
  request_latency_ms: number;
}

export interface LSMState {
  memtable: MemtableState;
  immutable_memtable: MemtableState | null;
  levels: SSTableLevel[];
}

export interface MemtableState {
  key_count: number;
  size_bytes: number;
}

export interface SSTableLevel {
  level: number;
  file_count: number;
  total_size_bytes: number;
}

export interface KVOperationRequest {
  operation: 'get' | 'set' | 'delete' | 'flush_all';
  key?: string;
  value?: string;
  flags?: number;
  exptime?: number;
}

export interface KVOperationResponse {
  success: boolean;
  result?: string;
  error?: string;
}

export interface HealthStatus {
  status: 'healthy' | 'unhealthy';
  timestamp: string;
}

export type Theme = 'light' | 'dark';
