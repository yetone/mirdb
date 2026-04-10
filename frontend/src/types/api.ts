/**
 * API response type definitions
 *
 * Owner: Scenario 2 - Real-time System Metrics Dashboard
 *
 * Expected types:
 * - MetricsResponse
 * - KeyResponse, KeyListResponse
 * - ConfigResponse
 * - QueryResponse
 * - ApiError
 */

export interface ApiError {
  message: string;
  code?: string;
}

export interface MetricsResponse {
  total_keys: number;
  memory_used_bytes: number;
  memory_available_bytes: number;
  cache_hit_rate: number;
  active_connections: number;
  uptime_seconds: number;
  ls_levels: LsLevel[];
}

export interface LsLevel {
  level: number;
  size_bytes: number;
  file_count: number;
}
