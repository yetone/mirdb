/**
 * API client for communicating with MirDB HTTP adapter.
 */

import type { ServerConfig, HealthStatus, Metrics, LSMState, KVOperationRequest, KVOperationResponse } from '../types';

const BASE_URL = import.meta.env.VITE_API_BASE_URL || '';

async function apiFetch<T>(path: string, options?: RequestInit): Promise<T> {
  const response = await fetch(`${BASE_URL}${path}`, {
    headers: {
      'Content-Type': 'application/json',
    },
    ...options,
  });
  if (!response.ok) {
    throw new Error(`API error: ${response.status} ${response.statusText}`);
  }
  return response.json();
}

export function getConfig(): Promise<ServerConfig> {
  return apiFetch<ServerConfig>('/api/config');
}

export function getHealth(): Promise<HealthStatus> {
  return apiFetch<HealthStatus>('/api/health');
}

export function getMetrics(): Promise<Metrics> {
  return apiFetch<Metrics>('/api/metrics');
}

export function getLSMState(): Promise<LSMState> {
  return apiFetch<LSMState>('/api/lsm-state');
}

export function executeOperation(op: KVOperationRequest): Promise<KVOperationResponse> {
  return apiFetch<KVOperationResponse>('/api/operation', {
    method: 'POST',
    body: JSON.stringify(op),
  });
}
