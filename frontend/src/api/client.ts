/**
 * API client for communicating with MirDB HTTP adapter.
 *
 * Enhanced with timeout, retry logic, and structured error handling.
 * Covers NFR-4 (graceful degradation).
 */

import type {
  ServerConfig,
  HealthStatus,
  Metrics,
  LSMState,
  KVOperationRequest,
  KVOperationResponse,
} from '../types';
import {
  ApiError,
  ApiErrorCode,
  NetworkError,
  TimeoutError,
  responseToError,
  getUserFriendlyMessage,
} from './errors';

const BASE_URL = import.meta.env.VITE_API_BASE_URL || '';
const DEFAULT_TIMEOUT_MS = 10000;
const DEFAULT_RETRIES = 2;
const RETRY_DELAY_MS = 1000;

export interface FetchOptions extends RequestInit {
  timeout?: number;
  retries?: number;
}

/**
 * Fetch with timeout support using AbortController.
 */
async function fetchWithTimeout(
  url: string,
  options: RequestInit,
  timeoutMs: number
): Promise<Response> {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const response = await fetch(url, {
      ...options,
      signal: controller.signal,
    });
    return response;
  } catch (err) {
    if (err instanceof Error && err.name === 'AbortError') {
      throw new TimeoutError();
    }
    throw err;
  } finally {
    clearTimeout(timeoutId);
  }
}

/**
 * Execute a fetch with retry logic for network/timeout errors.
 * Only retries NetworkError, TimeoutError, and retryable ApiErrors (5xx).
 * Does not retry generic errors to avoid blocking callers with fake timers.
 */
async function fetchWithRetry(
  url: string,
  options: RequestInit,
  timeoutMs: number,
  retries: number
): Promise<Response> {
  let lastError: Error | null = null;

  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const response = await fetchWithTimeout(url, options, timeoutMs);
      return response;
    } catch (err) {
      lastError = err instanceof Error ? err : new Error(String(err));

      // Convert TypeError from fetch to NetworkError for retry logic
      if (lastError instanceof TypeError && lastError.message.toLowerCase().includes('fetch')) {
        lastError = new NetworkError();
      }

      // Only retry specific retryable error types
      const isRetryable =
        lastError instanceof NetworkError ||
        lastError instanceof TimeoutError ||
        (lastError instanceof ApiError && lastError.retryable);

      if (!isRetryable || attempt >= retries) {
        break;
      }

      await delay(RETRY_DELAY_MS * (attempt + 1));
    }
  }

  throw lastError || new Error('Request failed');
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Core API fetch function with timeout, retry, and structured error handling.
 */
async function apiFetch<T>(
  path: string,
  options?: FetchOptions
): Promise<T> {
  const url = `${BASE_URL}${path}`;
  const timeoutMs = options?.timeout ?? DEFAULT_TIMEOUT_MS;
  const retries = options?.retries ?? DEFAULT_RETRIES;

  try {
    const response = await fetchWithRetry(
      url,
      {
        headers: {
          'Content-Type': 'application/json',
        },
        ...options,
      },
      timeoutMs,
      retries
    );

    if (!response.ok) {
      let bodyText: string | undefined;
      try {
        bodyText = await response.text();
      } catch {
        // ignore parse error, use status text
      }
      throw responseToError(response, bodyText);
    }

    try {
      return await response.json();
    } catch {
      throw new ApiError('Failed to parse JSON response', ApiErrorCode.PARSE_ERROR, response.status);
    }
  } catch (err) {
    if (err instanceof ApiError) {
      throw err;
    }
    if (err instanceof TypeError && err.message.includes('fetch')) {
      throw new NetworkError();
    }
    throw err;
  }
}

export function getConfig(options?: FetchOptions): Promise<ServerConfig> {
  return apiFetch<ServerConfig>('/api/config', options);
}

export function getHealth(options?: FetchOptions): Promise<HealthStatus> {
  return apiFetch<HealthStatus>('/api/health', { ...options, timeout: 5000, retries: 0 });
}

export function getMetrics(options?: FetchOptions): Promise<Metrics> {
  return apiFetch<Metrics>('/api/metrics', options);
}

export function getLSMState(options?: FetchOptions): Promise<LSMState> {
  return apiFetch<LSMState>('/api/lsm-state', options);
}

export function executeOperation(
  op: KVOperationRequest,
  options?: FetchOptions
): Promise<KVOperationResponse> {
  return apiFetch<KVOperationResponse>('/api/operation', {
    method: 'POST',
    body: JSON.stringify(op),
    ...options,
  });
}

export function getValue(
  key: string,
  options?: FetchOptions
): Promise<KVOperationResponse> {
  return executeOperation({ op: 'get', key }, options);
}

export function deleteKey(
  key: string,
  options?: FetchOptions
): Promise<KVOperationResponse> {
  return executeOperation({ op: 'delete', key }, options);
}

export function flushAll(options?: FetchOptions): Promise<KVOperationResponse> {
  return executeOperation({ op: 'flush_all' }, options);
}

// Re-export error utilities
export {
  ApiError,
  NetworkError,
  TimeoutError,
  getUserFriendlyMessage,
};
