/**
 * Integration tests for enhanced API client error handling.
 * Owner: Scenario 17 - Error Handling and Graceful Degradation
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { getHealth, getMetrics, executeOperation, ApiError, NetworkError, TimeoutError } from '../../../src/api/client';

describe('API client error handling', () => {
  beforeEach(() => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    vi.stubGlobal('fetch', vi.fn());
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  it('throws NetworkError when fetch fails with TypeError', async () => {
    (global.fetch as ReturnType<typeof vi.fn>).mockRejectedValue(
      new TypeError('Failed to fetch')
    );

    await expect(getMetrics({ retries: 0 })).rejects.toThrow('Server unreachable. Please check your connection.');
  });

  it('throws NetworkError with user-friendly message for connection refused', async () => {
    (global.fetch as ReturnType<typeof vi.fn>).mockRejectedValue(
      new TypeError('Failed to fetch')
    );

    try {
      await getHealth();
    } catch (err) {
      expect(err).toBeInstanceOf(NetworkError);
      expect((err as Error).message).toBe('Server unreachable. Please check your connection.');
    }
  });

  it('throws TimeoutError when request exceeds timeout', async () => {
    function createAbortError(): Error {
      const err = new Error('The operation was aborted');
      err.name = 'AbortError';
      return err;
    }

    (global.fetch as ReturnType<typeof vi.fn>).mockImplementation((_url, options) => {
      return new Promise((_, reject) => {
        const signal = options?.signal as AbortSignal | undefined;
        if (signal?.aborted) {
          reject(createAbortError());
          return;
        }
        const timeoutId = setTimeout(() => {
          reject(new Error('never resolves'));
        }, 100000);
        signal?.addEventListener('abort', () => {
          clearTimeout(timeoutId);
          reject(createAbortError());
        });
      });
    });

    const promise = getMetrics({ timeout: 100, retries: 0 });

    await vi.advanceTimersByTimeAsync(200);

    await expect(promise).rejects.toThrow('Request timed out. Please try again.');
  });

  it('returns HTTP 400 with clear error message for malformed JSON', async () => {
    (global.fetch as ReturnType<typeof vi.fn>).mockResolvedValueOnce({
      ok: false,
      status: 400,
      statusText: 'Bad Request',
      text: async () => 'Invalid JSON: unexpected token at position 5',
    });

    try {
      await executeOperation({ op: 'get', key: 'test' }, { retries: 0 });
    } catch (err) {
      expect(err).toBeInstanceOf(ApiError);
      expect((err as ApiError).status).toBe(400);
      expect((err as Error).message).toContain('Invalid JSON');
    }
  });

  it('returns HTTP 404 for non-existent endpoint', async () => {
    (global.fetch as ReturnType<typeof vi.fn>).mockResolvedValueOnce({
      ok: false,
      status: 404,
      statusText: 'Not Found',
      text: async () => 'Endpoint not found',
    });

    try {
      await getMetrics({ retries: 0 });
    } catch (err) {
      expect(err).toBeInstanceOf(ApiError);
      expect((err as ApiError).status).toBe(404);
    }
  });

  it('retries on network error up to specified retries', async () => {
    (global.fetch as ReturnType<typeof vi.fn>)
      .mockRejectedValueOnce(new TypeError('Failed to fetch'))
      .mockRejectedValueOnce(new TypeError('Failed to fetch'))
      .mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: async () => ({ total_keys_in_memory: 100, total_disk_storage_mb: 10, active_compactions: 0, request_latency_ms: 1 }),
      });

    const promise = getMetrics({ retries: 2 });
    await vi.advanceTimersByTimeAsync(5000);

    const result = await promise;
    expect(result.total_keys_in_memory).toBe(100);
    expect(global.fetch).toHaveBeenCalledTimes(3);
  });

  it('does not retry on 4xx client errors', async () => {
    (global.fetch as ReturnType<typeof vi.fn>).mockResolvedValueOnce({
      ok: false,
      status: 400,
      statusText: 'Bad Request',
      text: async () => 'Bad request',
    });

    await expect(executeOperation({ op: 'get', key: 'test' })).rejects.toThrow();
    expect(global.fetch).toHaveBeenCalledTimes(1);
  });

  it('clears loading state after timeout', async () => {
    function createAbortError(): Error {
      const err = new Error('The operation was aborted');
      err.name = 'AbortError';
      return err;
    }

    (global.fetch as ReturnType<typeof vi.fn>).mockImplementation((_url, options) => {
      return new Promise((_, reject) => {
        const signal = options?.signal as AbortSignal | undefined;
        if (signal?.aborted) {
          reject(createAbortError());
          return;
        }
        const timeoutId = setTimeout(() => {
          reject(new Error('never resolves'));
        }, 100000);
        signal?.addEventListener('abort', () => {
          clearTimeout(timeoutId);
          reject(createAbortError());
        });
      });
    });

    const promise = getMetrics({ timeout: 100, retries: 0 });
    await vi.advanceTimersByTimeAsync(200);

    await expect(promise).rejects.toThrow('Request timed out. Please try again.');
  });

  it('health check uses shorter timeout and no retries', async () => {
    (global.fetch as ReturnType<typeof vi.fn>).mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => ({ status: 'healthy', timestamp: new Date().toISOString() }),
    });

    await getHealth();

    expect(global.fetch).toHaveBeenCalledTimes(1);
    const call = vi.mocked(global.fetch).mock.calls[0];
    expect(call[0]).toContain('/api/health');
  });
});
