/**
 * Integration tests for Metrics API client.
 * Covers GET /api/metrics endpoint (REQ-6).
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { getMetrics } from '../../../src/api/client';

describe('getMetrics API integration', () => {
  beforeEach(() => {
    vi.stubGlobal('fetch', vi.fn());
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('calls GET /api/metrics endpoint', async () => {
    const mockResponse = {
      total_keys_in_memory: 12345,
      total_disk_storage_mb: 450,
      active_compactions: 2,
      request_latency_ms: 1.2,
    };

    (global.fetch as ReturnType<typeof vi.fn>).mockResolvedValueOnce({
      ok: true,
      json: async () => mockResponse,
    });

    const result = await getMetrics();

    expect(global.fetch).toHaveBeenCalledWith(
      '/api/metrics',
      expect.objectContaining({
        headers: { 'Content-Type': 'application/json' },
      })
    );
    expect(result).toEqual(mockResponse);
  });

  it('response contains total_keys field', async () => {
    const mockResponse = {
      total_keys_in_memory: 12345,
      total_disk_storage_mb: 450,
      active_compactions: 2,
      request_latency_ms: 1.2,
    };

    (global.fetch as ReturnType<typeof vi.fn>).mockResolvedValueOnce({
      ok: true,
      json: async () => mockResponse,
    });

    const result = await getMetrics();
    expect(result.total_keys_in_memory).toBe(12345);
  });

  it('response contains disk_usage_bytes field', async () => {
    const mockResponse = {
      total_keys_in_memory: 12345,
      total_disk_storage_mb: 450,
      active_compactions: 2,
      request_latency_ms: 1.2,
    };

    (global.fetch as ReturnType<typeof vi.fn>).mockResolvedValueOnce({
      ok: true,
      json: async () => mockResponse,
    });

    const result = await getMetrics();
    expect(result.total_disk_storage_mb).toBe(450);
  });

  it('response contains active_compactions field', async () => {
    const mockResponse = {
      total_keys_in_memory: 12345,
      total_disk_storage_mb: 450,
      active_compactions: 2,
      request_latency_ms: 1.2,
    };

    (global.fetch as ReturnType<typeof vi.fn>).mockResolvedValueOnce({
      ok: true,
      json: async () => mockResponse,
    });

    const result = await getMetrics();
    expect(result.active_compactions).toBe(2);
  });

  it('response contains avg_latency_ms field', async () => {
    const mockResponse = {
      total_keys_in_memory: 12345,
      total_disk_storage_mb: 450,
      active_compactions: 2,
      request_latency_ms: 1.2,
    };

    (global.fetch as ReturnType<typeof vi.fn>).mockResolvedValueOnce({
      ok: true,
      json: async () => mockResponse,
    });

    const result = await getMetrics();
    expect(result.request_latency_ms).toBe(1.2);
  });

  it('throws on API error response', async () => {
    (global.fetch as ReturnType<typeof vi.fn>).mockResolvedValueOnce({
      ok: false,
      status: 500,
      statusText: 'Internal Server Error',
    });

    await expect(getMetrics()).rejects.toThrow('API error: 500 Internal Server Error');
  });

  it('handles zero values in response', async () => {
    const mockResponse = {
      total_keys_in_memory: 0,
      total_disk_storage_mb: 0,
      active_compactions: 0,
      request_latency_ms: 0,
    };

    (global.fetch as ReturnType<typeof vi.fn>).mockResolvedValueOnce({
      ok: true,
      json: async () => mockResponse,
    });

    const result = await getMetrics();
    expect(result.total_keys_in_memory).toBe(0);
    expect(result.total_disk_storage_mb).toBe(0);
    expect(result.active_compactions).toBe(0);
    expect(result.request_latency_ms).toBe(0);
  });

  it('handles large metric values', async () => {
    const mockResponse = {
      total_keys_in_memory: 999999999,
      total_disk_storage_mb: 1048576,
      active_compactions: 99,
      request_latency_ms: 999.99,
    };

    (global.fetch as ReturnType<typeof vi.fn>).mockResolvedValueOnce({
      ok: true,
      json: async () => mockResponse,
    });

    const result = await getMetrics();
    expect(result.total_keys_in_memory).toBe(999999999);
    expect(result.total_disk_storage_mb).toBe(1048576);
    expect(result.active_compactions).toBe(99);
    expect(result.request_latency_ms).toBe(999.99);
  });
});
