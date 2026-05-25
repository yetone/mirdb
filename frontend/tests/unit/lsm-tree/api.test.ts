/**
 * Integration tests for LSM state API client.
 * Covers GET /api/lsm-state endpoint (REQ-5).
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { getLSMState } from '../../../src/api/client';

describe('getLSMState API integration', () => {
  beforeEach(() => {
    vi.stubGlobal('fetch', vi.fn());
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('calls GET /api/lsm-state endpoint', async () => {
    const mockResponse = {
      memtable: { key_count: 50, size_bytes: 4096 },
      immutable_memtable: null,
      levels: [
        { level: 0, file_count: 3, total_size_bytes: 102400 },
        { level: 1, file_count: 2, total_size_bytes: 204800 },
      ],
    };

    (global.fetch as ReturnType<typeof vi.fn>).mockResolvedValueOnce({
      ok: true,
      json: async () => mockResponse,
    });

    const result = await getLSMState();

    expect(global.fetch).toHaveBeenCalledWith(
      '/api/lsm-state',
      expect.objectContaining({
        headers: { 'Content-Type': 'application/json' },
      })
    );
    expect(result).toEqual(mockResponse);
  });

  it('response contains memtable key count', async () => {
    const mockResponse = {
      memtable: { key_count: 50, size_bytes: 4096 },
      immutable_memtable: null,
      levels: [],
    };

    (global.fetch as ReturnType<typeof vi.fn>).mockResolvedValueOnce({
      ok: true,
      json: async () => mockResponse,
    });

    const result = await getLSMState();
    expect(result.memtable.key_count).toBe(50);
  });

  it('response contains immutable_memtable status', async () => {
    const mockResponse = {
      memtable: { key_count: 50, size_bytes: 4096 },
      immutable_memtable: { key_count: 30, size_bytes: 2048 },
      levels: [],
    };

    (global.fetch as ReturnType<typeof vi.fn>).mockResolvedValueOnce({
      ok: true,
      json: async () => mockResponse,
    });

    const result = await getLSMState();
    expect(result.immutable_memtable).not.toBeNull();
    expect(result.immutable_memtable?.key_count).toBe(30);
  });

  it('response contains sst_levels array with file counts per level', async () => {
    const mockResponse = {
      memtable: { key_count: 50, size_bytes: 4096 },
      immutable_memtable: null,
      levels: [
        { level: 0, file_count: 3, total_size_bytes: 102400 },
        { level: 1, file_count: 2, total_size_bytes: 204800 },
        { level: 2, file_count: 1, total_size_bytes: 512000 },
      ],
    };

    (global.fetch as ReturnType<typeof vi.fn>).mockResolvedValueOnce({
      ok: true,
      json: async () => mockResponse,
    });

    const result = await getLSMState();
    expect(Array.isArray(result.levels)).toBe(true);
    expect(result.levels).toHaveLength(3);
    expect(result.levels[0]).toMatchObject({ level: 0, file_count: 3 });
    expect(result.levels[1]).toMatchObject({ level: 1, file_count: 2 });
    expect(result.levels[2]).toMatchObject({ level: 2, file_count: 1 });
  });

  it('throws on API error response', async () => {
    (global.fetch as ReturnType<typeof vi.fn>).mockResolvedValueOnce({
      ok: false,
      status: 500,
      statusText: 'Internal Server Error',
    });

    await expect(getLSMState()).rejects.toThrow('API error: 500 Internal Server Error');
  });

  it('handles empty levels array', async () => {
    const mockResponse = {
      memtable: { key_count: 0, size_bytes: 0 },
      immutable_memtable: null,
      levels: [],
    };

    (global.fetch as ReturnType<typeof vi.fn>).mockResolvedValueOnce({
      ok: true,
      json: async () => mockResponse,
    });

    const result = await getLSMState();
    expect(result.levels).toEqual([]);
    expect(result.memtable.key_count).toBe(0);
  });
});
