/**
 * Unit tests for useUrlShortener hook.
 * Owner: Scenario 2 - Guest URL Creation Flow
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { renderHook, act, waitFor } from '@testing-library/react';
import { useUrlShortener } from '../../../src/hooks/useUrlShortener';

describe('useUrlShortener', () => {
  const mockFetch = vi.fn();
  const originalFetch = global.fetch;

  beforeEach(() => {
    global.fetch = mockFetch;
    mockFetch.mockReset();
  });

  afterEach(() => {
    global.fetch = originalFetch;
  });

  it('should initialize with default state', () => {
    const { result } = renderHook(() => useUrlShortener());

    expect(result.current.isLoading).toBe(false);
    expect(result.current.error).toBeNull();
    expect(result.current.result).toBeNull();
    expect(typeof result.current.shortenUrl).toBe('function');
    expect(typeof result.current.reset).toBe('function');
  });

  it('should return error for empty URL', async () => {
    const { result } = renderHook(() => useUrlShortener());

    await act(async () => {
      const response = await result.current.shortenUrl('');
      expect(response).toBeNull();
    });

    expect(result.current.error).toBe('Please enter a URL');
    expect(result.current.isLoading).toBe(false);
    expect(mockFetch).not.toHaveBeenCalled();
  });

  it('should return error for whitespace-only URL', async () => {
    const { result } = renderHook(() => useUrlShortener());

    await act(async () => {
      const response = await result.current.shortenUrl('   ');
      expect(response).toBeNull();
    });

    expect(result.current.error).toBe('Please enter a URL');
  });

  it('should return error for invalid URL format', async () => {
    const { result } = renderHook(() => useUrlShortener());

    await act(async () => {
      const response = await result.current.shortenUrl('not-a-valid-url');
      expect(response).toBeNull();
    });

    expect(result.current.error).toBe('Please enter a valid URL');
    expect(mockFetch).not.toHaveBeenCalled();
  });

  it('should shorten valid URL successfully', async () => {
    const mockResponse = {
      short_code: 'abc123',
      original_url: 'https://example.com/long-path',
    };

    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve(mockResponse),
    });

    const { result } = renderHook(() => useUrlShortener());

    await act(async () => {
      const response = await result.current.shortenUrl('https://example.com/long-path');
      expect(response).not.toBeNull();
      expect(response?.shortCode).toBe('abc123');
    });

    expect(result.current.isLoading).toBe(false);
    expect(result.current.error).toBeNull();
    expect(result.current.result).not.toBeNull();
    expect(result.current.result?.shortCode).toBe('abc123');
    expect(result.current.result?.isGuest).toBe(true);

    // Verify fetch was called with correct parameters
    expect(mockFetch).toHaveBeenCalledWith('/api/urls/', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        original_url: 'https://example.com/long-path',
        guest: true,
      }),
    });
  });

  it('should normalize URL without protocol', async () => {
    const mockResponse = {
      short_code: 'abc123',
      original_url: 'https://example.com',
    };

    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve(mockResponse),
    });

    const { result } = renderHook(() => useUrlShortener());

    await act(async () => {
      await result.current.shortenUrl('example.com');
    });

    // Verify URL was normalized with https://
    expect(mockFetch).toHaveBeenCalledWith('/api/urls/', expect.objectContaining({
      body: JSON.stringify({
        original_url: 'https://example.com',
        guest: true,
      }),
    }));
  });

  it('should set loading state during API call', async () => {
    let resolvePromise: (value: unknown) => void;
    const pendingPromise = new Promise((resolve) => {
      resolvePromise = resolve;
    });

    mockFetch.mockReturnValueOnce(pendingPromise);

    const { result } = renderHook(() => useUrlShortener());

    // Start the API call (don't await)
    let shortenPromise: Promise<unknown>;
    act(() => {
      shortenPromise = result.current.shortenUrl('https://example.com');
    });

    // Check loading state is true
    expect(result.current.isLoading).toBe(true);

    // Resolve the promise
    await act(async () => {
      resolvePromise!({
        ok: true,
        json: () => Promise.resolve({ short_code: 'abc', original_url: 'https://example.com' }),
      });
      await shortenPromise;
    });

    expect(result.current.isLoading).toBe(false);
  });

  it('should handle API error response', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 400,
      json: () => Promise.resolve({ detail: 'Invalid URL format' }),
    });

    const { result } = renderHook(() => useUrlShortener());

    await act(async () => {
      const response = await result.current.shortenUrl('https://example.com');
      expect(response).toBeNull();
    });

    expect(result.current.error).toBe('Invalid URL format');
    expect(result.current.result).toBeNull();
  });

  it('should handle network error', async () => {
    mockFetch.mockRejectedValueOnce(new Error('Network error'));

    const { result } = renderHook(() => useUrlShortener());

    await act(async () => {
      const response = await result.current.shortenUrl('https://example.com');
      expect(response).toBeNull();
    });

    expect(result.current.error).toBe('Network error');
    expect(result.current.result).toBeNull();
  });

  it('should reset state when reset is called', async () => {
    const mockResponse = {
      short_code: 'abc123',
      original_url: 'https://example.com',
    };

    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve(mockResponse),
    });

    const { result } = renderHook(() => useUrlShortener());

    await act(async () => {
      await result.current.shortenUrl('https://example.com');
    });

    expect(result.current.result).not.toBeNull();

    act(() => {
      result.current.reset();
    });

    expect(result.current.result).toBeNull();
    expect(result.current.error).toBeNull();
    expect(result.current.isLoading).toBe(false);
  });

  it('should validate various URL formats', async () => {
    const { result } = renderHook(() => useUrlShortener());

    // Test valid URLs
    const validUrls = [
      'https://example.com',
      'http://example.com',
      'https://subdomain.example.com',
      'https://example.com/path',
      'https://example.com/path?query=1',
      'example.com',
    ];

    for (const url of validUrls) {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({ short_code: 'test', original_url: url }),
      });

      await act(async () => {
        result.current.reset();
        await result.current.shortenUrl(url);
      });

      expect(result.current.error).toBeNull();
    }
  });

  it('should reject invalid URL formats', async () => {
    const { result } = renderHook(() => useUrlShortener());

    const invalidUrls = [
      'not-a-url',
      'ftp://invalid.com',
      '://invalid',
      '.com',
    ];

    for (const url of invalidUrls) {
      await act(async () => {
        result.current.reset();
        const response = await result.current.shortenUrl(url);
        expect(response).toBeNull();
      });

      expect(result.current.error).toBe('Please enter a valid URL');
    }
  });
});
