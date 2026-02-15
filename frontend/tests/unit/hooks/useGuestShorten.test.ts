/**
 * Unit Tests for useGuestShorten Hook
 * Owner: Scenario 2 - Guest URL Shortening
 *
 * Test case 5: useGuestShorten hook with valid URL
 * Expected: Hook returns { shortenUrl, isLoading, error, result, reset } with correct state management
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { useGuestShorten } from '../../../src/hooks/useGuestShorten';
import { urlsApi } from '../../../src/api';

// Mock the API module
vi.mock('../../../src/api', () => ({
  urlsApi: {
    shorten: vi.fn(),
  },
}));

describe('useGuestShorten hook', () => {
  const mockApiResponse = {
    id: 1,
    original_url: 'https://example.com/very/long/path',
    short_code: 'abc123',
    created_at: '2026-02-15T00:00:00Z',
    user_id: null,
    click_count: 0,
  };

  beforeEach(() => {
    vi.clearAllMocks();
    // Mock window.location.origin
    Object.defineProperty(window, 'location', {
      value: { origin: 'http://localhost:3000' },
      writable: true,
    });
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('returns correct initial state', () => {
    const { result } = renderHook(() => useGuestShorten());

    expect(result.current.shortenUrl).toBeDefined();
    expect(typeof result.current.shortenUrl).toBe('function');
    expect(result.current.isLoading).toBe(false);
    expect(result.current.error).toBeNull();
    expect(result.current.result).toBeNull();
    expect(result.current.reset).toBeDefined();
    expect(typeof result.current.reset).toBe('function');
  });

  it('sets isLoading to true during API call', async () => {
    // Create a promise we can control
    let resolvePromise: (value: typeof mockApiResponse) => void;
    const controlledPromise = new Promise<typeof mockApiResponse>((resolve) => {
      resolvePromise = resolve;
    });

    vi.mocked(urlsApi.shorten).mockReturnValue(controlledPromise);

    const { result } = renderHook(() => useGuestShorten());

    expect(result.current.isLoading).toBe(false);

    // Start the shortening process
    let shortenPromise: Promise<unknown>;
    act(() => {
      shortenPromise = result.current.shortenUrl('https://example.com/long/path');
    });

    // Should be loading now
    expect(result.current.isLoading).toBe(true);

    // Resolve the API call
    await act(async () => {
      resolvePromise!(mockApiResponse);
      await shortenPromise;
    });

    // Should no longer be loading
    expect(result.current.isLoading).toBe(false);
  });

  it('successfully shortens a URL and updates result', async () => {
    vi.mocked(urlsApi.shorten).mockResolvedValue(mockApiResponse);

    const { result } = renderHook(() => useGuestShorten());

    await act(async () => {
      await result.current.shortenUrl('https://example.com/very/long/path');
    });

    expect(result.current.result).toEqual({
      originalUrl: 'https://example.com/very/long/path',
      shortUrl: 'http://localhost:3000/r/abc123',
      shortCode: 'abc123',
    });
    expect(result.current.error).toBeNull();
    expect(result.current.isLoading).toBe(false);
  });

  it('handles API errors correctly', async () => {
    const errorMessage = 'Network error';
    vi.mocked(urlsApi.shorten).mockRejectedValue(new Error(errorMessage));

    const { result } = renderHook(() => useGuestShorten());

    // The hook throws an error, we need to catch it
    let caughtError: Error | null = null;
    await act(async () => {
      try {
        await result.current.shortenUrl('https://example.com/path');
      } catch (e) {
        caughtError = e as Error;
      }
    });

    expect(caughtError).not.toBeNull();
    expect(caughtError?.message).toBe(errorMessage);
    expect(result.current.error).toBe(errorMessage);
    expect(result.current.result).toBeNull();
    expect(result.current.isLoading).toBe(false);
  });

  it('handles non-Error exceptions', async () => {
    vi.mocked(urlsApi.shorten).mockRejectedValue('Unknown error');

    const { result } = renderHook(() => useGuestShorten());

    let caughtError: Error | null = null;
    await act(async () => {
      try {
        await result.current.shortenUrl('https://example.com/path');
      } catch (e) {
        caughtError = e as Error;
      }
    });

    expect(caughtError).not.toBeNull();
    expect(result.current.error).toBe('Failed to shorten URL. Please try again.');
  });

  it('reset clears all state', async () => {
    vi.mocked(urlsApi.shorten).mockResolvedValue(mockApiResponse);

    const { result } = renderHook(() => useGuestShorten());

    // First, shorten a URL
    await act(async () => {
      await result.current.shortenUrl('https://example.com/path');
    });

    expect(result.current.result).not.toBeNull();

    // Then reset
    act(() => {
      result.current.reset();
    });

    expect(result.current.isLoading).toBe(false);
    expect(result.current.error).toBeNull();
    expect(result.current.result).toBeNull();
  });

  it('clears previous error on new request', async () => {
    // First request fails
    vi.mocked(urlsApi.shorten).mockRejectedValueOnce(new Error('First error'));

    const { result } = renderHook(() => useGuestShorten());

    await act(async () => {
      try {
        await result.current.shortenUrl('https://example.com/path');
      } catch {
        // Expected error
      }
    });

    expect(result.current.error).toBe('First error');

    // Second request succeeds
    vi.mocked(urlsApi.shorten).mockResolvedValueOnce(mockApiResponse);

    await act(async () => {
      await result.current.shortenUrl('https://example.com/another');
    });

    expect(result.current.error).toBeNull();
    expect(result.current.result).not.toBeNull();
  });

  it('calls API with correct parameters', async () => {
    vi.mocked(urlsApi.shorten).mockResolvedValue(mockApiResponse);

    const { result } = renderHook(() => useGuestShorten());
    const testUrl = 'https://example.com/test/path?param=value';

    await act(async () => {
      await result.current.shortenUrl(testUrl);
    });

    expect(urlsApi.shorten).toHaveBeenCalledWith({
      original_url: testUrl,
    });
  });
});
