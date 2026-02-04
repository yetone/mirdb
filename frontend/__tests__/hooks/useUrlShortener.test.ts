import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { renderHook, act, waitFor } from '@testing-library/react';
import { useUrlShortener } from '../../src/hooks/useUrlShortener';
import * as api from '../../src/api';

// Mock the API module
vi.mock('../../src/api', () => ({
  shortenUrlAnonymous: vi.fn(),
  getFullShortUrl: vi.fn((shortCode: string) => `http://localhost:3000/r/${shortCode}`),
}));

describe('useUrlShortener', () => {
  const mockShortenUrlAnonymous = api.shortenUrlAnonymous as ReturnType<typeof vi.fn>;

  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('initializes with correct default state', () => {
    const { result } = renderHook(() => useUrlShortener());

    expect(result.current.shortUrl).toBeNull();
    expect(result.current.isLoading).toBe(false);
    expect(result.current.error).toBeNull();
    expect(typeof result.current.shortenUrl).toBe('function');
    expect(typeof result.current.reset).toBe('function');
  });

  // Test Case 2: Submit form with URL - API POST request made to /api/urls/shorten with correct payload
  it('makes API call and sets shortUrl on success', async () => {
    const mockResponse = {
      id: 1,
      original_url: 'https://example.com/test',
      short_code: 'abc123',
      created_at: '2024-01-01T00:00:00Z',
      user_id: null,
      click_count: 0,
    };

    mockShortenUrlAnonymous.mockResolvedValueOnce(mockResponse);

    const { result } = renderHook(() => useUrlShortener());

    await act(async () => {
      await result.current.shortenUrl('https://example.com/test');
    });

    expect(mockShortenUrlAnonymous).toHaveBeenCalledWith('https://example.com/test');
    expect(result.current.shortUrl).toBe('http://localhost:3000/r/abc123');
    expect(result.current.isLoading).toBe(false);
    expect(result.current.error).toBeNull();
  });

  it('sets loading state while API call is in progress', async () => {
    let resolvePromise: (value: unknown) => void;
    const slowPromise = new Promise((resolve) => {
      resolvePromise = resolve;
    });

    mockShortenUrlAnonymous.mockReturnValueOnce(slowPromise);

    const { result } = renderHook(() => useUrlShortener());

    act(() => {
      result.current.shortenUrl('https://example.com/test');
    });

    // Should be loading
    expect(result.current.isLoading).toBe(true);

    // Resolve the promise
    await act(async () => {
      resolvePromise!({
        id: 1,
        original_url: 'https://example.com/test',
        short_code: 'abc123',
        created_at: '2024-01-01T00:00:00Z',
        user_id: null,
        click_count: 0,
      });
    });

    // Should no longer be loading
    expect(result.current.isLoading).toBe(false);
  });

  it('sets error state on API failure', async () => {
    mockShortenUrlAnonymous.mockRejectedValueOnce(new Error('Network error'));

    const { result } = renderHook(() => useUrlShortener());

    await act(async () => {
      await result.current.shortenUrl('https://example.com/test');
    });

    expect(result.current.shortUrl).toBeNull();
    expect(result.current.isLoading).toBe(false);
    expect(result.current.error).toBe('Network error');
  });

  it('handles non-Error rejection', async () => {
    mockShortenUrlAnonymous.mockRejectedValueOnce('Some string error');

    const { result } = renderHook(() => useUrlShortener());

    await act(async () => {
      await result.current.shortenUrl('https://example.com/test');
    });

    expect(result.current.error).toBe('Failed to shorten URL');
  });

  it('resets state correctly', async () => {
    const mockResponse = {
      id: 1,
      original_url: 'https://example.com/test',
      short_code: 'abc123',
      created_at: '2024-01-01T00:00:00Z',
      user_id: null,
      click_count: 0,
    };

    mockShortenUrlAnonymous.mockResolvedValueOnce(mockResponse);

    const { result } = renderHook(() => useUrlShortener());

    // First shorten a URL
    await act(async () => {
      await result.current.shortenUrl('https://example.com/test');
    });

    expect(result.current.shortUrl).not.toBeNull();

    // Now reset
    act(() => {
      result.current.reset();
    });

    expect(result.current.shortUrl).toBeNull();
    expect(result.current.isLoading).toBe(false);
    expect(result.current.error).toBeNull();
  });

  it('clears previous error on new request', async () => {
    mockShortenUrlAnonymous.mockRejectedValueOnce(new Error('First error'));

    const { result } = renderHook(() => useUrlShortener());

    // First request fails
    await act(async () => {
      await result.current.shortenUrl('https://example.com/test');
    });

    expect(result.current.error).toBe('First error');

    // Second request succeeds
    mockShortenUrlAnonymous.mockResolvedValueOnce({
      id: 1,
      original_url: 'https://example.com/test2',
      short_code: 'xyz789',
      created_at: '2024-01-01T00:00:00Z',
      user_id: null,
      click_count: 0,
    });

    await act(async () => {
      await result.current.shortenUrl('https://example.com/test2');
    });

    expect(result.current.error).toBeNull();
    expect(result.current.shortUrl).toBe('http://localhost:3000/r/xyz789');
  });
});
