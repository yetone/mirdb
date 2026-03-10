/**
 * Unit tests for useUrlShortener hook
 * Owner: Scenario 1 - Hero Section URL Shortening
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { renderHook, act, waitFor } from '@testing-library/react';
import { useUrlShortener } from '../../../src/hooks/useUrlShortener';
import { server } from '../../mocks/server';
import { errorHandlers } from '../../mocks/handlers';

describe('useUrlShortener', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('should initialize with default state', () => {
    const { result } = renderHook(() => useUrlShortener());

    expect(result.current.isLoading).toBe(false);
    expect(result.current.error).toBeNull();
    expect(result.current.result).toBeNull();
    expect(typeof result.current.shortenUrl).toBe('function');
    expect(typeof result.current.reset).toBe('function');
  });

  it('should successfully shorten a URL', async () => {
    const { result } = renderHook(() => useUrlShortener());
    const testUrl = 'https://example.com/very/long/path';

    await act(async () => {
      await result.current.shortenUrl(testUrl);
    });

    expect(result.current.isLoading).toBe(false);
    expect(result.current.error).toBeNull();
    expect(result.current.result).toEqual({
      short_url: 'http://localhost:8000/abc123',
      short_code: 'abc123',
      original_url: testUrl,
    });
  });

  it('should show loading state during API call', async () => {
    const { result } = renderHook(() => useUrlShortener());

    // Start the request but don't wait for it
    let shortenPromise: Promise<unknown>;
    act(() => {
      shortenPromise = result.current.shortenUrl('https://example.com');
    });

    // Check loading state is true immediately
    expect(result.current.isLoading).toBe(true);

    // Wait for the request to complete
    await act(async () => {
      await shortenPromise;
    });

    expect(result.current.isLoading).toBe(false);
  });

  it('should handle server errors', async () => {
    server.use(errorHandlers.serverError);

    const { result } = renderHook(() => useUrlShortener());

    await act(async () => {
      await result.current.shortenUrl('https://example.com');
    });

    expect(result.current.isLoading).toBe(false);
    expect(result.current.error).toBeTruthy();
    expect(result.current.result).toBeNull();
  });

  it('should handle network errors', async () => {
    server.use(errorHandlers.networkError);

    const { result } = renderHook(() => useUrlShortener());

    await act(async () => {
      await result.current.shortenUrl('https://example.com');
    });

    expect(result.current.isLoading).toBe(false);
    expect(result.current.error).toBeTruthy();
    expect(result.current.result).toBeNull();
  });

  it('should reset state correctly', async () => {
    const { result } = renderHook(() => useUrlShortener());

    // First, make a successful request
    await act(async () => {
      await result.current.shortenUrl('https://example.com');
    });

    expect(result.current.result).not.toBeNull();

    // Now reset
    act(() => {
      result.current.reset();
    });

    expect(result.current.isLoading).toBe(false);
    expect(result.current.error).toBeNull();
    expect(result.current.result).toBeNull();
  });

  it('should clear previous results when making a new request', async () => {
    const { result } = renderHook(() => useUrlShortener());

    // First request
    await act(async () => {
      await result.current.shortenUrl('https://example.com/first');
    });

    const firstResult = result.current.result;
    expect(firstResult).not.toBeNull();

    // Second request
    await act(async () => {
      await result.current.shortenUrl('https://example.com/second');
    });

    // Should have new result
    expect(result.current.result).not.toBeNull();
    expect(result.current.result?.original_url).toBe('https://example.com/second');
  });
});
