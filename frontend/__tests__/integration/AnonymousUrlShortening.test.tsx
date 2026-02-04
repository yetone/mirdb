/**
 * Integration tests for Anonymous URL Shortening flow
 * Tests the complete user journey: entering URL -> submitting -> seeing result -> copying
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import React, { useState, useCallback } from 'react';
import { UrlShortenerForm } from '../../src/components/UrlShortenerForm';
import { ShortUrlResult } from '../../src/components/ShortUrlResult';
import * as api from '../../src/api';

// Mock the API module
vi.mock('../../src/api', () => ({
  shortenUrlAnonymous: vi.fn(),
  getFullShortUrl: vi.fn((shortCode: string) => `http://localhost:3000/r/${shortCode}`),
}));

// Integration test component that combines form and result
function AnonymousUrlShortener() {
  const [shortUrl, setShortUrl] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [copied, setCopied] = useState(false);

  const handleShorten = useCallback(async (url: string) => {
    setIsLoading(true);
    setError(null);
    try {
      const response = await api.shortenUrlAnonymous(url);
      const fullUrl = api.getFullShortUrl(response.short_code);
      setShortUrl(fullUrl);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to shorten URL');
    } finally {
      setIsLoading(false);
    }
  }, []);

  const handleCopy = useCallback(async () => {
    if (shortUrl) {
      await navigator.clipboard.writeText(shortUrl);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    }
  }, [shortUrl]);

  return (
    <div>
      <UrlShortenerForm
        onShorten={handleShorten}
        isLoading={isLoading}
        error={error}
      />
      {shortUrl && (
        <ShortUrlResult
          shortUrl={shortUrl}
          onCopy={handleCopy}
          copied={copied}
        />
      )}
    </div>
  );
}

describe('Anonymous URL Shortening Integration', () => {
  const mockShortenUrlAnonymous = api.shortenUrlAnonymous as ReturnType<typeof vi.fn>;
  let clipboardWriteTextMock: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    vi.clearAllMocks();
    // Mock clipboard for each test
    clipboardWriteTextMock = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(navigator, 'clipboard', {
      value: { writeText: clipboardWriteTextMock },
      writable: true,
      configurable: true,
    });
  });

  // Test Case 2: Submit form with URL 'https://example.com/test' - API POST request made
  // Test Case 3: Mock successful API response with shortCode 'abc123' - ShortUrlResult displays URL
  it('completes full URL shortening flow', async () => {
    mockShortenUrlAnonymous.mockResolvedValueOnce({
      id: 1,
      original_url: 'https://example.com/test',
      short_code: 'abc123',
      created_at: '2024-01-01T00:00:00Z',
      user_id: null,
      click_count: 0,
    });

    render(<AnonymousUrlShortener />);

    // Step 1: Enter URL
    const input = screen.getByPlaceholderText(/enter your long url/i);
    fireEvent.change(input, { target: { value: 'https://example.com/test' } });

    // Step 2: Submit form
    const submitButton = screen.getByRole('button', { name: /shorten/i });
    fireEvent.click(submitButton);

    // Verify API was called with correct payload
    expect(mockShortenUrlAnonymous).toHaveBeenCalledWith('https://example.com/test');

    // Step 3: Verify shortened URL is displayed
    await waitFor(() => {
      expect(screen.getByRole('link', { name: /http:\/\/localhost:3000\/r\/abc123/i })).toBeInTheDocument();
    });
  });

  // Test Case 5: Click copy button - navigator.clipboard.writeText called, 'Copied!' feedback shown
  it('copies shortened URL to clipboard and shows feedback', async () => {
    mockShortenUrlAnonymous.mockResolvedValueOnce({
      id: 1,
      original_url: 'https://example.com/test',
      short_code: 'abc123',
      created_at: '2024-01-01T00:00:00Z',
      user_id: null,
      click_count: 0,
    });

    render(<AnonymousUrlShortener />);

    // Shorten URL first
    const input = screen.getByPlaceholderText(/enter your long url/i);
    fireEvent.change(input, { target: { value: 'https://example.com/test' } });
    fireEvent.click(screen.getByRole('button', { name: /shorten/i }));

    // Wait for result to appear
    await waitFor(() => {
      expect(screen.getByRole('link')).toBeInTheDocument();
    });

    // Click copy button
    const copyButton = screen.getByRole('button', { name: /copy shortened url/i });
    fireEvent.click(copyButton);

    // Verify clipboard was called
    await waitFor(() => {
      expect(clipboardWriteTextMock).toHaveBeenCalledWith('http://localhost:3000/r/abc123');
    });

    // Verify feedback
    await waitFor(() => {
      expect(screen.getByText(/copied!/i)).toBeInTheDocument();
    });
  });

  // Test Case 4: Press Enter key while input is focused with valid URL - form submits
  it('submits form on Enter key press', async () => {
    mockShortenUrlAnonymous.mockResolvedValueOnce({
      id: 1,
      original_url: 'https://example.com/test',
      short_code: 'xyz789',
      created_at: '2024-01-01T00:00:00Z',
      user_id: null,
      click_count: 0,
    });

    render(<AnonymousUrlShortener />);

    const input = screen.getByPlaceholderText(/enter your long url/i);
    fireEvent.change(input, { target: { value: 'https://example.com/test' } });
    fireEvent.keyDown(input, { key: 'Enter', code: 'Enter' });

    expect(mockShortenUrlAnonymous).toHaveBeenCalledWith('https://example.com/test');

    await waitFor(() => {
      expect(screen.getByRole('link')).toBeInTheDocument();
    });
  });

  // Test Case 6: Submit form while loading state is active - button disabled, no duplicate API calls
  it('prevents duplicate submissions while loading', async () => {
    // Create a slow promise
    let resolvePromise: (value: unknown) => void;
    const slowPromise = new Promise((resolve) => {
      resolvePromise = resolve;
    });
    mockShortenUrlAnonymous.mockReturnValueOnce(slowPromise);

    render(<AnonymousUrlShortener />);

    const input = screen.getByPlaceholderText(/enter your long url/i);
    fireEvent.change(input, { target: { value: 'https://example.com/test' } });

    const submitButton = screen.getByRole('button', { name: /shorten/i });
    fireEvent.click(submitButton);

    // Button should be disabled during loading
    await waitFor(() => {
      expect(screen.getByRole('button')).toBeDisabled();
    });

    // Try clicking again while loading - should not add another call
    fireEvent.click(screen.getByRole('button'));

    expect(mockShortenUrlAnonymous).toHaveBeenCalledTimes(1);

    // Resolve the promise to clean up
    resolvePromise!({
      id: 1,
      original_url: 'https://example.com/test',
      short_code: 'abc123',
      created_at: '2024-01-01T00:00:00Z',
      user_id: null,
      click_count: 0,
    });
  });

  it('displays error when API call fails', async () => {
    mockShortenUrlAnonymous.mockRejectedValueOnce(new Error('Server error'));

    render(<AnonymousUrlShortener />);

    const input = screen.getByPlaceholderText(/enter your long url/i);
    fireEvent.change(input, { target: { value: 'https://example.com/test' } });
    fireEvent.click(screen.getByRole('button', { name: /shorten/i }));

    await waitFor(() => {
      expect(screen.getByRole('alert')).toBeInTheDocument();
      expect(screen.getByText(/server error/i)).toBeInTheDocument();
    });

    // Result should not be displayed
    expect(screen.queryByRole('link')).not.toBeInTheDocument();
  });
});
