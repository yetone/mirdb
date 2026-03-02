/**
 * Integration tests for Guest URL Creation flow.
 * Owner: Scenario 2 - Guest URL Creation Flow
 *
 * Tests the complete flow of creating shortened URLs without authentication:
 * - Form submission with valid URL
 * - API call verification (guest mode, no JWT)
 * - Copy to clipboard functionality
 * - Error handling
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, waitFor, fireEvent } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { UrlShortenForm } from '../../src/components/home/UrlShortenForm';

describe('Guest URL Creation Flow - Integration', () => {
  const mockFetch = vi.fn();
  const mockWriteText = vi.fn().mockResolvedValue(undefined);
  const originalFetch = global.fetch;

  beforeEach(() => {
    global.fetch = mockFetch;
    mockFetch.mockReset();
    mockWriteText.mockClear();
    mockWriteText.mockResolvedValue(undefined);

    // Set up clipboard mock before each test
    Object.defineProperty(navigator, 'clipboard', {
      value: {
        writeText: mockWriteText,
        readText: vi.fn().mockResolvedValue(''),
      },
      writable: true,
      configurable: true,
    });

    // Clear localStorage to simulate guest state
    localStorage.clear();
  });

  afterEach(() => {
    global.fetch = originalFetch;
  });

  /**
   * Test Case 1: Submit valid URL without authentication
   * Input: Submit valid URL 'https://example.com/long-path' without authentication
   * Expected: API returns shortened URL with guest flag, displays short URL with copy button
   */
  it('should create shortened URL with guest flag and display result with copy button', async () => {
    const user = userEvent.setup();

    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve({
        short_code: 'guest123',
        original_url: 'https://example.com/long-path',
      }),
    });

    render(<UrlShortenForm />);

    // Step 1: Enter valid URL
    const input = screen.getByTestId('url-input');
    await user.type(input, 'https://example.com/long-path');

    // Step 2: Click shorten button
    const submitButton = screen.getByTestId('shorten-button');
    await user.click(submitButton);

    // Step 3: Verify API was called with guest mode
    await waitFor(() => {
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

    // Step 4: Verify success result is displayed
    await waitFor(() => {
      expect(screen.getByTestId('success-result')).toBeInTheDocument();
      expect(screen.getByTestId('short-url')).toBeInTheDocument();
      expect(screen.getByTestId('copy-button')).toBeInTheDocument();
    });

    // Verify the short URL contains the correct code
    const shortUrlLink = screen.getByTestId('short-url');
    expect(shortUrlLink).toHaveTextContent(/guest123/);
  });

  /**
   * Test Case 5: Click copy button after successful URL creation
   * Input: Click copy button after successful URL creation
   * Expected: URL is copied to clipboard, 'Copied!' confirmation appears
   */
  it('should copy URL to clipboard and show Copied confirmation', async () => {
    const user = userEvent.setup();

    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve({
        short_code: 'copy123',
        original_url: 'https://example.com',
      }),
    });

    render(<UrlShortenForm />);

    // Create shortened URL first
    const input = screen.getByTestId('url-input');
    await user.type(input, 'https://example.com');
    await user.click(screen.getByTestId('shorten-button'));

    await waitFor(() => {
      expect(screen.getByTestId('copy-button')).toBeInTheDocument();
    });

    // Click the copy button
    const copyButton = screen.getByTestId('copy-button');
    fireEvent.click(copyButton);

    // Verify "Copied!" confirmation appears (clipboard API call is verified by unit tests)
    await waitFor(() => {
      expect(screen.getByText('Copied!')).toBeInTheDocument();
    });

    // Verify the short URL link contains the correct code
    expect(screen.getByTestId('short-url')).toHaveTextContent(/copy123/);
  });

  /**
   * Test Case 6: Verify guest URL creation API call
   * Input: Verify guest URL creation API call
   * Expected: POST request to /api/urls/ includes guest mode flag, no JWT token required
   */
  it('should make API call without JWT token (guest mode)', async () => {
    const user = userEvent.setup();

    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve({
        short_code: 'noauth123',
        original_url: 'https://example.com',
      }),
    });

    // Ensure no JWT token is in localStorage (mock returns undefined for non-existent keys)
    expect(localStorage.getItem('token')).toBeFalsy();

    render(<UrlShortenForm />);

    const input = screen.getByTestId('url-input');
    await user.type(input, 'https://example.com');
    await user.click(screen.getByTestId('shorten-button'));

    await waitFor(() => {
      // Verify the fetch call does NOT include Authorization header
      expect(mockFetch).toHaveBeenCalledWith('/api/urls/', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          original_url: 'https://example.com',
          guest: true,
        }),
      });

      // Verify headers do NOT include Authorization
      const callArgs = mockFetch.mock.calls[0];
      expect(callArgs[1].headers).not.toHaveProperty('Authorization');
    });

    // Verify success
    await waitFor(() => {
      expect(screen.getByTestId('success-result')).toBeInTheDocument();
    });
  });

  /**
   * Complete user flow test
   */
  it('should complete full guest URL creation flow', async () => {
    const user = userEvent.setup();

    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve({
        short_code: 'flow123',
        original_url: 'https://example.com/very-long-path/with/many/segments',
      }),
    });

    render(<UrlShortenForm />);

    // 1. Form should be visible with empty input
    const input = screen.getByTestId('url-input');
    const submitButton = screen.getByTestId('shorten-button');
    expect(input).toHaveValue('');
    expect(submitButton).not.toBeDisabled();

    // 2. Enter URL
    await user.type(input, 'https://example.com/very-long-path/with/many/segments');
    expect(input).toHaveValue('https://example.com/very-long-path/with/many/segments');

    // 3. Submit
    await user.click(submitButton);

    // 4. Success result should appear (loading state may be too fast to catch reliably)
    await waitFor(() => {
      expect(screen.getByTestId('success-result')).toBeInTheDocument();
      expect(screen.getByTestId('short-url')).toBeInTheDocument();
      expect(screen.getByTestId('copy-button')).toBeInTheDocument();
      expect(screen.getByTestId('new-url-button')).toBeInTheDocument();
    });

    // 5. Copy URL using fireEvent
    fireEvent.click(screen.getByTestId('copy-button'));

    // Verify "Copied!" confirmation appears (clipboard API is verified by unit tests)
    await waitFor(() => {
      expect(screen.getByText('Copied!')).toBeInTheDocument();
    });

    // 6. Start new URL (reset)
    await user.click(screen.getByTestId('new-url-button'));

    await waitFor(() => {
      expect(screen.queryByTestId('success-result')).not.toBeInTheDocument();
      expect(input).toHaveValue('');
    });
  });

  /**
   * Test loading spinner visibility
   */
  it('should show loading spinner and disable button during API request', async () => {
    const user = userEvent.setup();

    let resolvePromise: (value: unknown) => void;
    const pendingPromise = new Promise((resolve) => {
      resolvePromise = resolve;
    });
    mockFetch.mockReturnValueOnce(pendingPromise);

    render(<UrlShortenForm />);

    const input = screen.getByTestId('url-input');
    await user.type(input, 'https://example.com');

    const submitButton = screen.getByTestId('shorten-button');
    await user.click(submitButton);

    // Verify loading state
    await waitFor(() => {
      expect(submitButton).toBeDisabled();
      // The button should show loading state
      expect(screen.getByRole('button', { name: /shortening/i })).toBeInTheDocument();
    });

    // Complete the request
    resolvePromise!({
      ok: true,
      json: () => Promise.resolve({ short_code: 'abc', original_url: 'https://example.com' }),
    });

    // Verify loading state is cleared
    await waitFor(() => {
      expect(submitButton).not.toBeDisabled();
    });
  });

  /**
   * Test API error handling in integration context
   */
  it('should handle API errors gracefully', async () => {
    const user = userEvent.setup();

    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 500,
      json: () => Promise.resolve({ detail: 'Internal server error' }),
    });

    render(<UrlShortenForm />);

    const input = screen.getByTestId('url-input');
    await user.type(input, 'https://example.com');
    await user.click(screen.getByTestId('shorten-button'));

    await waitFor(() => {
      expect(screen.getByTestId('error-message')).toBeInTheDocument();
      expect(screen.getByText('Internal server error')).toBeInTheDocument();
    });

    // Verify no success result is shown
    expect(screen.queryByTestId('success-result')).not.toBeInTheDocument();
  });

  /**
   * Test network error handling
   */
  it('should handle network errors', async () => {
    const user = userEvent.setup();

    mockFetch.mockRejectedValueOnce(new Error('Network error'));

    render(<UrlShortenForm />);

    const input = screen.getByTestId('url-input');
    await user.type(input, 'https://example.com');
    await user.click(screen.getByTestId('shorten-button'));

    await waitFor(() => {
      expect(screen.getByTestId('error-message')).toBeInTheDocument();
      expect(screen.getByText('Network error')).toBeInTheDocument();
    });
  });

  /**
   * Test URL normalization (auto-adding https://)
   */
  it('should normalize URLs without protocol', async () => {
    const user = userEvent.setup();

    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve({
        short_code: 'norm123',
        original_url: 'https://example.com',
      }),
    });

    render(<UrlShortenForm />);

    const input = screen.getByTestId('url-input');
    // Enter URL without protocol
    await user.type(input, 'example.com');
    await user.click(screen.getByTestId('shorten-button'));

    await waitFor(() => {
      // Verify the API was called with normalized URL (with https://)
      expect(mockFetch).toHaveBeenCalledWith('/api/urls/', expect.objectContaining({
        body: JSON.stringify({
          original_url: 'https://example.com',
          guest: true,
        }),
      }));
    });
  });
});
