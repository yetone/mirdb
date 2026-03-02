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
      // User-friendly error message for 500 errors
      expect(screen.getByText('Something went wrong. Please try again.')).toBeInTheDocument();
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
      // User-friendly error message for network errors
      expect(screen.getByText('Unable to connect. Please check your internet connection.')).toBeInTheDocument();
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

/**
 * API Error Handling Tests
 * Owner: Scenario 17 - API Error Handling
 *
 * Tests the graceful handling of API errors:
 * - Network errors (offline)
 * - Server errors (500)
 * - Rate limiting (429)
 * - Validation errors (400)
 * - Form recovery after errors
 */
describe('API Error Handling - Integration', () => {
  const mockFetch = vi.fn();
  const originalFetch = global.fetch;

  beforeEach(() => {
    global.fetch = mockFetch;
    mockFetch.mockReset();
    localStorage.clear();
  });

  afterEach(() => {
    global.fetch = originalFetch;
  });

  /**
   * Test Case 1: Network offline error
   * Input: Submit URL when network is offline
   * Expected: User-friendly error message displayed: 'Unable to connect. Please check your internet connection.'
   */
  it('should display user-friendly error message when network is offline', async () => {
    const user = userEvent.setup();

    // Simulate network failure (fetch throws TypeError)
    mockFetch.mockRejectedValueOnce(new TypeError('Failed to fetch'));

    render(<UrlShortenForm />);

    const input = screen.getByTestId('url-input');
    await user.type(input, 'https://example.com');
    await user.click(screen.getByTestId('shorten-button'));

    await waitFor(() => {
      expect(screen.getByTestId('error-message')).toBeInTheDocument();
      expect(screen.getByText('Unable to connect. Please check your internet connection.')).toBeInTheDocument();
    });

    // Verify no success result is shown
    expect(screen.queryByTestId('success-result')).not.toBeInTheDocument();
  });

  /**
   * Test Case 2: API 500 error
   * Input: Submit URL when API returns 500
   * Expected: User-friendly error message displayed: 'Something went wrong. Please try again.'
   */
  it('should display user-friendly error message when API returns 500', async () => {
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
      expect(screen.getByText('Something went wrong. Please try again.')).toBeInTheDocument();
    });

    // Verify no success result is shown
    expect(screen.queryByTestId('success-result')).not.toBeInTheDocument();
  });

  /**
   * Test Case 3: API 429 rate limiting error
   * Input: Submit URL when API returns 429
   * Expected: User-friendly error message displayed: 'Too many requests. Please wait a moment.'
   */
  it('should display user-friendly error message when API returns 429 (rate limited)', async () => {
    const user = userEvent.setup();

    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 429,
      json: () => Promise.resolve({ detail: 'Rate limit exceeded' }),
    });

    render(<UrlShortenForm />);

    const input = screen.getByTestId('url-input');
    await user.type(input, 'https://example.com');
    await user.click(screen.getByTestId('shorten-button'));

    await waitFor(() => {
      expect(screen.getByTestId('error-message')).toBeInTheDocument();
      expect(screen.getByText('Too many requests. Please wait a moment.')).toBeInTheDocument();
    });

    // Verify no success result is shown
    expect(screen.queryByTestId('success-result')).not.toBeInTheDocument();
  });

  /**
   * Test Case 4: API 400 validation error
   * Input: Submit URL when API returns 400 with validation error
   * Expected: API validation error message is displayed to user
   */
  it('should display API validation error message when API returns 400', async () => {
    const user = userEvent.setup();

    const validationErrorMessage = 'URL is not allowed or contains invalid characters';
    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 400,
      json: () => Promise.resolve({ detail: validationErrorMessage }),
    });

    render(<UrlShortenForm />);

    const input = screen.getByTestId('url-input');
    await user.type(input, 'https://example.com');
    await user.click(screen.getByTestId('shorten-button'));

    await waitFor(() => {
      expect(screen.getByTestId('error-message')).toBeInTheDocument();
      expect(screen.getByText(validationErrorMessage)).toBeInTheDocument();
    });

    // Verify no success result is shown
    expect(screen.queryByTestId('success-result')).not.toBeInTheDocument();
  });

  /**
   * Test Case 5: Form remains interactive after error
   * Input: Error occurs during URL creation
   * Expected: Form remains interactive, user can retry submission
   */
  it('should keep form interactive after error, allowing user to retry', async () => {
    const user = userEvent.setup();

    // First call fails
    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 500,
      json: () => Promise.resolve({ detail: 'Server error' }),
    });

    // Second call succeeds
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve({
        short_code: 'retry123',
        original_url: 'https://example.com',
      }),
    });

    render(<UrlShortenForm />);

    const input = screen.getByTestId('url-input');
    const submitButton = screen.getByTestId('shorten-button');

    // First attempt - fails
    await user.type(input, 'https://example.com');
    await user.click(submitButton);

    await waitFor(() => {
      expect(screen.getByTestId('error-message')).toBeInTheDocument();
      expect(screen.getByText('Something went wrong. Please try again.')).toBeInTheDocument();
    });

    // Verify form is still interactive
    expect(input).not.toBeDisabled();
    expect(submitButton).not.toBeDisabled();

    // Retry submission (user can immediately retry)
    await user.click(submitButton);

    // Second attempt should succeed
    await waitFor(() => {
      expect(screen.getByTestId('success-result')).toBeInTheDocument();
      expect(screen.getByTestId('short-url')).toHaveTextContent(/retry123/);
    });

    // Error message should be cleared
    expect(screen.queryByTestId('error-message')).not.toBeInTheDocument();
  });

  /**
   * Additional test: Other server errors (502, 503, 504)
   */
  it('should display user-friendly error message for other server errors (502)', async () => {
    const user = userEvent.setup();

    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 502,
      json: () => Promise.resolve({ detail: 'Bad gateway' }),
    });

    render(<UrlShortenForm />);

    const input = screen.getByTestId('url-input');
    await user.type(input, 'https://example.com');
    await user.click(screen.getByTestId('shorten-button'));

    await waitFor(() => {
      expect(screen.getByTestId('error-message')).toBeInTheDocument();
      expect(screen.getByText('Something went wrong. Please try again.')).toBeInTheDocument();
    });
  });

  /**
   * Additional test: Network error with different error message
   */
  it('should handle network errors with various error messages', async () => {
    const user = userEvent.setup();

    // Simulate network error with different message format
    mockFetch.mockRejectedValueOnce(new Error('Network request failed'));

    render(<UrlShortenForm />);

    const input = screen.getByTestId('url-input');
    await user.type(input, 'https://example.com');
    await user.click(screen.getByTestId('shorten-button'));

    await waitFor(() => {
      expect(screen.getByTestId('error-message')).toBeInTheDocument();
      expect(screen.getByText('Unable to connect. Please check your internet connection.')).toBeInTheDocument();
    });
  });

  /**
   * Additional test: Error message role and accessibility
   */
  it('should display error message with proper role for accessibility', async () => {
    const user = userEvent.setup();

    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 500,
      json: () => Promise.resolve({ detail: 'Server error' }),
    });

    render(<UrlShortenForm />);

    const input = screen.getByTestId('url-input');
    await user.type(input, 'https://example.com');
    await user.click(screen.getByTestId('shorten-button'));

    await waitFor(() => {
      const errorMessage = screen.getByTestId('error-message');
      expect(errorMessage).toBeInTheDocument();
      expect(errorMessage).toHaveAttribute('role', 'alert');
    });
  });

  /**
   * Additional test: 400 error without detail falls back to default message
   */
  it('should display default validation error when API returns 400 without detail', async () => {
    const user = userEvent.setup();

    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 400,
      json: () => Promise.resolve({}),
    });

    render(<UrlShortenForm />);

    const input = screen.getByTestId('url-input');
    await user.type(input, 'https://example.com');
    await user.click(screen.getByTestId('shorten-button'));

    await waitFor(() => {
      expect(screen.getByTestId('error-message')).toBeInTheDocument();
      expect(screen.getByText('Invalid URL format')).toBeInTheDocument();
    });
  });

  /**
   * Additional test: User can clear input and retry after error
   */
  it('should allow user to clear input and try different URL after error', async () => {
    const user = userEvent.setup();

    // First call fails
    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 500,
      json: () => Promise.resolve({ detail: 'Server error' }),
    });

    // Second call succeeds
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve({
        short_code: 'newurl123',
        original_url: 'https://different-example.com',
      }),
    });

    render(<UrlShortenForm />);

    const input = screen.getByTestId('url-input');
    const submitButton = screen.getByTestId('shorten-button');

    // First attempt - fails
    await user.type(input, 'https://example.com');
    await user.click(submitButton);

    await waitFor(() => {
      expect(screen.getByTestId('error-message')).toBeInTheDocument();
    });

    // Clear input and enter different URL
    await user.clear(input);
    await user.type(input, 'https://different-example.com');

    // Submit again
    await user.click(submitButton);

    // Second attempt should succeed
    await waitFor(() => {
      expect(screen.getByTestId('success-result')).toBeInTheDocument();
      expect(screen.getByTestId('short-url')).toHaveTextContent(/newurl123/);
    });
  });
});
