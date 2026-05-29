/**
 * Error Handling Integration Tests
 * Owner: Scenario 9 - Error Handling and API Failure Resilience
 *
 * Tests for graceful API error handling in URL shortening.
 * Covers network errors, server errors, rate limiting, validation errors,
 * timeouts, and CORS errors with user-friendly messages.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import UrlShortenerForm from '../../src/components/Home/UrlShortenerForm';

const VALID_URL = 'https://example.com';

describe('Error Handling Integration', () => {
  let fetchSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    fetchSpy = vi.spyOn(globalThis, 'fetch');
  });

  afterEach(() => {
    fetchSpy.mockRestore();
    vi.useRealTimers();
  });

  // TC-1: Network error — API unreachable
  it('shows user-friendly message on network error (TC-1)', async () => {
    const user = userEvent.setup();

    fetchSpy.mockRejectedValueOnce(new TypeError('Failed to fetch'));

    render(<UrlShortenerForm />);

    await user.type(screen.getByTestId('url-input'), VALID_URL);
    await user.click(screen.getByTestId('shorten-button'));

    const error = await screen.findByTestId('url-error');
    expect(error).toHaveTextContent('Unable to connect. Please try again later.');
    expect(screen.queryByTestId('result')).not.toBeInTheDocument();
  });

  // TC-2: Server error — HTTP 500 response
  it('shows user-friendly message on server error 500 (TC-2)', async () => {
    const user = userEvent.setup();

    fetchSpy.mockResolvedValueOnce(
      new Response('Internal Server Error', { status: 500, statusText: 'Internal Server Error' })
    );

    render(<UrlShortenerForm />);

    await user.type(screen.getByTestId('url-input'), VALID_URL);
    await user.click(screen.getByTestId('shorten-button'));

    const error = await screen.findByTestId('url-error');
    expect(error).toHaveTextContent('Something went wrong. Please try again.');
    expect(screen.queryByTestId('result')).not.toBeInTheDocument();
  });

  // TC-3: Rate limit — HTTP 429 response
  it('shows rate-limit message with retry guidance on 429 (TC-3)', async () => {
    const user = userEvent.setup();

    fetchSpy.mockResolvedValueOnce(
      new Response('Too Many Requests', {
        status: 429,
        statusText: 'Too Many Requests',
        headers: { 'Retry-After': '60' },
      })
    );

    render(<UrlShortenerForm />);

    await user.type(screen.getByTestId('url-input'), VALID_URL);
    await user.click(screen.getByTestId('shorten-button'));

    const error = await screen.findByTestId('url-error');
    expect(error).toHaveTextContent('Too many requests. Please wait a moment before trying again.');
    expect(screen.queryByTestId('result')).not.toBeInTheDocument();
  });

  // TC-4: Backend validation — HTTP 422 response
  it('shows specific validation error and preserves form state on 422 (TC-4)', async () => {
    const user = userEvent.setup();

    fetchSpy.mockResolvedValueOnce(
      new Response(JSON.stringify({ detail: 'URL domain is blacklisted' }), {
        status: 422,
        statusText: 'Unprocessable Entity',
        headers: { 'Content-Type': 'application/json' },
      })
    );

    render(<UrlShortenerForm />);

    const input = screen.getByTestId('url-input');
    await user.type(input, VALID_URL);
    await user.click(screen.getByTestId('shorten-button'));

    const error = await screen.findByTestId('url-error');
    expect(error).toHaveTextContent('URL domain is blacklisted');
    expect(screen.queryByTestId('result')).not.toBeInTheDocument();

    // Form state preserved: input still has the URL
    expect(input).toHaveValue(VALID_URL);
  });

  // TC-5: Timeout — API response takes >30s
  it('shows timeout message and allows retry after timeout (TC-5)', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    const user = userEvent.setup();

    // Simulate a fetch that never resolves within the timeout window
    fetchSpy.mockImplementationOnce(
      () =>
        new Promise((_resolve, reject) => {
          setTimeout(() => {
            reject(new DOMException('The operation was aborted', 'AbortError'));
          }, 31000);
        })
    );

    render(<UrlShortenerForm />);

    await user.type(screen.getByTestId('url-input'), VALID_URL);
    await user.click(screen.getByTestId('shorten-button'));

    // Advance past the 30-second timeout
    vi.advanceTimersByTime(31000);

    const error = await screen.findByTestId('url-error');
    expect(error).toHaveTextContent('Request timed out. Please try again.');
    expect(screen.queryByTestId('result')).not.toBeInTheDocument();

    // Verify user can retry: form is still usable
    const button = screen.getByTestId('shorten-button');
    expect(button).not.toBeDisabled();
    const input = screen.getByTestId('url-input');
    expect(input).not.toBeDisabled();
  });

  // TC-6: CORS error — generic message, no technical details
  it('shows generic error message on CORS error without exposing technical details (TC-6)', async () => {
    const user = userEvent.setup();

    // CORS errors typically throw TypeError with no specific message in many browsers
    fetchSpy.mockRejectedValueOnce(new TypeError('Failed to fetch'));

    render(<UrlShortenerForm />);

    await user.type(screen.getByTestId('url-input'), VALID_URL);
    await user.click(screen.getByTestId('shorten-button'));

    const error = await screen.findByTestId('url-error');
    const errorText = error.textContent || '';

    // Should NOT contain technical CORS terminology
    expect(errorText.toLowerCase()).not.toContain('cors');
    expect(errorText.toLowerCase()).not.toContain('cross-origin');
    expect(errorText.toLowerCase()).not.toContain('header');
    expect(errorText.toLowerCase()).not.toContain('preflight');

    // Should show a generic user-friendly message
    expect(errorText).toBe('Unable to connect. Please try again later.');
  });

  // Retry capability: user can retry after any error without refreshing
  it('allows retry after network error without page refresh', async () => {
    const user = userEvent.setup();

    // First call fails with network error
    fetchSpy.mockRejectedValueOnce(new TypeError('Failed to fetch'));
    // Second call succeeds
    fetchSpy.mockResolvedValueOnce(
      new Response(JSON.stringify({ short_url: 'https://short.link/abc123' }), {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
      })
    );

    render(<UrlShortenerForm />);

    await user.type(screen.getByTestId('url-input'), VALID_URL);
    await user.click(screen.getByTestId('shorten-button'));

    // First error shown
    const firstError = await screen.findByTestId('url-error');
    expect(firstError).toHaveTextContent('Unable to connect. Please try again later.');

    // Retry: click the button again
    await user.click(screen.getByTestId('shorten-button'));

    // Success on retry
    await waitFor(() => {
      expect(screen.getByTestId('result')).toBeInTheDocument();
    });
    expect(screen.queryByTestId('url-error')).not.toBeInTheDocument();
  });

  // Retry capability: user can retry after server error
  it('allows retry after server error without page refresh', async () => {
    const user = userEvent.setup();

    // First call fails with 500
    fetchSpy.mockResolvedValueOnce(
      new Response('Internal Server Error', { status: 500 })
    );
    // Second call succeeds
    fetchSpy.mockResolvedValueOnce(
      new Response(JSON.stringify({ short_url: 'https://short.link/xyz789' }), {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
      })
    );

    render(<UrlShortenerForm />);

    await user.type(screen.getByTestId('url-input'), VALID_URL);
    await user.click(screen.getByTestId('shorten-button'));

    await screen.findByTestId('url-error');

    // Retry
    await user.click(screen.getByTestId('shorten-button'));

    await waitFor(() => {
      expect(screen.getByTestId('result')).toBeInTheDocument();
    });
  });

  // Generic 4xx errors are handled gracefully
  it('handles generic 400 error gracefully', async () => {
    const user = userEvent.setup();

    fetchSpy.mockResolvedValueOnce(
      new Response('Bad Request', { status: 400 })
    );

    render(<UrlShortenerForm />);

    await user.type(screen.getByTestId('url-input'), VALID_URL);
    await user.click(screen.getByTestId('shorten-button'));

    const error = await screen.findByTestId('url-error');
    expect(error).toHaveTextContent('Something went wrong. Please try again.');
  });

  // 503 Service Unavailable is handled as server error
  it('handles 503 service unavailable as server error', async () => {
    const user = userEvent.setup();

    fetchSpy.mockResolvedValueOnce(
      new Response('Service Unavailable', { status: 503 })
    );

    render(<UrlShortenerForm />);

    await user.type(screen.getByTestId('url-input'), VALID_URL);
    await user.click(screen.getByTestId('shorten-button'));

    const error = await screen.findByTestId('url-error');
    expect(error).toHaveTextContent('Something went wrong. Please try again.');
  });
});
