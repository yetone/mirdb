/**
 * Integration tests for error handling during URL shortening on the homepage.
 *
 * These tests simulate the full user flow:
 * 1. User enters a URL
 * 2. Form validates the URL
 * 3. On submit, an API call is made
 * 4. Errors are displayed in a user-friendly way
 * 5. User can retry
 */

import React, { useState } from 'react';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';

import { isValidHttpUrl, getUrlValidationError } from '../../src/utils/validation';
import { mapApiError } from '../../src/utils/errorHandler';

// ------------------------------------------------------------------
// Test helpers / stubs
// ------------------------------------------------------------------

interface UrlShortenerFormProps {
  onSubmit: (url: string) => Promise<void>;
}

/** Minimal form that mimics the real URLInputForm error-handling behaviour. */
function TestUrlShortenerForm({ onSubmit }: UrlShortenerFormProps) {
  const [url, setUrl] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();

    // Client-side validation before API call
    const validationError = getUrlValidationError(url);
    if (validationError) {
      setError(validationError);
      return;
    }

    setError(null);
    setLoading(true);

    try {
      await onSubmit(url);
    } catch (err) {
      const mapped = mapApiError(err);
      setError(mapped.message);
    } finally {
      setLoading(false);
    }
  };

  const handleRetry = () => {
    setError(null);
    setUrl('');
  };

  return (
    <form onSubmit={handleSubmit} data-testid="url-form">
      <label htmlFor="url-input">URL</label>
      <input
        id="url-input"
        type="text"
        value={url}
        onChange={(e) => setUrl(e.target.value)}
        disabled={loading}
        data-testid="url-input"
        aria-invalid={error ? 'true' : 'false'}
        aria-describedby={error ? 'error-message' : undefined}
      />
      <button type="submit" disabled={loading} data-testid="submit-btn">
        {loading ? 'Shortening…' : 'Shorten URL'}
      </button>
      {error && (
        <div id="error-message" role="alert" data-testid="error-message">
          {error}
          <button type="button" onClick={handleRetry} data-testid="retry-btn">
            Retry
          </button>
        </div>
      )}
    </form>
  );
}

// ------------------------------------------------------------------
// Tests
// ------------------------------------------------------------------

describe('Error Handling Integration', () => {
  let user: ReturnType<typeof userEvent.setup>;

  beforeEach(() => {
    user = userEvent.setup();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('TC-1: displays user-friendly error and resets UI when fetch is rejected with network error', async () => {
    // Use a delayed rejection so the loading state is observable
    const mockSubmit = vi.fn().mockImplementation(
      () => new Promise((_resolve, reject) => {
        setTimeout(() => reject(new TypeError('Failed to fetch')), 50);
      })
    );

    render(<TestUrlShortenerForm onSubmit={mockSubmit} />);

    const input = screen.getByTestId('url-input');
    const submitBtn = screen.getByTestId('submit-btn');

    await user.type(input, 'https://example.com');
    await user.click(submitBtn);

    // Loading state should be visible immediately after submit
    expect(screen.getByText('Shortening…')).toBeInTheDocument();

    // Wait for error
    await waitFor(() => {
      expect(screen.getByTestId('error-message')).toBeInTheDocument();
    });

    expect(screen.getByTestId('error-message')).toHaveTextContent(
      'Unable to connect to the server'
    );

    // Loading indicator hidden, submit button re-enabled
    expect(screen.queryByText('Shortening…')).not.toBeInTheDocument();
    expect(screen.getByTestId('submit-btn')).not.toBeDisabled();
    expect(input).not.toBeDisabled();
  });

  it('TC-2: displays validation-specific error when API returns 400', async () => {
    const mockSubmit = vi.fn().mockRejectedValue(new Error('HTTP 400: Bad Request'));

    render(<TestUrlShortenerForm onSubmit={mockSubmit} />);

    const input = screen.getByTestId('url-input');
    const submitBtn = screen.getByTestId('submit-btn');

    await user.type(input, 'https://example.com');
    await user.click(submitBtn);

    await waitFor(() => {
      expect(screen.getByTestId('error-message')).toBeInTheDocument();
    });

    // Should show validation-specific message, not generic
    expect(screen.getByTestId('error-message')).toHaveTextContent('Please enter a valid URL');
    expect(screen.getByTestId('error-message')).not.toHaveTextContent('unexpected');
  });

  it('TC-3: displays rate-limit message when API returns 429', async () => {
    const mockSubmit = vi.fn().mockRejectedValue(new Error('HTTP 429: Too Many Requests'));

    render(<TestUrlShortenerForm onSubmit={mockSubmit} />);

    const input = screen.getByTestId('url-input');
    const submitBtn = screen.getByTestId('submit-btn');

    await user.type(input, 'https://example.com');
    await user.click(submitBtn);

    await waitFor(() => {
      expect(screen.getByTestId('error-message')).toBeInTheDocument();
    });

    expect(screen.getByTestId('error-message')).toHaveTextContent('Too many requests');
    expect(screen.getByTestId('error-message')).toHaveTextContent('try again');
  });

  it('TC-4: rejects overly long URL on client side before any API call', async () => {
    const mockSubmit = vi.fn().mockResolvedValue(undefined);

    render(<TestUrlShortenerForm onSubmit={mockSubmit} />);

    const input = screen.getByTestId('url-input');
    const submitBtn = screen.getByTestId('submit-btn');

    const longUrl = 'https://example.com/' + 'x'.repeat(3000);
    await user.type(input, longUrl);
    await user.click(submitBtn);

    // Error should appear immediately (no async wait needed because it's client-side)
    expect(screen.getByTestId('error-message')).toBeInTheDocument();
    expect(screen.getByTestId('error-message')).toHaveTextContent('URL must be under');

    // API should NOT have been called
    expect(mockSubmit).not.toHaveBeenCalled();
  });

  it('TC-4: rejects empty URL on client side before any API call', async () => {
    const mockSubmit = vi.fn().mockResolvedValue(undefined);

    render(<TestUrlShortenerForm onSubmit={mockSubmit} />);

    const submitBtn = screen.getByTestId('submit-btn');
    await user.click(submitBtn);

    expect(screen.getByTestId('error-message')).toHaveTextContent('Please enter a URL');
    expect(mockSubmit).not.toHaveBeenCalled();
  });

  it('TC-5: rejects javascript: scheme before any API call', async () => {
    const mockSubmit = vi.fn().mockResolvedValue(undefined);

    render(<TestUrlShortenerForm onSubmit={mockSubmit} />);

    const input = screen.getByTestId('url-input');
    const submitBtn = screen.getByTestId('submit-btn');

    await user.type(input, 'javascript:alert("xss")');
    await user.click(submitBtn);

    expect(screen.getByTestId('error-message')).toHaveTextContent('Unsafe URL scheme is not allowed');
    expect(mockSubmit).not.toHaveBeenCalled();
  });

  it('TC-5: rejects data: scheme before any API call', async () => {
    const mockSubmit = vi.fn().mockResolvedValue(undefined);

    render(<TestUrlShortenerForm onSubmit={mockSubmit} />);

    const input = screen.getByTestId('url-input');
    const submitBtn = screen.getByTestId('submit-btn');

    await user.type(input, 'data:text/html,<script>alert(1)</script>');
    await user.click(submitBtn);

    expect(screen.getByTestId('error-message')).toHaveTextContent('Unsafe URL scheme is not allowed');
    expect(mockSubmit).not.toHaveBeenCalled();
  });

  it('TC-3 retry: clears error and allows a second submission', async () => {
    const mockSubmit = vi
      .fn()
      .mockRejectedValueOnce(new Error('HTTP 429: Too Many Requests'))
      .mockResolvedValueOnce(undefined);

    render(<TestUrlShortenerForm onSubmit={mockSubmit} />);

    const input = screen.getByTestId('url-input');
    const submitBtn = screen.getByTestId('submit-btn');

    await user.type(input, 'https://example.com');
    await user.click(submitBtn);

    await waitFor(() => {
      expect(screen.getByTestId('error-message')).toBeInTheDocument();
    });

    // Click retry
    const retryBtn = screen.getByTestId('retry-btn');
    await user.click(retryBtn);

    // Error should be gone
    expect(screen.queryByTestId('error-message')).not.toBeInTheDocument();

    // Submit again
    await user.type(input, 'https://example.com/retry');
    await user.click(submitBtn);

    await waitFor(() => {
      expect(mockSubmit).toHaveBeenCalledTimes(2);
    });

    // No error this time
    expect(screen.queryByTestId('error-message')).not.toBeInTheDocument();
  });

  it('does not expose stack traces or internal details in error messages', async () => {
    const mockSubmit = vi.fn().mockRejectedValue(
      new Error('HTTP 500: Internal Server Error - database connection timeout at /usr/src/db/pool.js:42')
    );

    render(<TestUrlShortenerForm onSubmit={mockSubmit} />);

    const input = screen.getByTestId('url-input');
    const submitBtn = screen.getByTestId('submit-btn');

    await user.type(input, 'https://example.com');
    await user.click(submitBtn);

    await waitFor(() => {
      expect(screen.getByTestId('error-message')).toBeInTheDocument();
    });

    const errorMessage = screen.getByTestId('error-message').textContent;
    expect(errorMessage).not.toContain('database');
    expect(errorMessage).not.toContain('connection timeout');
    expect(errorMessage).not.toContain('/usr/src/db');
    expect(errorMessage).not.toContain('pool.js');
    expect(errorMessage).not.toContain('500');
  });

  it('handles a 5xx error with a generic server error message', async () => {
    const mockSubmit = vi.fn().mockRejectedValue(new Error('HTTP 503: Service Unavailable'));

    render(<TestUrlShortenerForm onSubmit={mockSubmit} />);

    const input = screen.getByTestId('url-input');
    const submitBtn = screen.getByTestId('submit-btn');

    await user.type(input, 'https://example.com');
    await user.click(submitBtn);

    await waitFor(() => {
      expect(screen.getByTestId('error-message')).toBeInTheDocument();
    });

    expect(screen.getByTestId('error-message')).toHaveTextContent('Something went wrong on our end');
  });
});
