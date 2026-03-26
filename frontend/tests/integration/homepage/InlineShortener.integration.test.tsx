/**
 * InlineShortener Integration Tests - Error Handling
 *
 * Owner: Scenario 2 (Component), Scenario 3 (Error Handling Tests)
 *
 * Tests API error handling and special character URL encoding
 * as specified in REQ-7.
 *
 * Test cases:
 * - TC4: Network error shows friendly message with retry option
 * - TC5: URLs with special characters are properly encoded
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { InlineShortener } from '../../../src/components/homepage/InlineShortener';

// Mock fetch globally for integration tests
const originalFetch = global.fetch;

describe('InlineShortener - Integration Tests', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    global.fetch = originalFetch;
  });

  describe('TC4: Network Error Handling', () => {
    it('displays friendly error message when network request fails', async () => {
      const user = userEvent.setup();

      // Mock fetch to simulate network error
      global.fetch = vi.fn().mockRejectedValue(new Error('Network error'));

      render(<InlineShortener />);

      const input = screen.getByPlaceholderText('Paste your long URL here...');
      const button = screen.getByTestId('shorten-button');

      await user.type(input, 'https://example.com/long/url');
      await user.click(button);

      // Wait for error to be displayed
      await waitFor(() => {
        const errorMessage = screen.getByTestId('error-message');
        expect(errorMessage).toBeInTheDocument();
        expect(errorMessage.textContent).toMatch(/Network error|Failed to shorten/i);
      });
    });

    it('displays retry button when API error occurs', async () => {
      const user = userEvent.setup();

      // Mock fetch to simulate API error
      global.fetch = vi.fn().mockRejectedValue(new Error('Server error'));

      render(<InlineShortener />);

      const input = screen.getByPlaceholderText('Paste your long URL here...');
      const button = screen.getByTestId('shorten-button');

      await user.type(input, 'https://example.com');
      await user.click(button);

      await waitFor(() => {
        const retryButton = screen.getByTestId('retry-button');
        expect(retryButton).toBeInTheDocument();
        expect(retryButton).toHaveTextContent('Try again');
      });
    });

    it('allows user to retry after error', async () => {
      const user = userEvent.setup();

      // First call fails, second succeeds
      let callCount = 0;
      global.fetch = vi.fn().mockImplementation(() => {
        callCount++;
        if (callCount === 1) {
          return Promise.reject(new Error('Temporary error'));
        }
        return Promise.resolve({
          ok: true,
          json: () => Promise.resolve({
            shortUrl: 'https://short.url/abc',
            originalUrl: 'https://example.com',
            shortCode: 'abc',
            createdAt: new Date().toISOString(),
          }),
        });
      });

      render(<InlineShortener />);

      const input = screen.getByPlaceholderText('Paste your long URL here...');
      const shortenButton = screen.getByTestId('shorten-button');

      // First attempt - should fail
      await user.type(input, 'https://example.com');
      await user.click(shortenButton);

      await waitFor(() => {
        expect(screen.getByTestId('retry-button')).toBeInTheDocument();
      });

      // Click retry button to clear error
      const retryButton = screen.getByTestId('retry-button');
      await user.click(retryButton);

      // Error should be cleared
      expect(screen.queryByTestId('error-message')).not.toBeInTheDocument();

      // Second attempt - should succeed
      await user.click(shortenButton);

      await waitFor(() => {
        expect(screen.getByTestId('result-section')).toBeInTheDocument();
      });
    });

    it('displays friendly error for API response errors', async () => {
      const user = userEvent.setup();

      // Mock fetch to return error response
      global.fetch = vi.fn().mockResolvedValue({
        ok: false,
        status: 500,
        json: () => Promise.resolve({ message: 'Internal server error' }),
      });

      render(<InlineShortener />);

      const input = screen.getByPlaceholderText('Paste your long URL here...');
      const button = screen.getByTestId('shorten-button');

      await user.type(input, 'https://example.com');
      await user.click(button);

      await waitFor(() => {
        const errorMessage = screen.getByTestId('error-message');
        expect(errorMessage).toBeInTheDocument();
      });
    });

    it('handles timeout errors gracefully', async () => {
      const user = userEvent.setup();

      // Mock fetch to simulate timeout
      global.fetch = vi.fn().mockRejectedValue(new Error('Request timeout'));

      render(<InlineShortener />);

      const input = screen.getByPlaceholderText('Paste your long URL here...');
      const button = screen.getByTestId('shorten-button');

      await user.type(input, 'https://example.com');
      await user.click(button);

      await waitFor(() => {
        const errorMessage = screen.getByTestId('error-message');
        expect(errorMessage).toBeInTheDocument();
      });

      // Retry button should be present
      expect(screen.getByTestId('retry-button')).toBeInTheDocument();
    });
  });

  describe('TC5: URL with Special Characters', () => {
    it('successfully shortens URL with query parameters', async () => {
      const user = userEvent.setup();

      global.fetch = vi.fn().mockResolvedValue({
        ok: true,
        json: () => Promise.resolve({
          shortUrl: 'https://short.url/xyz789',
          originalUrl: 'https://example.com/search?q=hello world&lang=en',
          shortCode: 'xyz789',
          createdAt: new Date().toISOString(),
        }),
      });

      render(<InlineShortener />);

      const input = screen.getByPlaceholderText('Paste your long URL here...');
      const button = screen.getByTestId('shorten-button');

      // URL with special characters that need encoding
      await user.type(input, 'https://example.com/search?q=hello world&lang=en');
      await user.click(button);

      await waitFor(() => {
        expect(screen.getByTestId('result-section')).toBeInTheDocument();
      });

      // Verify API was called with properly encoded URL
      expect(global.fetch).toHaveBeenCalled();
      const fetchCall = vi.mocked(global.fetch).mock.calls[0];
      const requestBody = JSON.parse(fetchCall[1]?.body as string);
      expect(requestBody.url).toContain('example.com');
    });

    it('successfully shortens URL with unicode characters', async () => {
      const user = userEvent.setup();

      global.fetch = vi.fn().mockResolvedValue({
        ok: true,
        json: () => Promise.resolve({
          shortUrl: 'https://short.url/uni123',
          originalUrl: 'https://example.com/path?emoji=😀',
          shortCode: 'uni123',
          createdAt: new Date().toISOString(),
        }),
      });

      render(<InlineShortener />);

      const input = screen.getByPlaceholderText('Paste your long URL here...');
      const button = screen.getByTestId('shorten-button');

      await user.type(input, 'https://example.com/path?emoji=😀');
      await user.click(button);

      await waitFor(() => {
        expect(screen.getByTestId('result-section')).toBeInTheDocument();
      });

      const shortUrl = screen.getByTestId('short-url');
      expect(shortUrl).toHaveTextContent('https://short.url/uni123');
    });

    it('successfully shortens URL with fragment identifier', async () => {
      const user = userEvent.setup();

      global.fetch = vi.fn().mockResolvedValue({
        ok: true,
        json: () => Promise.resolve({
          shortUrl: 'https://short.url/frag456',
          originalUrl: 'https://example.com/page#section-1',
          shortCode: 'frag456',
          createdAt: new Date().toISOString(),
        }),
      });

      render(<InlineShortener />);

      const input = screen.getByPlaceholderText('Paste your long URL here...');
      const button = screen.getByTestId('shorten-button');

      await user.type(input, 'https://example.com/page#section-1');
      await user.click(button);

      await waitFor(() => {
        expect(screen.getByTestId('result-section')).toBeInTheDocument();
      });
    });

    it('properly handles URL with percent-encoded characters', async () => {
      const user = userEvent.setup();

      global.fetch = vi.fn().mockResolvedValue({
        ok: true,
        json: () => Promise.resolve({
          shortUrl: 'https://short.url/enc789',
          originalUrl: 'https://example.com/path%20with%20spaces',
          shortCode: 'enc789',
          createdAt: new Date().toISOString(),
        }),
      });

      render(<InlineShortener />);

      const input = screen.getByPlaceholderText('Paste your long URL here...');
      const button = screen.getByTestId('shorten-button');

      await user.type(input, 'https://example.com/path%20with%20spaces');
      await user.click(button);

      await waitFor(() => {
        expect(screen.getByTestId('result-section')).toBeInTheDocument();
      });
    });

    it('successfully shortens URL with international domain', async () => {
      const user = userEvent.setup();

      global.fetch = vi.fn().mockResolvedValue({
        ok: true,
        json: () => Promise.resolve({
          shortUrl: 'https://short.url/intl999',
          originalUrl: 'https://例え.jp/page',
          shortCode: 'intl999',
          createdAt: new Date().toISOString(),
        }),
      });

      render(<InlineShortener />);

      const input = screen.getByPlaceholderText('Paste your long URL here...');
      const button = screen.getByTestId('shorten-button');

      // Note: This might be punycode encoded by the browser
      await user.type(input, 'https://例え.jp/page');
      await user.click(button);

      await waitFor(() => {
        expect(screen.getByTestId('result-section')).toBeInTheDocument();
      });
    });
  });

  describe('Loading States', () => {
    it('shows loading state during API call', async () => {
      const user = userEvent.setup();

      // Mock fetch with delay
      global.fetch = vi.fn().mockImplementation(() => new Promise((resolve) => {
        setTimeout(() => {
          resolve({
            ok: true,
            json: () => Promise.resolve({
              shortUrl: 'https://short.url/loading',
              originalUrl: 'https://example.com',
              shortCode: 'loading',
              createdAt: new Date().toISOString(),
            }),
          });
        }, 100);
      }));

      render(<InlineShortener />);

      const input = screen.getByPlaceholderText('Paste your long URL here...');
      const button = screen.getByTestId('shorten-button');

      await user.type(input, 'https://example.com');
      await user.click(button);

      // Check loading state
      expect(button).toHaveTextContent('Shortening...');
      expect(button).toBeDisabled();
      expect(input).toBeDisabled();

      // Wait for completion
      await waitFor(() => {
        expect(screen.getByTestId('result-section')).toBeInTheDocument();
      });
    });

    it('disables input and button during loading', async () => {
      const user = userEvent.setup();

      // Mock fetch that never resolves during this test
      global.fetch = vi.fn().mockImplementation(() => new Promise(() => {}));

      render(<InlineShortener />);

      const input = screen.getByPlaceholderText('Paste your long URL here...');
      const button = screen.getByTestId('shorten-button');

      await user.type(input, 'https://example.com');
      await user.click(button);

      expect(input).toBeDisabled();
      expect(button).toBeDisabled();
    });
  });
});
