/**
 * Integration Tests for Homepage Guest URL Shortening Flow
 * Owner: Scenario 2 - Guest URL Shortening
 *
 * Test case 6: End-to-end guest URL shortening flow
 * Expected: User can enter URL, click shorten, see result, copy to clipboard, and form clears for another attempt
 */

import React from 'react';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, waitFor, fireEvent } from '@testing-library/react';
import { QuickShortenForm } from '../../src/components/home/QuickShortenForm';
import { urlsApi } from '../../src/api';
import * as clipboardModule from '../../src/utils/clipboard';

// Mock the API module
vi.mock('../../src/api', () => ({
  urlsApi: {
    shorten: vi.fn(),
  },
}));

// Mock our clipboard utility instead of navigator.clipboard
vi.mock('../../src/utils/clipboard', () => ({
  copyToClipboard: vi.fn(),
}));

describe('Guest URL Shortening Integration', () => {
  const mockApiResponse = {
    id: 1,
    original_url: 'https://example.com/very/long/path/to/resource?param=value',
    short_code: 'abc123',
    created_at: '2026-02-15T00:00:00Z',
    user_id: null,
    click_count: 0,
  };

  beforeEach(() => {
    vi.clearAllMocks();
    Object.defineProperty(window, 'location', {
      value: { origin: 'http://localhost:3000' },
      writable: true,
    });
    vi.mocked(clipboardModule.copyToClipboard).mockResolvedValue(true);
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('Test Case 6: Complete guest URL shortening flow', () => {
    it('allows user to enter URL, shorten it, see result, and copy to clipboard', async () => {
      vi.mocked(urlsApi.shorten).mockResolvedValue(mockApiResponse);

      render(<QuickShortenForm />);

      // Step 1: Find and interact with the shortening form
      const input = screen.getByLabelText('URL to shorten');
      expect(input).toBeInTheDocument();
      expect(input).toHaveValue('');

      // Step 2: Enter valid URL
      const testUrl = 'https://example.com/very/long/path/to/resource?param=value';
      fireEvent.change(input, { target: { value: testUrl } });
      expect(input).toHaveValue(testUrl);

      // Step 3: Submit form
      const submitButton = screen.getByRole('button', { name: /shorten url/i });
      expect(submitButton).toBeEnabled();
      fireEvent.click(submitButton);

      // Verify API was called correctly
      expect(urlsApi.shorten).toHaveBeenCalledWith({ original_url: testUrl });

      // Step 4: View result
      await waitFor(() => {
        expect(screen.getByTestId('shorten-result')).toBeInTheDocument();
      });

      // Verify original URL is displayed
      expect(screen.getByText('Original URL:')).toBeInTheDocument();

      // Verify short URL is displayed
      expect(screen.getByText('Short URL:')).toBeInTheDocument();
      expect(screen.getByText('http://localhost:3000/r/abc123')).toBeInTheDocument();

      // Verify copy button is present
      const copyButton = screen.getByRole('button', { name: /copy short url to clipboard/i });
      expect(copyButton).toBeInTheDocument();

      // Step 5: Copy to clipboard
      fireEvent.click(copyButton);

      // Verify clipboard was written
      await waitFor(() => {
        expect(clipboardModule.copyToClipboard).toHaveBeenCalledWith('http://localhost:3000/r/abc123');
      });

      // Verify visual feedback
      await waitFor(() => {
        expect(screen.getByText('Copied to clipboard!')).toBeInTheDocument();
      });

      // Step 6: View registration prompt
      expect(screen.getByTestId('registration-prompt')).toBeInTheDocument();
      expect(screen.getByText(/track clicks/i)).toBeInTheDocument();
      expect(screen.getByText(/analytics/i)).toBeInTheDocument();
    });

    it('allows user to shorten another URL after first one', async () => {
      vi.mocked(urlsApi.shorten)
        .mockResolvedValueOnce(mockApiResponse)
        .mockResolvedValueOnce({
          ...mockApiResponse,
          id: 2,
          original_url: 'https://another-example.com/path',
          short_code: 'xyz789',
        });

      render(<QuickShortenForm />);

      // First URL shortening
      const input = screen.getByLabelText('URL to shorten');
      fireEvent.change(input, { target: { value: 'https://example.com/very/long/path/to/resource?param=value' } });
      fireEvent.click(screen.getByRole('button', { name: /shorten url/i }));

      await waitFor(() => {
        expect(screen.getByTestId('shorten-result')).toBeInTheDocument();
      });

      // Click "Shorten Another URL"
      const resetButton = screen.getByRole('button', { name: /shorten another url/i });
      fireEvent.click(resetButton);

      // Result should be cleared
      await waitFor(() => {
        expect(screen.queryByTestId('shorten-result')).not.toBeInTheDocument();
      });

      // Enter and shorten a second URL
      const newInput = screen.getByLabelText('URL to shorten');
      fireEvent.change(newInput, { target: { value: 'https://another-example.com/path' } });
      fireEvent.click(screen.getByRole('button', { name: /shorten url/i }));

      await waitFor(() => {
        expect(screen.getByText('http://localhost:3000/r/xyz789')).toBeInTheDocument();
      });
    });

    it('shows loading state during API request', async () => {
      // Create a delayed promise to simulate loading
      let resolveApi: (value: typeof mockApiResponse) => void;
      const delayedPromise = new Promise<typeof mockApiResponse>((resolve) => {
        resolveApi = resolve;
      });
      vi.mocked(urlsApi.shorten).mockReturnValue(delayedPromise);

      render(<QuickShortenForm />);

      const input = screen.getByLabelText('URL to shorten');
      fireEvent.change(input, { target: { value: 'https://example.com/path' } });
      fireEvent.click(screen.getByRole('button', { name: /shorten url/i }));

      // Should show loading state
      expect(screen.getByText('Shortening...')).toBeInTheDocument();

      // Resolve the API call
      resolveApi!(mockApiResponse);

      // Should show result
      await waitFor(() => {
        expect(screen.queryByText('Shortening...')).not.toBeInTheDocument();
        expect(screen.getByTestId('shorten-result')).toBeInTheDocument();
      });
    });

    it('handles API errors gracefully', async () => {
      vi.mocked(urlsApi.shorten).mockRejectedValue(new Error('Network error'));

      render(<QuickShortenForm />);

      const input = screen.getByLabelText('URL to shorten');
      fireEvent.change(input, { target: { value: 'https://example.com/path' } });
      fireEvent.click(screen.getByRole('button', { name: /shorten url/i }));

      // Should show error
      await waitFor(() => {
        expect(screen.getByRole('alert')).toBeInTheDocument();
        expect(screen.getByText('Network error')).toBeInTheDocument();
      });

      // Result section should not appear
      expect(screen.queryByTestId('shorten-result')).not.toBeInTheDocument();
    });

    it('trims whitespace from URLs before submission', async () => {
      vi.mocked(urlsApi.shorten).mockResolvedValue(mockApiResponse);

      render(<QuickShortenForm />);

      const input = screen.getByLabelText('URL to shorten');
      fireEvent.change(input, { target: { value: '  https://example.com/path  ' } });
      fireEvent.click(screen.getByRole('button', { name: /shorten url/i }));

      expect(urlsApi.shorten).toHaveBeenCalledWith({ original_url: 'https://example.com/path' });
    });
  });
});
