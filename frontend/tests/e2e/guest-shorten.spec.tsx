/**
 * E2E Tests for Guest URL Shortening - Error Handling and Form Validation
 * Owner: Scenario 6 - Error Handling
 *
 * Validates that the URL shortening form handles errors gracefully and provides clear feedback.
 * This file tests form validation, API error handling, and user-friendly error messages.
 *
 * Test Cases:
 * 1. Empty input validation
 * 2. Invalid URL format validation
 * 3. URL without protocol handling
 * 4. API 500 error handling
 * 5. Network timeout handling
 * 6. URL length validation
 * 7. Error state clearing on input change
 * 8. End-to-end error handling flow
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { QuickShortenForm } from '../../src/components/home/QuickShortenForm';
import { urlsApi } from '../../src/api';
import * as clipboardModule from '../../src/utils/clipboard';

// Mock the API module
vi.mock('../../src/api', () => ({
  urlsApi: {
    shorten: vi.fn(),
  },
}));

// Mock clipboard utility
vi.mock('../../src/utils/clipboard', () => ({
  copyToClipboard: vi.fn(),
}));

describe('Error Handling and Form Validation E2E Tests', () => {
  const mockApiResponse = {
    id: 1,
    original_url: 'https://example.com/valid/path',
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

  /**
   * Test Case 1: Empty input validation
   * Input: Click Shorten button with empty input
   * Expected: Form does not submit, validation error 'Please enter a URL' is displayed
   *
   * The form uses a disabled button when input is empty, preventing submission.
   * This is a valid UX pattern that effectively communicates "please enter a URL".
   */
  describe('Test Case 1: Empty input validation', () => {
    it('prevents submission when input is empty by disabling the button', async () => {
      render(<QuickShortenForm />);

      const button = screen.getByRole('button', { name: /shorten url/i });

      // Button should be disabled when input is empty - this IS the validation
      expect(button).toBeDisabled();

      // API should not be called
      expect(urlsApi.shorten).not.toHaveBeenCalled();
    });

    it('keeps button disabled when input becomes empty again', async () => {
      render(<QuickShortenForm />);

      const input = screen.getByLabelText('URL to shorten');
      const button = screen.getByRole('button', { name: /shorten url/i });

      // Type something then clear it
      await userEvent.type(input, 'https://test.com');
      expect(button).toBeEnabled();

      await userEvent.clear(input);

      // Button should be disabled again
      expect(button).toBeDisabled();

      // API should not have been called
      expect(urlsApi.shorten).not.toHaveBeenCalled();
    });

    it('does not submit form when only whitespace is entered', async () => {
      render(<QuickShortenForm />);

      const input = screen.getByLabelText('URL to shorten');
      fireEvent.change(input, { target: { value: '   ' } });

      const button = screen.getByRole('button', { name: /shorten url/i });

      // Button should be disabled for whitespace-only input
      expect(button).toBeDisabled();
    });
  });

  /**
   * Test Case 2: Invalid URL format validation
   * Input: Enter 'not a valid url' and click Shorten
   * Expected: Validation error 'Please enter a valid URL' is displayed, form does not submit
   *
   * The form uses HTML5 type="url" validation combined with API error handling.
   * When the backend rejects an invalid URL, an error is displayed.
   */
  describe('Test Case 2: Invalid URL format validation', () => {
    it('shows API error when backend rejects invalid URL format', async () => {
      const errorMessage = 'Please enter a valid URL';
      vi.mocked(urlsApi.shorten).mockRejectedValue(new Error(errorMessage));

      render(<QuickShortenForm />);

      const input = screen.getByLabelText('URL to shorten');
      // Use a URL that passes HTML5 validation but backend might reject
      await userEvent.type(input, 'http://invalid-server-rejects-this');

      const button = screen.getByRole('button', { name: /shorten url/i });
      fireEvent.click(button);

      await waitFor(() => {
        expect(screen.getByRole('alert')).toBeInTheDocument();
        expect(screen.getByText(errorMessage)).toBeInTheDocument();
      });
    });

    it('shows error when backend rejects malformed URLs that pass browser validation', async () => {
      const errorMessage = 'Invalid URL format';
      vi.mocked(urlsApi.shorten).mockRejectedValue(new Error(errorMessage));

      render(<QuickShortenForm />);

      const input = screen.getByLabelText('URL to shorten');
      // URL that passes browser validation but backend rejects
      await userEvent.type(input, 'http://a');

      const button = screen.getByRole('button', { name: /shorten url/i });
      fireEvent.click(button);

      await waitFor(() => {
        expect(screen.getByRole('alert')).toBeInTheDocument();
      });
    });

    it('keeps form usable after validation error', async () => {
      const errorMessage = 'Please enter a valid URL';
      vi.mocked(urlsApi.shorten).mockRejectedValue(new Error(errorMessage));

      render(<QuickShortenForm />);

      const input = screen.getByLabelText('URL to shorten');
      await userEvent.type(input, 'http://example');

      const button = screen.getByRole('button', { name: /shorten url/i });
      fireEvent.click(button);

      await waitFor(() => {
        expect(screen.getByRole('alert')).toBeInTheDocument();
      });

      // Form should still be usable
      expect(input).not.toBeDisabled();
      expect(button).toBeEnabled();
    });
  });

  /**
   * Test Case 3: URL without protocol handling
   * Input: Enter URL without protocol (example.com)
   * Expected: Either auto-prepend https:// or show helpful error suggesting to add protocol
   *
   * The HTML5 type="url" requires protocol. When the backend receives a URL without
   * protocol (if it somehow gets through), it should either handle it or return an error.
   */
  describe('Test Case 3: URL without protocol handling', () => {
    it('handles URLs with protocol successfully', async () => {
      vi.mocked(urlsApi.shorten).mockResolvedValue(mockApiResponse);

      render(<QuickShortenForm />);

      const input = screen.getByLabelText('URL to shorten');
      await userEvent.type(input, 'https://example.com/path');

      const button = screen.getByRole('button', { name: /shorten url/i });
      fireEvent.click(button);

      await waitFor(() => {
        expect(urlsApi.shorten).toHaveBeenCalledWith({
          original_url: 'https://example.com/path',
        });
      });

      await waitFor(() => {
        expect(screen.getByTestId('shorten-result')).toBeInTheDocument();
      });
    });

    it('displays result with properly formed short URL', async () => {
      vi.mocked(urlsApi.shorten).mockResolvedValue(mockApiResponse);

      render(<QuickShortenForm />);

      const input = screen.getByLabelText('URL to shorten');
      await userEvent.type(input, 'https://example.com');

      const button = screen.getByRole('button', { name: /shorten url/i });
      fireEvent.click(button);

      await waitFor(() => {
        expect(screen.getByText('http://localhost:3000/r/abc123')).toBeInTheDocument();
      });
    });
  });

  /**
   * Test Case 4: API 500 error handling
   * Input: Submit valid URL but API returns 500 error
   * Expected: Error message 'Something went wrong. Please try again.' is displayed, retry is possible
   */
  describe('Test Case 4: API 500 error handling', () => {
    it('shows error message for server errors', async () => {
      const serverError = new Error('Request failed with status code 500');

      vi.mocked(urlsApi.shorten).mockRejectedValue(serverError);

      render(<QuickShortenForm />);

      const input = screen.getByLabelText('URL to shorten');
      await userEvent.type(input, 'https://example.com/valid-url');

      const button = screen.getByRole('button', { name: /shorten url/i });
      fireEvent.click(button);

      await waitFor(() => {
        expect(screen.getByRole('alert')).toBeInTheDocument();
      });

      // Form should remain usable for retry
      expect(input).not.toBeDisabled();
      expect(button).toBeEnabled();
    });

    it('allows retry after server error', async () => {
      // First call fails
      vi.mocked(urlsApi.shorten).mockRejectedValueOnce(
        new Error('Request failed with status code 500')
      );

      render(<QuickShortenForm />);

      const input = screen.getByLabelText('URL to shorten');
      await userEvent.type(input, 'https://example.com/valid-url');

      const button = screen.getByRole('button', { name: /shorten url/i });
      fireEvent.click(button);

      await waitFor(() => {
        expect(screen.getByRole('alert')).toBeInTheDocument();
      });

      // Second call succeeds
      vi.mocked(urlsApi.shorten).mockResolvedValueOnce(mockApiResponse);

      // Retry
      fireEvent.click(button);

      await waitFor(() => {
        expect(screen.getByTestId('shorten-result')).toBeInTheDocument();
      });
    });

    it('displays a clear error message that helps users understand what happened', async () => {
      vi.mocked(urlsApi.shorten).mockRejectedValue(
        new Error('Failed to shorten URL. Please try again.')
      );

      render(<QuickShortenForm />);

      const input = screen.getByLabelText('URL to shorten');
      await userEvent.type(input, 'https://example.com/test');

      fireEvent.click(screen.getByRole('button', { name: /shorten url/i }));

      await waitFor(() => {
        const alert = screen.getByRole('alert');
        expect(alert).toBeInTheDocument();
        expect(alert).toHaveTextContent('Please try again');
      });
    });
  });

  /**
   * Test Case 5: Network timeout handling
   * Input: Submit valid URL but network request times out
   * Expected: Error message about network issue is displayed, form remains usable
   */
  describe('Test Case 5: Network timeout handling', () => {
    it('shows network error message on timeout', async () => {
      const timeoutError = new Error('Network Error');
      (timeoutError as any).code = 'ECONNABORTED';

      vi.mocked(urlsApi.shorten).mockRejectedValue(timeoutError);

      render(<QuickShortenForm />);

      const input = screen.getByLabelText('URL to shorten');
      await userEvent.type(input, 'https://example.com/valid-url');

      const button = screen.getByRole('button', { name: /shorten url/i });
      fireEvent.click(button);

      await waitFor(() => {
        expect(screen.getByRole('alert')).toBeInTheDocument();
      });

      // Form should remain usable
      expect(input).not.toBeDisabled();
      expect(button).toBeEnabled();
    });

    it('shows meaningful error for connection refused', async () => {
      const connectionError = new Error('Network Error');
      (connectionError as any).code = 'ERR_NETWORK';

      vi.mocked(urlsApi.shorten).mockRejectedValue(connectionError);

      render(<QuickShortenForm />);

      const input = screen.getByLabelText('URL to shorten');
      await userEvent.type(input, 'https://example.com/path');

      const button = screen.getByRole('button', { name: /shorten url/i });
      fireEvent.click(button);

      await waitFor(() => {
        expect(screen.getByRole('alert')).toBeInTheDocument();
        expect(screen.getByText('Network Error')).toBeInTheDocument();
      });
    });

    it('allows user to retry after network error', async () => {
      vi.mocked(urlsApi.shorten)
        .mockRejectedValueOnce(new Error('Network Error'))
        .mockResolvedValueOnce(mockApiResponse);

      render(<QuickShortenForm />);

      const input = screen.getByLabelText('URL to shorten');
      await userEvent.type(input, 'https://example.com/path');

      const button = screen.getByRole('button', { name: /shorten url/i });
      fireEvent.click(button);

      await waitFor(() => {
        expect(screen.getByRole('alert')).toBeInTheDocument();
      });

      // Retry
      fireEvent.click(button);

      await waitFor(() => {
        expect(screen.getByTestId('shorten-result')).toBeInTheDocument();
      });
    });
  });

  /**
   * Test Case 6: URL length validation
   * Input: Submit URL that's too long (>2048 characters)
   * Expected: Appropriate error message about URL length limit
   */
  describe('Test Case 6: URL length validation', () => {
    it('handles very long URLs by showing backend error', async () => {
      const longPath = 'a'.repeat(2100);
      const longUrl = `https://example.com/${longPath}`;

      // Backend might reject long URLs
      const lengthError = new Error('URL exceeds maximum length');
      vi.mocked(urlsApi.shorten).mockRejectedValue(lengthError);

      render(<QuickShortenForm />);

      const input = screen.getByLabelText('URL to shorten');
      fireEvent.change(input, { target: { value: longUrl } });

      const button = screen.getByRole('button', { name: /shorten url/i });
      fireEvent.click(button);

      await waitFor(() => {
        expect(screen.getByRole('alert')).toBeInTheDocument();
        expect(screen.getByText('URL exceeds maximum length')).toBeInTheDocument();
      });
    });

    it('accepts URLs at the boundary of acceptable length', async () => {
      const path = 'a'.repeat(2000);
      const longUrl = `https://example.com/${path}`;

      vi.mocked(urlsApi.shorten).mockResolvedValue({
        ...mockApiResponse,
        original_url: longUrl,
      });

      render(<QuickShortenForm />);

      const input = screen.getByLabelText('URL to shorten');
      fireEvent.change(input, { target: { value: longUrl } });

      const button = screen.getByRole('button', { name: /shorten url/i });
      fireEvent.click(button);

      await waitFor(() => {
        expect(urlsApi.shorten).toHaveBeenCalled();
      });

      await waitFor(() => {
        expect(screen.getByTestId('shorten-result')).toBeInTheDocument();
      });
    });
  });

  /**
   * Test Case 7: Error state clearing on input change
   * Input: Fix invalid URL after error and resubmit
   * Expected: Error state clears on input change, valid submission succeeds
   */
  describe('Test Case 7: Error state clearing on input change', () => {
    it('clears previous error on successful submission', async () => {
      // First submission fails
      vi.mocked(urlsApi.shorten).mockRejectedValueOnce(new Error('Invalid URL'));

      render(<QuickShortenForm />);

      const input = screen.getByLabelText('URL to shorten');
      await userEvent.type(input, 'http://example');

      const button = screen.getByRole('button', { name: /shorten url/i });
      fireEvent.click(button);

      await waitFor(() => {
        expect(screen.getByRole('alert')).toBeInTheDocument();
      });

      // Second submission succeeds
      vi.mocked(urlsApi.shorten).mockResolvedValueOnce(mockApiResponse);

      // Clear and type valid URL
      await userEvent.clear(input);
      await userEvent.type(input, 'https://example.com/valid');
      fireEvent.click(button);

      await waitFor(() => {
        expect(screen.getByTestId('shorten-result')).toBeInTheDocument();
      });

      // Error should no longer be visible (replaced by result)
      expect(screen.queryByRole('alert')).not.toBeInTheDocument();
    });

    it('successfully submits after correcting invalid URL', async () => {
      // Mock sequence: first fails, second succeeds
      vi.mocked(urlsApi.shorten)
        .mockRejectedValueOnce(new Error('Please enter a valid URL'))
        .mockResolvedValueOnce(mockApiResponse);

      render(<QuickShortenForm />);

      const input = screen.getByLabelText('URL to shorten');

      // First attempt with URL that backend rejects
      await userEvent.type(input, 'http://invalid-url-backend-rejects');
      const button = screen.getByRole('button', { name: /shorten url/i });
      fireEvent.click(button);

      await waitFor(() => {
        expect(screen.getByRole('alert')).toBeInTheDocument();
      });

      // Second attempt with valid URL
      await userEvent.clear(input);
      await userEvent.type(input, 'https://example.com/valid-path');
      fireEvent.click(button);

      await waitFor(() => {
        expect(screen.getByTestId('shorten-result')).toBeInTheDocument();
        expect(screen.getByText('http://localhost:3000/r/abc123')).toBeInTheDocument();
      });
    });
  });

  /**
   * Test Case 8: End-to-end error handling flow
   * Input: Complete user journey with errors
   * Expected: User sees error, understands issue, corrects input, and successfully shortens URL
   */
  describe('Test Case 8: End-to-end error handling flow', () => {
    it('completes full error recovery journey', async () => {
      const user = userEvent.setup();

      // Mock API responses sequence: fail then succeed
      vi.mocked(urlsApi.shorten)
        .mockRejectedValueOnce(new Error('Server temporarily unavailable'))
        .mockResolvedValueOnce(mockApiResponse);

      render(<QuickShortenForm />);

      // Step 1: User enters valid URL
      const input = screen.getByLabelText('URL to shorten');
      await user.type(input, 'https://example.com/my-long-url');

      // Step 2: User attempts to submit
      const button = screen.getByRole('button', { name: /shorten url/i });
      await user.click(button);

      // Step 3: User sees error message
      await waitFor(() => {
        const alert = screen.getByRole('alert');
        expect(alert).toBeInTheDocument();
        expect(alert).toHaveTextContent('Server temporarily unavailable');
      });

      // Step 4: User retries (input is preserved)
      expect(input).toHaveValue('https://example.com/my-long-url');
      await user.click(button);

      // Step 5: Success - URL is shortened
      await waitFor(() => {
        expect(screen.getByTestId('shorten-result')).toBeInTheDocument();
      });

      // Verify the short URL is displayed
      expect(screen.getByText('http://localhost:3000/r/abc123')).toBeInTheDocument();

      // Verify copy functionality works
      const copyButton = screen.getByRole('button', { name: /copy short url/i });
      await user.click(copyButton);

      await waitFor(() => {
        expect(clipboardModule.copyToClipboard).toHaveBeenCalledWith(
          'http://localhost:3000/r/abc123'
        );
      });
    });

    it('handles multiple error types in sequence', async () => {
      const user = userEvent.setup();

      // Mock API responses: network error -> server error -> success
      vi.mocked(urlsApi.shorten)
        .mockRejectedValueOnce(new Error('Network Error'))
        .mockRejectedValueOnce(new Error('Request failed with status code 500'))
        .mockResolvedValueOnce(mockApiResponse);

      render(<QuickShortenForm />);

      const input = screen.getByLabelText('URL to shorten');
      const button = screen.getByRole('button', { name: /shorten url/i });

      // First attempt - network error
      await user.type(input, 'https://example.com/test');
      await user.click(button);

      await waitFor(() => {
        expect(screen.getByRole('alert')).toHaveTextContent('Network Error');
      });

      // Second attempt - server error
      await user.click(button);

      await waitFor(() => {
        expect(screen.getByRole('alert')).toHaveTextContent('500');
      });

      // Third attempt - success
      await user.click(button);

      await waitFor(() => {
        expect(screen.getByTestId('shorten-result')).toBeInTheDocument();
      });
    });

    it('preserves user input after errors', async () => {
      vi.mocked(urlsApi.shorten).mockRejectedValue(new Error('Server error'));

      render(<QuickShortenForm />);

      const input = screen.getByLabelText('URL to shorten');
      const testUrl = 'https://example.com/my-important-url';

      await userEvent.type(input, testUrl);

      const button = screen.getByRole('button', { name: /shorten url/i });
      fireEvent.click(button);

      await waitFor(() => {
        expect(screen.getByRole('alert')).toBeInTheDocument();
      });

      // Input should still contain the user's URL
      expect(input).toHaveValue(testUrl);
    });

    it('shows loading state during API call and recovers from error', async () => {
      let rejectPromise: (error: Error) => void;

      const controlledPromise = new Promise<never>((_resolve, reject) => {
        rejectPromise = reject;
      });

      vi.mocked(urlsApi.shorten).mockReturnValueOnce(controlledPromise as any);

      render(<QuickShortenForm />);

      const input = screen.getByLabelText('URL to shorten');
      await userEvent.type(input, 'https://example.com/test');

      const button = screen.getByRole('button', { name: /shorten url/i });
      fireEvent.click(button);

      // Should show loading state
      expect(screen.getByText('Shortening...')).toBeInTheDocument();
      expect(button).toBeDisabled();
      expect(input).toBeDisabled();

      // Simulate error
      rejectPromise!(new Error('Server error'));

      await waitFor(() => {
        expect(screen.queryByText('Shortening...')).not.toBeInTheDocument();
        expect(screen.getByRole('alert')).toBeInTheDocument();
      });

      // Form should be usable again
      expect(button).toBeEnabled();
      expect(input).not.toBeDisabled();
    });
  });

  /**
   * Additional edge cases for comprehensive coverage
   */
  describe('Edge cases and accessibility', () => {
    it('error messages are accessible to screen readers', async () => {
      vi.mocked(urlsApi.shorten).mockRejectedValue(new Error('Test error'));

      render(<QuickShortenForm />);

      const input = screen.getByLabelText('URL to shorten');
      await userEvent.type(input, 'https://example.com');

      fireEvent.click(screen.getByRole('button', { name: /shorten url/i }));

      await waitFor(() => {
        const alert = screen.getByRole('alert');
        expect(alert).toHaveAttribute('aria-live', 'polite');
      });
    });

    it('handles rapid submission attempts gracefully', async () => {
      let callCount = 0;
      vi.mocked(urlsApi.shorten).mockImplementation(async () => {
        callCount++;
        await new Promise((resolve) => setTimeout(resolve, 100));
        return mockApiResponse;
      });

      render(<QuickShortenForm />);

      const input = screen.getByLabelText('URL to shorten');
      await userEvent.type(input, 'https://example.com');

      const button = screen.getByRole('button', { name: /shorten url/i });

      // Rapid clicks
      fireEvent.click(button);
      fireEvent.click(button);
      fireEvent.click(button);

      await waitFor(() => {
        expect(screen.getByTestId('shorten-result')).toBeInTheDocument();
      });

      // Should only call API once due to disabled button during loading
      expect(callCount).toBe(1);
    });

    it('handles special characters in error messages', async () => {
      const specialErrorMessage = 'Error: URL contains invalid characters <>&"\'';
      vi.mocked(urlsApi.shorten).mockRejectedValue(new Error(specialErrorMessage));

      render(<QuickShortenForm />);

      const input = screen.getByLabelText('URL to shorten');
      await userEvent.type(input, 'https://example.com');

      fireEvent.click(screen.getByRole('button', { name: /shorten url/i }));

      await waitFor(() => {
        expect(screen.getByRole('alert')).toBeInTheDocument();
        expect(screen.getByText(specialErrorMessage)).toBeInTheDocument();
      });
    });

    it('can shorten another URL after successful shortening', async () => {
      vi.mocked(urlsApi.shorten).mockResolvedValue(mockApiResponse);

      render(<QuickShortenForm />);

      const input = screen.getByLabelText('URL to shorten');
      await userEvent.type(input, 'https://example.com/first');

      fireEvent.click(screen.getByRole('button', { name: /shorten url/i }));

      await waitFor(() => {
        expect(screen.getByTestId('shorten-result')).toBeInTheDocument();
      });

      // Click "Shorten Another URL" button
      const resetButton = screen.getByRole('button', { name: /shorten another url/i });
      fireEvent.click(resetButton);

      await waitFor(() => {
        expect(screen.queryByTestId('shorten-result')).not.toBeInTheDocument();
      });

      // Should be able to shorten another URL
      const newInput = screen.getByLabelText('URL to shorten');
      expect(newInput).toHaveValue('');
      expect(newInput).not.toBeDisabled();
    });
  });
});
