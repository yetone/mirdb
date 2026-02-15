/**
 * Unit Tests for QuickShortenForm Component
 * Owner: Scenario 2 - Guest URL Shortening
 *
 * Test cases 1-4:
 * 1. Form displays with URL input field, 'Shorten' button, and placeholder text
 * 2. API call is made to POST /api/urls/ and loading state is shown during request
 * 3. Result section displays: original URL, short URL, copy button, and registration prompt
 * 4. Short URL is copied to clipboard and visual feedback is shown
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { QuickShortenForm } from '../../../../src/components/home/QuickShortenForm';
import * as useGuestShortenModule from '../../../../src/hooks/useGuestShorten';
import * as clipboardModule from '../../../../src/utils/clipboard';

// Mock the hook
vi.mock('../../../../src/hooks/useGuestShorten', () => ({
  useGuestShorten: vi.fn(),
}));

// Mock clipboard utility
vi.mock('../../../../src/utils/clipboard', () => ({
  copyToClipboard: vi.fn(),
}));

describe('QuickShortenForm', () => {
  const mockShortenUrl = vi.fn();
  const mockReset = vi.fn();

  const defaultHookReturn = {
    shortenUrl: mockShortenUrl,
    isLoading: false,
    error: null,
    result: null,
    reset: mockReset,
  };

  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(useGuestShortenModule.useGuestShorten).mockReturnValue(defaultHookReturn);
    vi.mocked(clipboardModule.copyToClipboard).mockResolvedValue(true);
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  // Test Case 1: Form renders correctly
  describe('Test Case 1: Form renders correctly', () => {
    it('displays URL input field with placeholder text', () => {
      render(<QuickShortenForm />);

      const input = screen.getByLabelText('URL to shorten');
      expect(input).toBeInTheDocument();
      expect(input).toHaveAttribute('placeholder', 'Paste your long URL here...');
      expect(input).toHaveAttribute('type', 'url');
    });

    it('displays Shorten button', () => {
      render(<QuickShortenForm />);

      const button = screen.getByRole('button', { name: /shorten url/i });
      expect(button).toBeInTheDocument();
      expect(button).toHaveTextContent('Shorten');
    });

    it('button is disabled when input is empty', () => {
      render(<QuickShortenForm />);

      const button = screen.getByRole('button', { name: /shorten url/i });
      expect(button).toBeDisabled();
    });

    it('button is enabled when URL is entered', () => {
      render(<QuickShortenForm />);

      const input = screen.getByLabelText('URL to shorten');
      fireEvent.change(input, { target: { value: 'https://example.com/long/path' } });

      const button = screen.getByRole('button', { name: /shorten url/i });
      expect(button).toBeEnabled();
    });
  });

  // Test Case 2: API call and loading state
  describe('Test Case 2: API call and loading state', () => {
    it('calls shortenUrl when form is submitted with valid URL', async () => {
      mockShortenUrl.mockResolvedValue({
        originalUrl: 'https://example.com/long/path',
        shortUrl: 'http://short.url/abc123',
        shortCode: 'abc123',
      });

      render(<QuickShortenForm />);

      const input = screen.getByLabelText('URL to shorten');
      fireEvent.change(input, { target: { value: 'https://example.com/long/path' } });

      const button = screen.getByRole('button', { name: /shorten url/i });
      fireEvent.click(button);

      await waitFor(() => {
        expect(mockShortenUrl).toHaveBeenCalledWith('https://example.com/long/path');
      });
    });

    it('shows loading state during API request', () => {
      vi.mocked(useGuestShortenModule.useGuestShorten).mockReturnValue({
        ...defaultHookReturn,
        isLoading: true,
      });

      render(<QuickShortenForm />);

      expect(screen.getByText('Shortening...')).toBeInTheDocument();
      const button = screen.getByRole('button', { name: /shorten url/i });
      expect(button).toBeDisabled();
    });

    it('disables input during loading', () => {
      vi.mocked(useGuestShortenModule.useGuestShorten).mockReturnValue({
        ...defaultHookReturn,
        isLoading: true,
      });

      render(<QuickShortenForm />);

      const input = screen.getByLabelText('URL to shorten');
      expect(input).toBeDisabled();
    });
  });

  // Test Case 3: Result display
  describe('Test Case 3: Result section displays correctly', () => {
    const mockResult = {
      originalUrl: 'https://example.com/very/long/path/to/resource',
      shortUrl: 'http://short.url/abc123',
      shortCode: 'abc123',
    };

    it('displays original URL', () => {
      vi.mocked(useGuestShortenModule.useGuestShorten).mockReturnValue({
        ...defaultHookReturn,
        result: mockResult,
      });

      render(<QuickShortenForm />);

      expect(screen.getByText('Original URL:')).toBeInTheDocument();
      expect(screen.getByText(/example\.com/)).toBeInTheDocument();
    });

    it('displays short URL', () => {
      vi.mocked(useGuestShortenModule.useGuestShorten).mockReturnValue({
        ...defaultHookReturn,
        result: mockResult,
      });

      render(<QuickShortenForm />);

      expect(screen.getByText('Short URL:')).toBeInTheDocument();
      expect(screen.getByText('http://short.url/abc123')).toBeInTheDocument();
    });

    it('displays copy button', () => {
      vi.mocked(useGuestShortenModule.useGuestShorten).mockReturnValue({
        ...defaultHookReturn,
        result: mockResult,
      });

      render(<QuickShortenForm />);

      const copyButton = screen.getByRole('button', { name: /copy short url to clipboard/i });
      expect(copyButton).toBeInTheDocument();
    });

    it('displays registration prompt', () => {
      vi.mocked(useGuestShortenModule.useGuestShorten).mockReturnValue({
        ...defaultHookReturn,
        result: mockResult,
      });

      render(<QuickShortenForm />);

      expect(screen.getByTestId('registration-prompt')).toBeInTheDocument();
      expect(screen.getByText(/track clicks/i)).toBeInTheDocument();
      expect(screen.getByText(/detailed analytics/i)).toBeInTheDocument();
    });

    it('truncates long original URLs', () => {
      const longUrl = 'https://example.com/' + 'a'.repeat(100);
      vi.mocked(useGuestShortenModule.useGuestShorten).mockReturnValue({
        ...defaultHookReturn,
        result: { ...mockResult, originalUrl: longUrl },
      });

      render(<QuickShortenForm />);

      // The URL should be truncated and contain ellipsis
      const urlDisplay = screen.getByText(/example\.com/);
      expect(urlDisplay.textContent).toContain('...');
      expect(urlDisplay.textContent!.length).toBeLessThan(longUrl.length);
    });
  });

  // Test Case 4: Copy to clipboard functionality
  describe('Test Case 4: Copy to clipboard and visual feedback', () => {
    const mockResult = {
      originalUrl: 'https://example.com/path',
      shortUrl: 'http://short.url/abc123',
      shortCode: 'abc123',
    };

    it('copies short URL to clipboard when copy button is clicked', async () => {
      vi.mocked(useGuestShortenModule.useGuestShorten).mockReturnValue({
        ...defaultHookReturn,
        result: mockResult,
      });

      render(<QuickShortenForm />);

      const copyButton = screen.getByRole('button', { name: /copy short url to clipboard/i });
      fireEvent.click(copyButton);

      await waitFor(() => {
        expect(clipboardModule.copyToClipboard).toHaveBeenCalledWith('http://short.url/abc123');
      });
    });

    it('shows visual feedback after successful copy', async () => {
      vi.mocked(useGuestShortenModule.useGuestShorten).mockReturnValue({
        ...defaultHookReturn,
        result: mockResult,
      });
      vi.mocked(clipboardModule.copyToClipboard).mockResolvedValue(true);

      render(<QuickShortenForm />);

      const copyButton = screen.getByRole('button', { name: /copy short url to clipboard/i });
      fireEvent.click(copyButton);

      await waitFor(() => {
        expect(screen.getByText('Copied to clipboard!')).toBeInTheDocument();
      });
    });

    it('changes button state after copy', async () => {
      vi.mocked(useGuestShortenModule.useGuestShorten).mockReturnValue({
        ...defaultHookReturn,
        result: mockResult,
      });
      vi.mocked(clipboardModule.copyToClipboard).mockResolvedValue(true);

      render(<QuickShortenForm />);

      const copyButton = screen.getByRole('button', { name: /copy short url to clipboard/i });
      fireEvent.click(copyButton);

      await waitFor(() => {
        expect(screen.getByRole('button', { name: /copied to clipboard/i })).toBeInTheDocument();
      });
    });
  });

  // Additional tests for error handling and callbacks
  describe('Error handling', () => {
    it('displays error message when error occurs', () => {
      vi.mocked(useGuestShortenModule.useGuestShorten).mockReturnValue({
        ...defaultHookReturn,
        error: 'Failed to shorten URL',
      });

      render(<QuickShortenForm />);

      expect(screen.getByRole('alert')).toBeInTheDocument();
      expect(screen.getByText('Failed to shorten URL')).toBeInTheDocument();
    });
  });

  describe('Callbacks', () => {
    it('calls onSuccess callback when URL is shortened successfully', async () => {
      const onSuccess = vi.fn();
      mockShortenUrl.mockResolvedValue({
        originalUrl: 'https://example.com/path',
        shortUrl: 'http://short.url/abc123',
        shortCode: 'abc123',
      });

      render(<QuickShortenForm onSuccess={onSuccess} />);

      const input = screen.getByLabelText('URL to shorten');
      fireEvent.change(input, { target: { value: 'https://example.com/path' } });

      const button = screen.getByRole('button', { name: /shorten url/i });
      fireEvent.click(button);

      await waitFor(() => {
        expect(onSuccess).toHaveBeenCalledWith('http://short.url/abc123');
      });
    });

    it('calls onError callback when shortening fails', async () => {
      const onError = vi.fn();
      mockShortenUrl.mockRejectedValue(new Error('Network error'));

      render(<QuickShortenForm onError={onError} />);

      const input = screen.getByLabelText('URL to shorten');
      fireEvent.change(input, { target: { value: 'https://example.com/path' } });

      const button = screen.getByRole('button', { name: /shorten url/i });
      fireEvent.click(button);

      await waitFor(() => {
        expect(onError).toHaveBeenCalledWith('Network error');
      });
    });
  });

  describe('Reset functionality', () => {
    it('calls reset and clears input when "Shorten Another URL" is clicked', async () => {
      vi.mocked(useGuestShortenModule.useGuestShorten).mockReturnValue({
        ...defaultHookReturn,
        result: {
          originalUrl: 'https://example.com/path',
          shortUrl: 'http://short.url/abc123',
          shortCode: 'abc123',
        },
      });

      render(<QuickShortenForm />);

      const resetButton = screen.getByRole('button', { name: /shorten another url/i });
      fireEvent.click(resetButton);

      expect(mockReset).toHaveBeenCalled();
    });
  });
});
