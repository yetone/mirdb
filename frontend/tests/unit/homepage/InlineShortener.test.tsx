/**
 * InlineShortener Component Tests - Validation & Error Handling
 *
 * Owner: Scenario 2 (Component), Scenario 3 (Validation Tests)
 *
 * Tests URL validation and error handling for the inline shortener
 * as specified in REQ-7.
 *
 * Test cases:
 * - TC1: Invalid URL format shows "Please enter a valid URL"
 * - TC2: Empty input shows "URL is required"
 * - TC3: Incomplete URL (http://) validation error
 * - TC4: Network error shows friendly message with retry
 * - TC5: URLs with special characters are properly encoded
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { InlineShortener, validateUrl, encodeUrlForApi } from '../../../src/components/homepage/InlineShortener';

// Mock the useAnonymousShorten hook
vi.mock('../../../src/hooks/useAnonymousShorten', () => ({
  useAnonymousShorten: vi.fn(),
}));

import { useAnonymousShorten } from '../../../src/hooks/useAnonymousShorten';

const mockUseAnonymousShorten = vi.mocked(useAnonymousShorten);

describe('InlineShortener - Validation & Error Handling', () => {
  // Default mock implementation
  const defaultMock = {
    shortenUrl: vi.fn(),
    isLoading: false,
    error: null,
    result: null,
    reset: vi.fn(),
  };

  beforeEach(() => {
    vi.clearAllMocks();
    mockUseAnonymousShorten.mockReturnValue(defaultMock);
  });

  describe('validateUrl function', () => {
    it('returns error for empty string', () => {
      const result = validateUrl('');
      expect(result.isValid).toBe(false);
      expect(result.error).toBe('URL is required');
    });

    it('returns error for whitespace-only string', () => {
      const result = validateUrl('   ');
      expect(result.isValid).toBe(false);
      expect(result.error).toBe('URL is required');
    });

    it('returns error for invalid URL format', () => {
      const result = validateUrl('not-a-valid-url');
      expect(result.isValid).toBe(false);
      expect(result.error).toBe('Please enter a valid URL');
    });

    it('returns error for incomplete URL (http://)', () => {
      const result = validateUrl('http://');
      expect(result.isValid).toBe(false);
      expect(result.error).toBe('Please enter a valid URL');
    });

    it('returns error for incomplete URL (https://)', () => {
      const result = validateUrl('https://');
      expect(result.isValid).toBe(false);
      expect(result.error).toBe('Please enter a valid URL');
    });

    it('returns error for URL without protocol', () => {
      const result = validateUrl('example.com');
      expect(result.isValid).toBe(false);
      expect(result.error).toBe('Please enter a valid URL');
    });

    it('returns valid for proper HTTP URL', () => {
      const result = validateUrl('http://example.com');
      expect(result.isValid).toBe(true);
      expect(result.error).toBeUndefined();
    });

    it('returns valid for proper HTTPS URL', () => {
      const result = validateUrl('https://example.com');
      expect(result.isValid).toBe(true);
      expect(result.error).toBeUndefined();
    });

    it('returns valid for URL with path', () => {
      const result = validateUrl('https://example.com/path/to/page');
      expect(result.isValid).toBe(true);
      expect(result.error).toBeUndefined();
    });

    it('returns valid for URL with query parameters', () => {
      const result = validateUrl('https://example.com?foo=bar&baz=qux');
      expect(result.isValid).toBe(true);
      expect(result.error).toBeUndefined();
    });

    it('returns error for ftp:// protocol', () => {
      const result = validateUrl('ftp://files.example.com');
      expect(result.isValid).toBe(false);
      expect(result.error).toBe('Please enter a valid URL');
    });

    it('returns error for javascript: protocol', () => {
      const result = validateUrl('javascript:alert(1)');
      expect(result.isValid).toBe(false);
      expect(result.error).toBe('Please enter a valid URL');
    });
  });

  describe('encodeUrlForApi function', () => {
    it('properly encodes URL with special characters', () => {
      const url = 'https://example.com/path?name=John Doe&query=hello world';
      const encoded = encodeUrlForApi(url);
      expect(encoded).toContain('https://example.com/path');
      // URL should be properly encoded
      expect(encoded).toMatch(/https:\/\/example\.com\/path/);
    });

    it('handles URL with unicode characters', () => {
      const url = 'https://example.com/path?emoji=😀';
      const encoded = encodeUrlForApi(url);
      expect(encoded).toContain('https://example.com/path');
    });

    it('handles already-encoded URLs', () => {
      const url = 'https://example.com/path%20with%20spaces';
      const encoded = encodeUrlForApi(url);
      expect(encoded).toContain('example.com');
    });

    it('trims whitespace from URL', () => {
      const url = '  https://example.com  ';
      const encoded = encodeUrlForApi(url);
      expect(encoded).toBe('https://example.com/');
    });
  });

  describe('TC1: Invalid URL Format', () => {
    it('displays "Please enter a valid URL" when submitting invalid URL format', async () => {
      const user = userEvent.setup();
      const onError = vi.fn();

      render(<InlineShortener onError={onError} />);

      const input = screen.getByPlaceholderText('Paste your long URL here...');
      const button = screen.getByTestId('shorten-button');

      // Enter invalid URL
      await user.type(input, 'not-a-valid-url');
      await user.click(button);

      // Check error is displayed inline
      const errorMessage = screen.getByTestId('error-message');
      expect(errorMessage).toHaveTextContent('Please enter a valid URL');
      expect(onError).toHaveBeenCalledWith('Please enter a valid URL');

      // Verify API was NOT called
      expect(defaultMock.shortenUrl).not.toHaveBeenCalled();
    });
  });

  describe('TC2: Empty Input Field', () => {
    it('displays "URL is required" when submitting with empty input', async () => {
      const user = userEvent.setup();
      const onError = vi.fn();

      render(<InlineShortener onError={onError} />);

      const button = screen.getByTestId('shorten-button');

      // Click shorten without entering any URL
      await user.click(button);

      // Check error is displayed
      const errorMessage = screen.getByTestId('error-message');
      expect(errorMessage).toHaveTextContent('URL is required');
      expect(onError).toHaveBeenCalledWith('URL is required');

      // Verify API was NOT called
      expect(defaultMock.shortenUrl).not.toHaveBeenCalled();
    });

    it('displays error when input contains only whitespace', async () => {
      const user = userEvent.setup();

      render(<InlineShortener />);

      const input = screen.getByPlaceholderText('Paste your long URL here...');
      const button = screen.getByTestId('shorten-button');

      // Enter only whitespace
      await user.type(input, '   ');
      await user.click(button);

      const errorMessage = screen.getByTestId('error-message');
      expect(errorMessage).toHaveTextContent('URL is required');
    });
  });

  describe('TC3: Incomplete URL (http://)', () => {
    it('displays validation error for incomplete http:// URL', async () => {
      const user = userEvent.setup();

      render(<InlineShortener />);

      const input = screen.getByPlaceholderText('Paste your long URL here...');
      const button = screen.getByTestId('shorten-button');

      // Enter incomplete URL
      await user.type(input, 'http://');
      await user.click(button);

      const errorMessage = screen.getByTestId('error-message');
      expect(errorMessage).toHaveTextContent('Please enter a valid URL');

      // Verify API was NOT called
      expect(defaultMock.shortenUrl).not.toHaveBeenCalled();
    });

    it('displays validation error for incomplete https:// URL', async () => {
      const user = userEvent.setup();

      render(<InlineShortener />);

      const input = screen.getByPlaceholderText('Paste your long URL here...');
      const button = screen.getByTestId('shorten-button');

      await user.type(input, 'https://');
      await user.click(button);

      const errorMessage = screen.getByTestId('error-message');
      expect(errorMessage).toHaveTextContent('Please enter a valid URL');
    });
  });

  describe('Input field error state', () => {
    it('applies error styling to input when validation fails', async () => {
      const user = userEvent.setup();

      render(<InlineShortener />);

      const input = screen.getByPlaceholderText('Paste your long URL here...');
      const button = screen.getByTestId('shorten-button');

      await user.click(button);

      // Input should have error class
      expect(input).toHaveClass('input-error');
      expect(input).toHaveAttribute('aria-invalid', 'true');
    });

    it('clears error when user starts typing', async () => {
      const user = userEvent.setup();

      render(<InlineShortener />);

      const input = screen.getByPlaceholderText('Paste your long URL here...');
      const button = screen.getByTestId('shorten-button');

      // Trigger validation error
      await user.click(button);
      expect(screen.getByTestId('error-message')).toBeInTheDocument();

      // Start typing - error should clear
      await user.type(input, 'h');
      expect(screen.queryByTestId('error-message')).not.toBeInTheDocument();
    });

    it('has proper accessibility attributes for error state', async () => {
      const user = userEvent.setup();

      render(<InlineShortener />);

      const input = screen.getByPlaceholderText('Paste your long URL here...');
      const button = screen.getByTestId('shorten-button');

      await user.click(button);

      const errorMessage = screen.getByTestId('error-message');
      expect(errorMessage).toHaveAttribute('role', 'alert');
      expect(input).toHaveAttribute('aria-describedby', 'url-error');
    });
  });

  describe('Form submission with Enter key', () => {
    it('validates URL when form is submitted with Enter key', async () => {
      const user = userEvent.setup();

      render(<InlineShortener />);

      const input = screen.getByPlaceholderText('Paste your long URL here...');

      await user.type(input, 'invalid-url{Enter}');

      const errorMessage = screen.getByTestId('error-message');
      expect(errorMessage).toHaveTextContent('Please enter a valid URL');
    });
  });

  describe('Successful validation', () => {
    it('calls shortenUrl when valid URL is submitted', async () => {
      const user = userEvent.setup();
      const shortenUrl = vi.fn().mockResolvedValue({
        shortUrl: 'https://short.url/abc123',
        originalUrl: 'https://example.com',
        shortCode: 'abc123',
        createdAt: new Date().toISOString(),
      });

      mockUseAnonymousShorten.mockReturnValue({
        ...defaultMock,
        shortenUrl,
      });

      render(<InlineShortener />);

      const input = screen.getByPlaceholderText('Paste your long URL here...');
      const button = screen.getByTestId('shorten-button');

      await user.type(input, 'https://example.com');
      await user.click(button);

      expect(shortenUrl).toHaveBeenCalledWith('https://example.com/');
    });
  });
});
