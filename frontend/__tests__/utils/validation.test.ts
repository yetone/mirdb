/**
 * URL Validation Utilities Tests
 * Owner: Scenario 6 - Guest URL Shortening Error Handling
 *
 * Test cases:
 * 1. Enter empty string and click Shorten - Validation error message is displayed, API call is not made
 * 2. Enter 'not-a-valid-url' and click Shorten - Invalid URL format error message is displayed
 * 3. API returns 500 error - Generic error message is displayed with retry option
 * 4. API returns rate limit error (429) - Rate limit error message is displayed
 * 5. Network timeout occurs - Network error message is displayed
 */
import { describe, it, expect } from 'vitest';
import {
  isValidUrl,
  validateUrl,
  sanitizeUrl,
  parseApiError,
  type ValidationResult,
  type ApiError,
} from '../../src/utils/validation';

describe('URL Validation Utilities', () => {
  /**
   * Test Case 1: Empty string validation
   * Input: Enter empty string and click Shorten
   * Expected: Validation error message is displayed, API call is not made
   */
  describe('Test Case 1: Empty string validation', () => {
    it('should return false for empty string with isValidUrl', () => {
      expect(isValidUrl('')).toBe(false);
    });

    it('should return validation error for empty string with validateUrl', () => {
      const result = validateUrl('');
      expect(result.valid).toBe(false);
      expect(result.error).toBe('Please enter a URL');
    });

    it('should return false for whitespace-only string', () => {
      expect(isValidUrl('   ')).toBe(false);
      expect(isValidUrl('\t\n')).toBe(false);
    });

    it('should return validation error for whitespace-only string', () => {
      const result = validateUrl('   ');
      expect(result.valid).toBe(false);
      expect(result.error).toBe('Please enter a URL');
    });

    it('should return false for null/undefined', () => {
      expect(isValidUrl(null as unknown as string)).toBe(false);
      expect(isValidUrl(undefined as unknown as string)).toBe(false);
    });

    it('should return validation error for null/undefined', () => {
      const result1 = validateUrl(null as unknown as string);
      expect(result1.valid).toBe(false);
      expect(result1.error).toBe('Please enter a URL');

      const result2 = validateUrl(undefined as unknown as string);
      expect(result2.valid).toBe(false);
      expect(result2.error).toBe('Please enter a URL');
    });
  });

  /**
   * Test Case 2: Invalid URL format validation
   * Input: Enter 'not-a-valid-url' and click Shorten
   * Expected: Invalid URL format error message is displayed
   */
  describe('Test Case 2: Invalid URL format validation', () => {
    it('should return false for non-URL string with isValidUrl', () => {
      expect(isValidUrl('not-a-valid-url')).toBe(false);
    });

    it('should return validation error for non-URL string with validateUrl', () => {
      const result = validateUrl('not-a-valid-url');
      expect(result.valid).toBe(false);
      expect(result.error).toBe('Please enter a valid URL (e.g., https://example.com)');
    });

    it('should return false for URL without protocol', () => {
      expect(isValidUrl('example.com')).toBe(false);
      expect(isValidUrl('www.example.com')).toBe(false);
    });

    it('should return validation error for URL without protocol', () => {
      const result = validateUrl('example.com');
      expect(result.valid).toBe(false);
      expect(result.error).toBe('Please enter a valid URL (e.g., https://example.com)');
    });

    it('should return false for invalid protocol', () => {
      expect(isValidUrl('ftp://example.com')).toBe(false);
      expect(isValidUrl('javascript:alert(1)')).toBe(false);
      expect(isValidUrl('file:///etc/passwd')).toBe(false);
    });

    it('should return validation error for invalid protocol', () => {
      const result = validateUrl('ftp://example.com');
      expect(result.valid).toBe(false);
      expect(result.error).toBe('URL must start with http:// or https://');
    });

    it('should return false for malformed URLs', () => {
      expect(isValidUrl('http://')).toBe(false);
      expect(isValidUrl('https://')).toBe(false);
      expect(isValidUrl('http:// ')).toBe(false);
    });

    it('should return true for valid HTTP URLs', () => {
      expect(isValidUrl('http://example.com')).toBe(true);
      expect(isValidUrl('https://example.com')).toBe(true);
      expect(isValidUrl('https://www.example.com/path?query=1')).toBe(true);
    });

    it('should return valid result for proper URLs', () => {
      const result = validateUrl('https://example.com');
      expect(result.valid).toBe(true);
      expect(result.error).toBeUndefined();
    });
  });

  /**
   * Test Case 3: API returns 500 error
   * Input: API returns 500 error
   * Expected: Generic error message is displayed with retry option
   */
  describe('Test Case 3: Server error (500) handling', () => {
    it('should return server error for 500 status', () => {
      const axiosError = {
        isAxiosError: true,
        response: {
          status: 500,
          data: {},
        },
      };

      const result = parseApiError(axiosError);
      expect(result.code).toBe('SERVER_ERROR');
      expect(result.message).toBe('Something went wrong on our end. Please try again.');
      expect(result.retryable).toBe(true);
    });

    it('should return server error for 502 status', () => {
      const axiosError = {
        isAxiosError: true,
        response: {
          status: 502,
          data: {},
        },
      };

      const result = parseApiError(axiosError);
      expect(result.code).toBe('SERVER_ERROR');
      expect(result.retryable).toBe(true);
    });

    it('should return server error for 503 status', () => {
      const axiosError = {
        isAxiosError: true,
        response: {
          status: 503,
          data: {},
        },
      };

      const result = parseApiError(axiosError);
      expect(result.code).toBe('SERVER_ERROR');
      expect(result.retryable).toBe(true);
    });
  });

  /**
   * Test Case 4: API returns rate limit error (429)
   * Input: API returns rate limit error (429)
   * Expected: Rate limit error message is displayed
   */
  describe('Test Case 4: Rate limit error (429) handling', () => {
    it('should return rate limit error for 429 status', () => {
      const axiosError = {
        isAxiosError: true,
        response: {
          status: 429,
          data: {},
        },
      };

      const result = parseApiError(axiosError);
      expect(result.code).toBe('RATE_LIMIT');
      expect(result.message).toBe('Too many requests. Please wait a moment and try again.');
      expect(result.retryable).toBe(true);
    });
  });

  /**
   * Test Case 5: Network timeout occurs
   * Input: Network timeout occurs
   * Expected: Network error message is displayed
   */
  describe('Test Case 5: Network timeout handling', () => {
    it('should return timeout error for ECONNABORTED code', () => {
      const axiosError = {
        isAxiosError: true,
        code: 'ECONNABORTED',
        message: 'timeout of 5000ms exceeded',
      };

      const result = parseApiError(axiosError);
      expect(result.code).toBe('TIMEOUT');
      expect(result.message).toBe('Request timed out. Please check your connection and try again.');
      expect(result.retryable).toBe(true);
    });

    it('should return timeout error for ETIMEDOUT code', () => {
      const axiosError = {
        isAxiosError: true,
        code: 'ETIMEDOUT',
        message: 'connection timed out',
      };

      const result = parseApiError(axiosError);
      expect(result.code).toBe('TIMEOUT');
      expect(result.retryable).toBe(true);
    });

    it('should return network error when no response (connection failed)', () => {
      const axiosError = {
        isAxiosError: true,
        code: 'ERR_NETWORK',
        message: 'Network Error',
      };

      const result = parseApiError(axiosError);
      expect(result.code).toBe('NETWORK_ERROR');
      expect(result.message).toBe('Unable to connect. Please check your internet connection and try again.');
      expect(result.retryable).toBe(true);
    });

    it('should return network error for generic Error with network message', () => {
      const error = new Error('Network Error');

      const result = parseApiError(error);
      expect(result.code).toBe('NETWORK_ERROR');
      expect(result.retryable).toBe(true);
    });

    it('should return timeout error for generic Error with timeout message', () => {
      const error = new Error('Request timeout');

      const result = parseApiError(error);
      expect(result.code).toBe('TIMEOUT');
      expect(result.retryable).toBe(true);
    });
  });

  /**
   * Additional edge case tests
   */
  describe('Edge Cases', () => {
    describe('sanitizeUrl', () => {
      it('should trim whitespace', () => {
        expect(sanitizeUrl('  https://example.com  ')).toBe('https://example.com');
      });

      it('should add https:// if no protocol', () => {
        expect(sanitizeUrl('example.com')).toBe('https://example.com');
      });

      it('should preserve existing http protocol', () => {
        expect(sanitizeUrl('http://example.com')).toBe('http://example.com');
      });

      it('should preserve existing https protocol', () => {
        expect(sanitizeUrl('https://example.com')).toBe('https://example.com');
      });

      it('should return empty string for null/undefined', () => {
        expect(sanitizeUrl(null as unknown as string)).toBe('');
        expect(sanitizeUrl(undefined as unknown as string)).toBe('');
      });

      it('should return empty string for empty input', () => {
        expect(sanitizeUrl('')).toBe('');
      });
    });

    describe('parseApiError for validation errors (400)', () => {
      it('should return validation error with detail message', () => {
        const axiosError = {
          isAxiosError: true,
          response: {
            status: 400,
            data: { detail: 'Invalid URL format' },
          },
        };

        const result = parseApiError(axiosError);
        expect(result.code).toBe('VALIDATION_ERROR');
        expect(result.message).toBe('Invalid URL format');
        expect(result.retryable).toBe(false);
      });

      it('should return default validation message if no detail', () => {
        const axiosError = {
          isAxiosError: true,
          response: {
            status: 400,
            data: {},
          },
        };

        const result = parseApiError(axiosError);
        expect(result.code).toBe('VALIDATION_ERROR');
        expect(result.message).toBe('Invalid URL provided. Please check and try again.');
        expect(result.retryable).toBe(false);
      });
    });

    describe('parseApiError for unknown errors', () => {
      it('should return unknown error for unrecognized error object', () => {
        const result = parseApiError({ random: 'object' });
        expect(result.code).toBe('UNKNOWN');
        expect(result.message).toBe('An unexpected error occurred. Please try again.');
        expect(result.retryable).toBe(true);
      });

      it('should return unknown error for string error', () => {
        const result = parseApiError('Some error string');
        expect(result.code).toBe('UNKNOWN');
        expect(result.retryable).toBe(true);
      });

      it('should return unknown error for null', () => {
        const result = parseApiError(null);
        expect(result.code).toBe('UNKNOWN');
        expect(result.retryable).toBe(true);
      });
    });
  });
});
