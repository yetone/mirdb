/**
 * URL Validation Utilities Unit Tests.
 * Owner: Scenario 16 - URL Input Validation
 *
 * Tests cover:
 * - Valid URL formats (https, http, with paths, query params)
 * - URLs without protocol (auto-prefixing)
 * - Invalid URL formats
 * - Empty input handling
 * - Long URL handling
 * - Unicode/internationalized domain names
 */
import { describe, it, expect } from 'vitest';
import {
  isValidUrl,
  normalizeUrl,
  getValidationError,
  validateUrlForSubmission,
  MAX_URL_LENGTH,
} from '@/utils/validation';

describe('URL Validation Utilities', () => {
  describe('isValidUrl', () => {
    // Test Case 1: Valid HTTPS URL
    describe('valid URLs', () => {
      it('accepts https://example.com', () => {
        expect(isValidUrl('https://example.com')).toBe(true);
      });

      it('accepts http://example.com', () => {
        expect(isValidUrl('http://example.com')).toBe(true);
      });

      it('accepts URL with www prefix', () => {
        expect(isValidUrl('https://www.example.com')).toBe(true);
      });

      it('accepts URL with subdomain', () => {
        expect(isValidUrl('https://subdomain.example.com')).toBe(true);
      });

      it('accepts URL with port number', () => {
        expect(isValidUrl('https://example.com:8080')).toBe(true);
      });

      // Test Case 2: URL with query parameters
      it('accepts URL with path', () => {
        expect(isValidUrl('https://example.com/path')).toBe(true);
      });

      it('accepts URL with path and query parameters', () => {
        expect(isValidUrl('http://example.com/path?query=value')).toBe(true);
      });

      it('accepts URL with multiple query parameters', () => {
        expect(isValidUrl('https://example.com/search?q=test&page=1')).toBe(true);
      });

      it('accepts URL with fragment', () => {
        expect(isValidUrl('https://example.com/page#section')).toBe(true);
      });

      it('accepts URL with path, query, and fragment', () => {
        expect(isValidUrl('https://example.com/path?query=value#section')).toBe(true);
      });
    });

    // Test Case 3: URL without protocol
    describe('URLs without protocol', () => {
      it('accepts example.com (no protocol)', () => {
        expect(isValidUrl('example.com')).toBe(true);
      });

      it('accepts www.example.com (no protocol)', () => {
        expect(isValidUrl('www.example.com')).toBe(true);
      });

      it('accepts domain with path (no protocol)', () => {
        expect(isValidUrl('example.com/path')).toBe(true);
      });

      it('accepts domain with query params (no protocol)', () => {
        expect(isValidUrl('example.com?query=value')).toBe(true);
      });
    });

    // Test Case 4: Invalid URLs
    describe('invalid URLs', () => {
      it('rejects "not a url at all"', () => {
        expect(isValidUrl('not a url at all')).toBe(false);
      });

      it('rejects random text', () => {
        expect(isValidUrl('hello world')).toBe(false);
      });

      it('rejects single word', () => {
        expect(isValidUrl('notaurl')).toBe(false);
      });

      it('rejects URL-like text without TLD', () => {
        expect(isValidUrl('example')).toBe(false);
      });

      it('rejects incomplete protocol', () => {
        expect(isValidUrl('http:/example.com')).toBe(false);
      });

      it('rejects malformed URL', () => {
        expect(isValidUrl('://example.com')).toBe(false);
      });

      it('rejects javascript: protocol', () => {
        expect(isValidUrl('javascript:alert(1)')).toBe(false);
      });

      it('rejects file: protocol', () => {
        expect(isValidUrl('file:///etc/passwd')).toBe(false);
      });
    });

    // Test Case 5: Empty input
    describe('empty input', () => {
      it('rejects empty string', () => {
        expect(isValidUrl('')).toBe(false);
      });

      it('rejects whitespace only', () => {
        expect(isValidUrl('   ')).toBe(false);
      });

      it('rejects null-like values', () => {
        expect(isValidUrl(null as unknown as string)).toBe(false);
        expect(isValidUrl(undefined as unknown as string)).toBe(false);
      });
    });

    // Test Case 6: Long URLs
    describe('long URLs', () => {
      it('accepts URL at maximum length', () => {
        const longPath = 'a'.repeat(MAX_URL_LENGTH - 'https://example.com/'.length);
        const longUrl = `https://example.com/${longPath}`;
        expect(longUrl.length).toBe(MAX_URL_LENGTH);
        expect(isValidUrl(longUrl)).toBe(true);
      });

      it('rejects URL exceeding maximum length', () => {
        const longPath = 'a'.repeat(MAX_URL_LENGTH);
        const veryLongUrl = `https://example.com/${longPath}`;
        expect(veryLongUrl.length).toBeGreaterThan(MAX_URL_LENGTH);
        expect(isValidUrl(veryLongUrl)).toBe(false);
      });

      it('handles 2000+ character URLs appropriately', () => {
        const longPath = 'a'.repeat(2000);
        const longUrl = `https://example.com/${longPath}`;
        // Should be rejected if exceeds MAX_URL_LENGTH
        if (longUrl.length > MAX_URL_LENGTH) {
          expect(isValidUrl(longUrl)).toBe(false);
        } else {
          expect(isValidUrl(longUrl)).toBe(true);
        }
      });
    });

    // Test Case 7: Unicode/Internationalized Domain Names
    describe('unicode and IDN URLs', () => {
      it('accepts URL with unicode domain (IDN)', () => {
        expect(isValidUrl('https://例え.jp')).toBe(true);
      });

      it('accepts URL with unicode path', () => {
        expect(isValidUrl('https://example.com/路径')).toBe(true);
      });

      it('accepts URL with unicode query parameter', () => {
        expect(isValidUrl('https://example.com?q=日本語')).toBe(true);
      });

      it('accepts URL with accented characters in domain', () => {
        expect(isValidUrl('https://ñoño.es')).toBe(true);
      });

      it('accepts Arabic domain names', () => {
        expect(isValidUrl('https://مثال.مصر')).toBe(true);
      });

      it('accepts Cyrillic domain names', () => {
        expect(isValidUrl('https://пример.рф')).toBe(true);
      });
    });

    describe('edge cases', () => {
      it('handles URL with trailing spaces', () => {
        expect(isValidUrl('https://example.com   ')).toBe(true);
      });

      it('handles URL with leading spaces', () => {
        expect(isValidUrl('   https://example.com')).toBe(true);
      });

      it('accepts URL with special characters in path', () => {
        expect(isValidUrl('https://example.com/path-with-dashes_and_underscores')).toBe(true);
      });

      it('accepts URL with encoded characters', () => {
        expect(isValidUrl('https://example.com/path%20with%20spaces')).toBe(true);
      });

      it('accepts localhost URLs', () => {
        expect(isValidUrl('http://localhost:3000')).toBe(true);
      });

      it('accepts IP address URLs', () => {
        expect(isValidUrl('http://192.168.1.1')).toBe(true);
      });
    });
  });

  describe('normalizeUrl', () => {
    it('keeps https:// URLs unchanged', () => {
      expect(normalizeUrl('https://example.com')).toBe('https://example.com');
    });

    it('keeps http:// URLs unchanged', () => {
      expect(normalizeUrl('http://example.com')).toBe('http://example.com');
    });

    it('adds https:// to URLs without protocol', () => {
      expect(normalizeUrl('example.com')).toBe('https://example.com');
    });

    it('adds https:// to www URLs without protocol', () => {
      expect(normalizeUrl('www.example.com')).toBe('https://www.example.com');
    });

    it('adds https:// to URLs with path', () => {
      expect(normalizeUrl('example.com/path')).toBe('https://example.com/path');
    });

    it('adds https:// to URLs with query parameters', () => {
      expect(normalizeUrl('example.com?q=test')).toBe('https://example.com?q=test');
    });

    it('returns empty string for empty input', () => {
      expect(normalizeUrl('')).toBe('');
    });

    it('returns empty string for whitespace only', () => {
      expect(normalizeUrl('   ')).toBe('');
    });

    it('returns empty string for null/undefined', () => {
      expect(normalizeUrl(null as unknown as string)).toBe('');
      expect(normalizeUrl(undefined as unknown as string)).toBe('');
    });

    it('trims whitespace from URLs', () => {
      expect(normalizeUrl('  example.com  ')).toBe('https://example.com');
    });

    it('preserves case sensitivity', () => {
      expect(normalizeUrl('Example.COM/Path')).toBe('https://Example.COM/Path');
    });
  });

  describe('getValidationError', () => {
    // Test Case 5: Empty string error message
    it('returns "Please enter a URL" for empty string', () => {
      expect(getValidationError('')).toBe('Please enter a URL');
    });

    it('returns "Please enter a URL" for whitespace only', () => {
      expect(getValidationError('   ')).toBe('Please enter a URL');
    });

    it('returns "Please enter a URL" for null/undefined', () => {
      expect(getValidationError(null as unknown as string)).toBe('Please enter a URL');
      expect(getValidationError(undefined as unknown as string)).toBe('Please enter a URL');
    });

    // Test Case 4: Invalid URL error message
    it('returns "Please enter a valid URL" for invalid URL', () => {
      expect(getValidationError('not a url at all')).toBe('Please enter a valid URL');
    });

    it('returns "Please enter a valid URL" for random text', () => {
      expect(getValidationError('hello world')).toBe('Please enter a valid URL');
    });

    // Test Case 6: Long URL error message
    it('returns length error for URLs exceeding maximum', () => {
      const veryLongUrl = 'a'.repeat(MAX_URL_LENGTH + 1);
      expect(getValidationError(veryLongUrl)).toBe(
        `URL is too long. Maximum length is ${MAX_URL_LENGTH} characters`
      );
    });

    // Test Case 1: Valid URL returns null
    it('returns null for valid https URL', () => {
      expect(getValidationError('https://example.com')).toBeNull();
    });

    // Test Case 2: Valid URL with query params returns null
    it('returns null for valid URL with query parameters', () => {
      expect(getValidationError('http://example.com/path?query=value')).toBeNull();
    });

    // Test Case 3: URL without protocol returns null (can be normalized)
    it('returns null for valid URL without protocol', () => {
      expect(getValidationError('example.com')).toBeNull();
    });

    // Test Case 7: Unicode URL returns null
    it('returns null for valid unicode URL', () => {
      expect(getValidationError('https://例え.jp')).toBeNull();
    });
  });

  describe('validateUrlForSubmission', () => {
    // Test Case 1: Valid URL submission
    it('validates and normalizes https://example.com', () => {
      const result = validateUrlForSubmission('https://example.com');
      expect(result.isValid).toBe(true);
      expect(result.normalizedUrl).toBe('https://example.com');
      expect(result.error).toBeNull();
    });

    // Test Case 2: URL with query parameters
    it('validates and preserves query parameters', () => {
      const result = validateUrlForSubmission('http://example.com/path?query=value');
      expect(result.isValid).toBe(true);
      expect(result.normalizedUrl).toBe('http://example.com/path?query=value');
      expect(result.error).toBeNull();
    });

    // Test Case 3: URL without protocol (auto-prefix)
    it('normalizes URL without protocol to https://', () => {
      const result = validateUrlForSubmission('example.com');
      expect(result.isValid).toBe(true);
      expect(result.normalizedUrl).toBe('https://example.com');
      expect(result.error).toBeNull();
    });

    // Test Case 4: Invalid URL
    it('rejects "not a url at all" with proper error', () => {
      const result = validateUrlForSubmission('not a url at all');
      expect(result.isValid).toBe(false);
      expect(result.normalizedUrl).toBe('');
      expect(result.error).toBe('Please enter a valid URL');
    });

    // Test Case 5: Empty string
    it('rejects empty string with proper error', () => {
      const result = validateUrlForSubmission('');
      expect(result.isValid).toBe(false);
      expect(result.normalizedUrl).toBe('');
      expect(result.error).toBe('Please enter a URL');
    });

    // Test Case 6: Very long URL
    it('handles very long URL appropriately', () => {
      const longPath = 'a'.repeat(MAX_URL_LENGTH + 1);
      const result = validateUrlForSubmission(longPath);
      expect(result.isValid).toBe(false);
      expect(result.normalizedUrl).toBe('');
      expect(result.error).toContain('too long');
    });

    // Test Case 7: Unicode URL
    it('handles URL with unicode characters', () => {
      const result = validateUrlForSubmission('https://例え.jp/パス');
      expect(result.isValid).toBe(true);
      expect(result.normalizedUrl).toBe('https://例え.jp/パス');
      expect(result.error).toBeNull();
    });

    it('trims whitespace before validation', () => {
      const result = validateUrlForSubmission('  example.com  ');
      expect(result.isValid).toBe(true);
      expect(result.normalizedUrl).toBe('https://example.com');
      expect(result.error).toBeNull();
    });
  });

  describe('MAX_URL_LENGTH constant', () => {
    it('is set to a reasonable value', () => {
      expect(MAX_URL_LENGTH).toBeGreaterThan(0);
      expect(MAX_URL_LENGTH).toBeLessThanOrEqual(2083); // IE limit
    });

    it('is exported and accessible', () => {
      expect(typeof MAX_URL_LENGTH).toBe('number');
    });
  });
});
