/**
 * URL Validation Utilities Tests.
 * Owner: Scenario 16 - URL Input Validation
 *
 * Tests for URL validation functions including:
 * - Valid URL formats (with/without protocol, query params)
 * - Invalid URL formats
 * - Edge cases (long URLs, unicode, special characters)
 * - Error message generation
 */
import { describe, it, expect } from 'vitest';
import {
  isValidUrl,
  normalizeUrl,
  getValidationError,
  validateUrl,
  MAX_URL_LENGTH,
} from '../../../src/utils/validation';

describe('URL Validation Utilities', () => {
  describe('isValidUrl', () => {
    // Test Case 1: Enter 'https://example.com'
    // URL is accepted and form can be submitted
    describe('valid URLs with https protocol', () => {
      it('accepts https://example.com', () => {
        expect(isValidUrl('https://example.com')).toBe(true);
      });

      it('accepts https URLs with www', () => {
        expect(isValidUrl('https://www.example.com')).toBe(true);
      });

      it('accepts https URLs with subdomain', () => {
        expect(isValidUrl('https://subdomain.example.com')).toBe(true);
      });

      it('accepts https URLs with port', () => {
        expect(isValidUrl('https://example.com:8080')).toBe(true);
      });

      it('accepts https URLs with path', () => {
        expect(isValidUrl('https://example.com/path/to/page')).toBe(true);
      });
    });

    // Test Case 2: Enter 'http://example.com/path?query=value'
    // URL with query parameters is accepted
    describe('valid URLs with query parameters', () => {
      it('accepts URL with query parameters', () => {
        expect(isValidUrl('http://example.com/path?query=value')).toBe(true);
      });

      it('accepts URL with multiple query parameters', () => {
        expect(isValidUrl('https://example.com?foo=bar&baz=qux')).toBe(true);
      });

      it('accepts URL with hash fragment', () => {
        expect(isValidUrl('https://example.com/page#section')).toBe(true);
      });

      it('accepts URL with both query params and fragment', () => {
        expect(isValidUrl('https://example.com/path?query=value#section')).toBe(
          true
        );
      });
    });

    // Test Case 3: Enter 'example.com' (no protocol)
    // URL without protocol is either accepted (with auto-prefix) or shows helpful error
    describe('URLs without protocol', () => {
      it('accepts example.com without protocol', () => {
        expect(isValidUrl('example.com')).toBe(true);
      });

      it('accepts www.example.com without protocol', () => {
        expect(isValidUrl('www.example.com')).toBe(true);
      });

      it('accepts domain with path without protocol', () => {
        expect(isValidUrl('example.com/path/to/page')).toBe(true);
      });

      it('accepts domain with query params without protocol', () => {
        expect(isValidUrl('example.com?query=value')).toBe(true);
      });
    });

    // Test Case 4: Enter 'not a url at all'
    // Validation fails with 'Please enter a valid URL' error message
    describe('invalid URLs', () => {
      it('rejects plain text without dots', () => {
        expect(isValidUrl('not a url at all')).toBe(false);
      });

      it('rejects single word', () => {
        expect(isValidUrl('example')).toBe(false);
      });

      it('rejects text with spaces', () => {
        expect(isValidUrl('example .com')).toBe(false);
      });

      it('rejects incomplete URLs', () => {
        expect(isValidUrl('http://')).toBe(false);
      });

      it('rejects URLs with only protocol', () => {
        expect(isValidUrl('https://')).toBe(false);
      });

      it('rejects random strings', () => {
        expect(isValidUrl('asdfasdf')).toBe(false);
      });

      it('rejects strings with special characters only', () => {
        expect(isValidUrl('!@#$%^&*()')).toBe(false);
      });
    });

    // Test Case 5: Enter empty string and submit
    // Validation fails with 'Please enter a URL' error message
    describe('empty inputs', () => {
      it('rejects empty string', () => {
        expect(isValidUrl('')).toBe(false);
      });

      it('rejects whitespace only', () => {
        expect(isValidUrl('   ')).toBe(false);
      });

      it('rejects string with only tabs', () => {
        expect(isValidUrl('\t\t')).toBe(false);
      });

      it('rejects string with only newlines', () => {
        expect(isValidUrl('\n\n')).toBe(false);
      });
    });

    // Test Case 7: Enter URL with unicode characters
    // URL with internationalized domain names is handled correctly
    describe('URLs with unicode characters', () => {
      it('accepts internationalized domain names', () => {
        expect(isValidUrl('https://münchen.example')).toBe(true);
      });

      it('accepts URLs with unicode in domain', () => {
        expect(isValidUrl('https://例え.jp')).toBe(true);
      });

      it('accepts URLs with unicode in path', () => {
        expect(isValidUrl('https://example.com/日本語')).toBe(true);
      });

      it('accepts URLs with punycode domain', () => {
        expect(isValidUrl('https://xn--mnchen-3ya.example')).toBe(true);
      });
    });

    describe('edge cases', () => {
      it('handles URLs with trailing spaces', () => {
        expect(isValidUrl('https://example.com   ')).toBe(true);
      });

      it('handles URLs with leading spaces', () => {
        expect(isValidUrl('   https://example.com')).toBe(true);
      });

      it('accepts various TLDs', () => {
        expect(isValidUrl('https://example.io')).toBe(true);
        expect(isValidUrl('https://example.co.uk')).toBe(true);
        expect(isValidUrl('https://example.museum')).toBe(true);
      });

      it('accepts localhost with port', () => {
        expect(isValidUrl('http://localhost:3000')).toBe(true);
      });
    });
  });

  describe('normalizeUrl', () => {
    it('adds https:// to URL without protocol', () => {
      expect(normalizeUrl('example.com')).toBe('https://example.com');
    });

    it('keeps existing https:// protocol', () => {
      expect(normalizeUrl('https://example.com')).toBe('https://example.com');
    });

    it('keeps existing http:// protocol', () => {
      expect(normalizeUrl('http://example.com')).toBe('http://example.com');
    });

    it('trims whitespace before normalizing', () => {
      expect(normalizeUrl('  example.com  ')).toBe('https://example.com');
    });

    it('handles empty string', () => {
      expect(normalizeUrl('')).toBe('');
    });

    it('handles whitespace only string', () => {
      expect(normalizeUrl('   ')).toBe('');
    });

    it('preserves path and query parameters', () => {
      expect(normalizeUrl('example.com/path?query=value')).toBe(
        'https://example.com/path?query=value'
      );
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

    // Test Case 4: Invalid URL error message
    it('returns "Please enter a valid URL" for invalid URLs', () => {
      expect(getValidationError('not a url at all')).toBe(
        'Please enter a valid URL'
      );
    });

    it('returns "Please enter a valid URL" for single word', () => {
      expect(getValidationError('example')).toBe('Please enter a valid URL');
    });

    // Test Case 6: Very long URL (2000+ characters)
    // URL is handled appropriately (accepted or shows length error)
    describe('long URLs', () => {
      it('returns length error for URLs exceeding max length', () => {
        const longPath = 'a'.repeat(MAX_URL_LENGTH + 100);
        const longUrl = `https://example.com/${longPath}`;
        expect(getValidationError(longUrl)).toBe(
          `URL is too long (max ${MAX_URL_LENGTH} characters)`
        );
      });

      it('accepts URLs at exactly max length', () => {
        const padding = MAX_URL_LENGTH - 'https://example.com/'.length;
        const maxLengthUrl = `https://example.com/${'a'.repeat(padding)}`;
        expect(getValidationError(maxLengthUrl)).toBe(null);
      });

      it('accepts URLs shorter than max length', () => {
        const longUrl = `https://example.com/${'a'.repeat(1000)}`;
        expect(getValidationError(longUrl)).toBe(null);
      });
    });

    // Valid URLs return null
    it('returns null for valid https URL', () => {
      expect(getValidationError('https://example.com')).toBe(null);
    });

    it('returns null for valid URL with query params', () => {
      expect(getValidationError('http://example.com/path?query=value')).toBe(
        null
      );
    });

    it('returns null for URL without protocol', () => {
      expect(getValidationError('example.com')).toBe(null);
    });

    it('returns null for URL with unicode', () => {
      expect(getValidationError('https://münchen.example')).toBe(null);
    });
  });

  describe('validateUrl', () => {
    it('returns valid result with normalized URL for valid input', () => {
      const result = validateUrl('example.com');
      expect(result.isValid).toBe(true);
      expect(result.normalizedUrl).toBe('https://example.com');
      expect(result.error).toBe(null);
    });

    it('returns invalid result with error for empty input', () => {
      const result = validateUrl('');
      expect(result.isValid).toBe(false);
      expect(result.normalizedUrl).toBe(null);
      expect(result.error).toBe('Please enter a URL');
    });

    it('returns invalid result with error for invalid URL', () => {
      const result = validateUrl('not a url at all');
      expect(result.isValid).toBe(false);
      expect(result.normalizedUrl).toBe(null);
      expect(result.error).toBe('Please enter a valid URL');
    });

    it('preserves protocol in normalized URL', () => {
      const result = validateUrl('http://example.com');
      expect(result.isValid).toBe(true);
      expect(result.normalizedUrl).toBe('http://example.com');
      expect(result.error).toBe(null);
    });

    it('handles URL with query params correctly', () => {
      const result = validateUrl('example.com/path?query=value');
      expect(result.isValid).toBe(true);
      expect(result.normalizedUrl).toBe('https://example.com/path?query=value');
      expect(result.error).toBe(null);
    });
  });

  // Summary of all 7 test cases from the scenario
  describe('Scenario Test Cases Summary', () => {
    it('TC1: https://example.com is accepted', () => {
      expect(isValidUrl('https://example.com')).toBe(true);
      expect(getValidationError('https://example.com')).toBe(null);
    });

    it('TC2: URL with query parameters is accepted', () => {
      expect(isValidUrl('http://example.com/path?query=value')).toBe(true);
      expect(getValidationError('http://example.com/path?query=value')).toBe(
        null
      );
    });

    it('TC3: URL without protocol is accepted with auto-prefix', () => {
      expect(isValidUrl('example.com')).toBe(true);
      expect(normalizeUrl('example.com')).toBe('https://example.com');
    });

    it('TC4: Invalid URL fails with correct error message', () => {
      expect(isValidUrl('not a url at all')).toBe(false);
      expect(getValidationError('not a url at all')).toBe(
        'Please enter a valid URL'
      );
    });

    it('TC5: Empty string fails with correct error message', () => {
      expect(isValidUrl('')).toBe(false);
      expect(getValidationError('')).toBe('Please enter a URL');
    });

    it('TC6: Very long URL is handled appropriately', () => {
      // Valid long URL (under limit)
      const validLongUrl = `https://example.com/${'a'.repeat(1000)}`;
      expect(isValidUrl(validLongUrl)).toBe(true);

      // Invalid long URL (over limit)
      const tooLongUrl = `https://example.com/${'a'.repeat(MAX_URL_LENGTH)}`;
      expect(getValidationError(tooLongUrl)).toBe(
        `URL is too long (max ${MAX_URL_LENGTH} characters)`
      );
    });

    it('TC7: URL with unicode characters is handled correctly', () => {
      expect(isValidUrl('https://münchen.example')).toBe(true);
      expect(getValidationError('https://例え.jp')).toBe(null);
    });
  });
});
