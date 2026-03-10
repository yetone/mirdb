/**
 * URL Validation Unit Tests
 * Owner: Scenario 2 - URL Input Validation and Error Handling
 *
 * Tests for URL validation utility functions.
 */

import { describe, it, expect } from 'vitest';
import {
  validateUrl,
  isValidHttpUrl,
  normalizeUrl,
  isValidUrl,
} from '../../../src/utils/urlValidation';
import { ERROR_MESSAGES } from '../../../src/constants/landingContent';

describe('urlValidation', () => {
  describe('validateUrl', () => {
    // Test Case 6: Returns true for valid HTTPS URL
    it('returns valid=true for valid HTTPS URL', () => {
      const result = validateUrl('https://example.com');
      expect(result.valid).toBe(true);
      expect(result.error).toBeUndefined();
      expect(result.normalizedUrl).toBe('https://example.com');
    });

    // Test Case 7: Returns false for invalid URL
    it('returns valid=false for invalid URL', () => {
      const result = validateUrl('invalid');
      expect(result.valid).toBe(false);
      expect(result.error).toBe(ERROR_MESSAGES.invalidUrl);
    });

    // Test Case 1: Empty input
    it('returns error for empty input', () => {
      const result = validateUrl('');
      expect(result.valid).toBe(false);
      expect(result.error).toBe(ERROR_MESSAGES.emptyUrl);
    });

    it('returns error for whitespace-only input', () => {
      const result = validateUrl('   ');
      expect(result.valid).toBe(false);
      expect(result.error).toBe(ERROR_MESSAGES.emptyUrl);
    });

    // Test Case 2: Invalid URL formats
    it('returns error for malformed URL "not-a-valid-url"', () => {
      const result = validateUrl('not-a-valid-url');
      expect(result.valid).toBe(false);
      expect(result.error).toBe(ERROR_MESSAGES.invalidUrl);
    });

    // Test Case 3: URL without protocol - normalizes with https://
    it('normalizes URL without protocol by adding https://', () => {
      const result = validateUrl('example.com/path');
      expect(result.valid).toBe(true);
      expect(result.normalizedUrl).toBe('https://example.com/path');
    });

    it('returns valid=true for HTTP URL', () => {
      const result = validateUrl('http://example.com');
      expect(result.valid).toBe(true);
      expect(result.normalizedUrl).toBe('http://example.com');
    });

    it('returns valid=true for URL with port', () => {
      const result = validateUrl('https://example.com:8080/path');
      expect(result.valid).toBe(true);
      expect(result.normalizedUrl).toBe('https://example.com:8080/path');
    });

    it('returns valid=true for URL with query parameters', () => {
      const result = validateUrl('https://example.com/path?query=value&other=123');
      expect(result.valid).toBe(true);
    });

    it('returns valid=true for URL with hash', () => {
      const result = validateUrl('https://example.com/path#section');
      expect(result.valid).toBe(true);
    });

    it('rejects FTP protocol', () => {
      const result = validateUrl('ftp://example.com');
      expect(result.valid).toBe(false);
      expect(result.error).toBe(ERROR_MESSAGES.invalidUrl);
    });

    it('rejects file protocol', () => {
      const result = validateUrl('file:///path/to/file');
      expect(result.valid).toBe(false);
      expect(result.error).toBe(ERROR_MESSAGES.invalidUrl);
    });

    it('rejects javascript protocol', () => {
      const result = validateUrl('javascript:alert(1)');
      expect(result.valid).toBe(false);
      expect(result.error).toBe(ERROR_MESSAGES.invalidUrl);
    });

    it('handles URL with special characters', () => {
      const result = validateUrl('https://example.com/path/to/resource?name=John%20Doe');
      expect(result.valid).toBe(true);
    });

    it('handles URL with international domain', () => {
      const result = validateUrl('https://例え.jp');
      expect(result.valid).toBe(true);
    });

    it('trims whitespace from URL', () => {
      const result = validateUrl('  https://example.com  ');
      expect(result.valid).toBe(true);
      expect(result.normalizedUrl).toBe('https://example.com');
    });
  });

  describe('isValidHttpUrl', () => {
    it('returns true for valid HTTPS URL', () => {
      expect(isValidHttpUrl('https://example.com')).toBe(true);
    });

    it('returns true for valid HTTP URL', () => {
      expect(isValidHttpUrl('http://example.com')).toBe(true);
    });

    it('returns false for FTP URL', () => {
      expect(isValidHttpUrl('ftp://example.com')).toBe(false);
    });

    it('returns false for invalid URL', () => {
      expect(isValidHttpUrl('not-a-url')).toBe(false);
    });

    it('returns false for empty string', () => {
      expect(isValidHttpUrl('')).toBe(false);
    });

    it('returns false for null/undefined', () => {
      expect(isValidHttpUrl(null as unknown as string)).toBe(false);
      expect(isValidHttpUrl(undefined as unknown as string)).toBe(false);
    });

    it('returns true for URL with path', () => {
      expect(isValidHttpUrl('https://example.com/path/to/page')).toBe(true);
    });

    it('returns true for URL with query string', () => {
      expect(isValidHttpUrl('https://example.com?q=test')).toBe(true);
    });
  });

  describe('normalizeUrl', () => {
    it('adds https:// to URL without protocol', () => {
      expect(normalizeUrl('example.com')).toBe('https://example.com');
    });

    it('adds https:// to URL with path but no protocol', () => {
      expect(normalizeUrl('example.com/path')).toBe('https://example.com/path');
    });

    it('does not modify URL with https://', () => {
      expect(normalizeUrl('https://example.com')).toBe('https://example.com');
    });

    it('does not modify URL with http://', () => {
      expect(normalizeUrl('http://example.com')).toBe('http://example.com');
    });

    it('does not modify URL with other protocols', () => {
      expect(normalizeUrl('ftp://example.com')).toBe('ftp://example.com');
    });

    it('trims whitespace', () => {
      expect(normalizeUrl('  example.com  ')).toBe('https://example.com');
    });

    it('handles empty string', () => {
      expect(normalizeUrl('')).toBe('');
    });

    it('handles null/undefined', () => {
      expect(normalizeUrl(null as unknown as string)).toBe(null);
      expect(normalizeUrl(undefined as unknown as string)).toBe(undefined);
    });

    it('handles URL with port', () => {
      expect(normalizeUrl('example.com:8080')).toBe('https://example.com:8080');
    });

    it('handles URL with query params', () => {
      expect(normalizeUrl('example.com?q=test')).toBe('https://example.com?q=test');
    });
  });

  describe('isValidUrl', () => {
    it('returns true for valid URL', () => {
      expect(isValidUrl('https://example.com')).toBe(true);
    });

    it('returns false for invalid URL', () => {
      expect(isValidUrl('invalid')).toBe(false);
    });

    it('returns true for URL without protocol (normalizes)', () => {
      expect(isValidUrl('example.com')).toBe(true);
    });

    it('returns false for empty string', () => {
      expect(isValidUrl('')).toBe(false);
    });
  });
});
