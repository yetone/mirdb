/**
 * Unit tests for URL validation utilities.
 */

import { describe, it, expect } from 'vitest';
import { isValidHttpUrl, getUrlValidationError } from '../../../src/utils/validation';
import { URL_MAX_LENGTH } from '../../../src/utils/constants';

describe('isValidHttpUrl', () => {
  describe('valid URLs', () => {
    it('returns true for a simple http URL', () => {
      expect(isValidHttpUrl('http://example.com')).toBe(true);
    });

    it('returns true for a simple https URL', () => {
      expect(isValidHttpUrl('https://example.com')).toBe(true);
    });

    it('returns true for a URL with a path', () => {
      expect(isValidHttpUrl('https://example.com/some/path')).toBe(true);
    });

    it('returns true for a URL with query parameters', () => {
      expect(isValidHttpUrl('https://example.com?foo=bar&baz=qux')).toBe(true);
    });

    it('returns true for a URL with a port', () => {
      expect(isValidHttpUrl('https://example.com:8080/path')).toBe(true);
    });

    it('returns true for a URL with a fragment', () => {
      expect(isValidHttpUrl('https://example.com#section')).toBe(true);
    });

    it('returns true for a URL with a subdomain', () => {
      expect(isValidHttpUrl('https://sub.example.com')).toBe(true);
    });

    it('returns true for a URL with user info', () => {
      expect(isValidHttpUrl('https://user:pass@example.com')).toBe(true);
    });
  });

  describe('invalid URLs', () => {
    it('returns false for an empty string', () => {
      expect(isValidHttpUrl('')).toBe(false);
    });

    it('returns false for whitespace only', () => {
      expect(isValidHttpUrl('   ')).toBe(false);
    });

    it('returns false for a non-string value (coerced)', () => {
      expect(isValidHttpUrl(null as unknown as string)).toBe(false);
      expect(isValidHttpUrl(undefined as unknown as string)).toBe(false);
    });

    it('returns false for a malformed URL', () => {
      expect(isValidHttpUrl('not a url')).toBe(false);
    });

    it('returns false for a URL missing protocol', () => {
      expect(isValidHttpUrl('example.com')).toBe(false);
    });

    it('returns false for ftp scheme', () => {
      expect(isValidHttpUrl('ftp://example.com')).toBe(false);
    });

    it('returns false for mailto scheme', () => {
      expect(isValidHttpUrl('mailto:test@example.com')).toBe(false);
    });
  });

  describe('security - blocked schemes', () => {
    it('returns false for javascript: scheme', () => {
      expect(isValidHttpUrl('javascript:alert("xss")')).toBe(false);
    });

    it('returns false for javascript: scheme with spaces', () => {
      expect(isValidHttpUrl('  javascript:void(0)')).toBe(false);
    });

    it('returns false for mixed-case javascript: scheme', () => {
      expect(isValidHttpUrl('JaVaScRiPt:alert(1)')).toBe(false);
    });

    it('returns false for data: scheme', () => {
      expect(isValidHttpUrl('data:text/html,<script>alert(1)</script>')).toBe(false);
    });

    it('returns false for data: image URI', () => {
      expect(isValidHttpUrl('data:image/png;base64,abc')).toBe(false);
    });

    it('returns false for vbscript: scheme', () => {
      expect(isValidHttpUrl('vbscript:msgbox("test")')).toBe(false);
    });

    it('returns false for file: scheme', () => {
      expect(isValidHttpUrl('file:///etc/passwd')).toBe(false);
    });
  });

  describe('maximum length validation', () => {
    it('returns false for a URL exactly at max length + 1', () => {
      const longUrl = 'https://example.com/' + 'a'.repeat(URL_MAX_LENGTH);
      expect(longUrl.length).toBeGreaterThan(URL_MAX_LENGTH);
      expect(isValidHttpUrl(longUrl)).toBe(false);
    });

    it('returns false for a URL far exceeding max length', () => {
      const longUrl = 'https://example.com/' + 'b'.repeat(URL_MAX_LENGTH + 1000);
      expect(isValidHttpUrl(longUrl)).toBe(false);
    });

    it('returns true for a URL just under max length', () => {
      const base = 'https://example.com/';
      const padding = 'c'.repeat(URL_MAX_LENGTH - base.length - 1);
      const url = base + padding;
      expect(url.length).toBe(URL_MAX_LENGTH - 1);
      expect(isValidHttpUrl(url)).toBe(true);
    });

    it('returns true for a URL exactly at max length', () => {
      const base = 'https://example.com/';
      const padding = 'd'.repeat(URL_MAX_LENGTH - base.length);
      const url = base + padding;
      expect(url.length).toBe(URL_MAX_LENGTH);
      expect(isValidHttpUrl(url)).toBe(true);
    });
  });
});

describe('getUrlValidationError', () => {
  it('returns null for a valid URL', () => {
    expect(getUrlValidationError('https://example.com')).toBeNull();
  });

  it('returns "Please enter a URL" for empty string', () => {
    expect(getUrlValidationError('')).toBe('Please enter a URL');
  });

  it('returns "Please enter a URL" for whitespace', () => {
    expect(getUrlValidationError('   ')).toBe('Please enter a URL');
  });

  it('returns length error for overly long URL', () => {
    const longUrl = 'https://example.com/' + 'x'.repeat(URL_MAX_LENGTH);
    expect(getUrlValidationError(longUrl)).toBe(`URL must be under ${URL_MAX_LENGTH} characters`);
  });

  it('returns security message for javascript: scheme', () => {
    expect(getUrlValidationError('javascript:alert(1)')).toBe('Unsafe URL scheme is not allowed');
  });

  it('returns security message for data: scheme', () => {
    expect(getUrlValidationError('data:text/html,test')).toBe('Unsafe URL scheme is not allowed');
  });

  it('returns "Please enter a valid URL" for malformed URLs', () => {
    expect(getUrlValidationError('not-a-url')).toBe('Please enter a valid URL');
  });

  it('returns "Please enter a valid URL" for ftp scheme', () => {
    expect(getUrlValidationError('ftp://example.com')).toBe('Please enter a valid URL');
  });
});
