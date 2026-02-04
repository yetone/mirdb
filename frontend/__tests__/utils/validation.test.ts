/**
 * Unit tests for URL validation utilities.
 * Owner: Scenario 3 - URL Input Validation
 *
 * Tests cover:
 * - Empty/missing URL validation
 * - URL without protocol detection
 * - Valid URL formats (http, https)
 * - Invalid URL formats
 * - Localhost and port handling
 * - Helper function behavior
 */

import { describe, it, expect } from 'vitest';
import {
  validateUrl,
  isValidUrlFormat,
  getUrlValidationError,
} from '../../src/utils/validation';

describe('validateUrl', () => {
  describe('Test Case 1: Invalid URL format ("not-a-url")', () => {
    it('should return isValid: false with error message for non-URL string', () => {
      const result = validateUrl('not-a-url');

      expect(result.isValid).toBe(false);
      expect(result.errorMessage).toBe('Please enter a valid URL');
    });
  });

  describe('Test Case 2: URL without protocol ("example.com")', () => {
    it('should return isValid: false with protocol error message', () => {
      const result = validateUrl('example.com');

      expect(result.isValid).toBe(false);
      expect(result.errorMessage).toBe(
        'URL must include protocol (http:// or https://)'
      );
    });

    it('should detect URL pattern with subdomains without protocol', () => {
      const result = validateUrl('www.example.com');

      expect(result.isValid).toBe(false);
      expect(result.errorMessage).toBe(
        'URL must include protocol (http:// or https://)'
      );
    });

    it('should detect URL pattern with path without protocol', () => {
      const result = validateUrl('example.com/path');

      expect(result.isValid).toBe(false);
      expect(result.errorMessage).toBe(
        'URL must include protocol (http:// or https://)'
      );
    });
  });

  describe('Test Case 3: Valid HTTPS URL ("https://example.com")', () => {
    it('should return isValid: true for valid https URL', () => {
      const result = validateUrl('https://example.com');

      expect(result.isValid).toBe(true);
      expect(result.errorMessage).toBeUndefined();
    });

    it('should accept URL with path', () => {
      const result = validateUrl('https://example.com/path/to/page');

      expect(result.isValid).toBe(true);
    });

    it('should accept URL with query parameters', () => {
      const result = validateUrl('https://example.com?foo=bar&baz=qux');

      expect(result.isValid).toBe(true);
    });

    it('should accept URL with hash fragment', () => {
      const result = validateUrl('https://example.com/page#section');

      expect(result.isValid).toBe(true);
    });
  });

  describe('Test Case 4: Valid localhost URL ("http://localhost:3000/path")', () => {
    it('should return isValid: true for localhost with port and path', () => {
      const result = validateUrl('http://localhost:3000/path');

      expect(result.isValid).toBe(true);
      expect(result.errorMessage).toBeUndefined();
    });

    it('should accept localhost without port', () => {
      const result = validateUrl('http://localhost');

      expect(result.isValid).toBe(true);
    });

    it('should accept localhost with different ports', () => {
      expect(validateUrl('http://localhost:8080').isValid).toBe(true);
      expect(validateUrl('http://localhost:80').isValid).toBe(true);
      expect(validateUrl('https://localhost:443').isValid).toBe(true);
    });

    it('should accept IP addresses', () => {
      expect(validateUrl('http://127.0.0.1:3000').isValid).toBe(true);
      expect(validateUrl('http://192.168.1.1').isValid).toBe(true);
    });
  });

  describe('Test Case 5: Empty URL', () => {
    it('should return isValid: false with enter URL message for empty string', () => {
      const result = validateUrl('');

      expect(result.isValid).toBe(false);
      expect(result.errorMessage).toBe('Please enter a URL');
    });

    it('should treat whitespace-only input as empty', () => {
      const result = validateUrl('   ');

      expect(result.isValid).toBe(false);
      expect(result.errorMessage).toBe('Please enter a URL');
    });

    it('should trim whitespace from valid URLs', () => {
      const result = validateUrl('  https://example.com  ');

      expect(result.isValid).toBe(true);
    });
  });

  describe('Additional edge cases', () => {
    it('should reject ftp:// protocol', () => {
      const result = validateUrl('ftp://files.example.com');

      expect(result.isValid).toBe(false);
      expect(result.errorMessage).toBe('Please enter a valid URL');
    });

    it('should reject mailto: protocol', () => {
      const result = validateUrl('mailto:test@example.com');

      expect(result.isValid).toBe(false);
      expect(result.errorMessage).toBe('Please enter a valid URL');
    });

    it('should reject javascript: protocol', () => {
      const result = validateUrl('javascript:alert(1)');

      expect(result.isValid).toBe(false);
      expect(result.errorMessage).toBe('Please enter a valid URL');
    });

    it('should accept http:// protocol', () => {
      const result = validateUrl('http://example.com');

      expect(result.isValid).toBe(true);
    });

    it('should accept complex URLs with authentication', () => {
      const result = validateUrl('https://user:pass@example.com:8080/path');

      expect(result.isValid).toBe(true);
    });

    it('should accept international domain names', () => {
      const result = validateUrl('https://münchen.example.com');

      expect(result.isValid).toBe(true);
    });
  });
});

describe('isValidUrlFormat', () => {
  it('should return true for valid URLs', () => {
    expect(isValidUrlFormat('https://example.com')).toBe(true);
    expect(isValidUrlFormat('http://localhost:3000')).toBe(true);
  });

  it('should return false for invalid URLs', () => {
    expect(isValidUrlFormat('')).toBe(false);
    expect(isValidUrlFormat('not-a-url')).toBe(false);
    expect(isValidUrlFormat('example.com')).toBe(false);
  });
});

describe('getUrlValidationError', () => {
  it('should return null for valid URLs', () => {
    expect(getUrlValidationError('https://example.com')).toBeNull();
    expect(getUrlValidationError('http://localhost:3000')).toBeNull();
  });

  it('should return error message for empty URL', () => {
    expect(getUrlValidationError('')).toBe('Please enter a URL');
  });

  it('should return error message for URL without protocol', () => {
    expect(getUrlValidationError('example.com')).toBe(
      'URL must include protocol (http:// or https://)'
    );
  });

  it('should return error message for invalid URL', () => {
    expect(getUrlValidationError('not-a-url')).toBe('Please enter a valid URL');
  });
});
