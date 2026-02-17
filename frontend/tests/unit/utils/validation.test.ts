/**
 * Unit tests for URL validation utilities.
 * Owner: Scenario 5 - Interactive URL Shortening Demo
 *
 * Test cases:
 * - Valid URL format validation
 * - Scheme whitelist (http, https only)
 * - Malicious scheme detection (javascript:, data:, etc.)
 * - URL sanitization
 */

import { describe, it, expect } from 'vitest'
import {
  isValidUrl,
  validateUrlScheme,
  hasMaliciousScheme,
  sanitizeUrl,
  getUrlValidationError,
} from '@/utils/validation'

describe('validation utilities', () => {
  describe('isValidUrl', () => {
    it('returns true for valid http URL', () => {
      expect(isValidUrl('http://example.com')).toBe(true)
    })

    it('returns true for valid https URL', () => {
      expect(isValidUrl('https://example.com')).toBe(true)
    })

    it('returns true for URL with path', () => {
      expect(isValidUrl('https://example.com/very/long/path/to/resource')).toBe(true)
    })

    it('returns true for URL with query parameters', () => {
      expect(isValidUrl('https://example.com?foo=bar&baz=qux')).toBe(true)
    })

    it('returns true for URL with fragment', () => {
      expect(isValidUrl('https://example.com#section')).toBe(true)
    })

    it('returns true for URL with port', () => {
      expect(isValidUrl('https://example.com:8080/path')).toBe(true)
    })

    it('returns false for invalid URL format', () => {
      expect(isValidUrl('not-a-url')).toBe(false)
    })

    it('returns false for URL without scheme', () => {
      expect(isValidUrl('example.com')).toBe(false)
    })

    it('returns false for empty string', () => {
      expect(isValidUrl('')).toBe(false)
    })

    it('returns false for null/undefined', () => {
      expect(isValidUrl(null as any)).toBe(false)
      expect(isValidUrl(undefined as any)).toBe(false)
    })

    it('returns false for javascript: scheme', () => {
      expect(isValidUrl('javascript:alert(1)')).toBe(false)
    })

    it('returns false for data: scheme', () => {
      expect(isValidUrl('data:text/html,<script>alert(1)</script>')).toBe(false)
    })

    it('returns false for ftp: scheme (not allowed)', () => {
      expect(isValidUrl('ftp://example.com')).toBe(false)
    })

    it('returns false for file: scheme', () => {
      expect(isValidUrl('file:///etc/passwd')).toBe(false)
    })

    it('handles URLs with whitespace by trimming', () => {
      expect(isValidUrl('  https://example.com  ')).toBe(true)
    })
  })

  describe('validateUrlScheme', () => {
    it('returns true for http scheme', () => {
      expect(validateUrlScheme('http://example.com')).toBe(true)
    })

    it('returns true for https scheme', () => {
      expect(validateUrlScheme('https://example.com')).toBe(true)
    })

    it('returns true for HTTP scheme (case insensitive)', () => {
      expect(validateUrlScheme('HTTP://example.com')).toBe(true)
    })

    it('returns true for HTTPS scheme (case insensitive)', () => {
      expect(validateUrlScheme('HTTPS://example.com')).toBe(true)
    })

    it('returns false for ftp scheme', () => {
      expect(validateUrlScheme('ftp://example.com')).toBe(false)
    })

    it('returns false for javascript scheme', () => {
      expect(validateUrlScheme('javascript:void(0)')).toBe(false)
    })

    it('returns false for data scheme', () => {
      expect(validateUrlScheme('data:text/plain,hello')).toBe(false)
    })

    it('returns false for malformed URL', () => {
      expect(validateUrlScheme('not-a-url')).toBe(false)
    })
  })

  describe('hasMaliciousScheme', () => {
    it('returns true for javascript: scheme', () => {
      expect(hasMaliciousScheme('javascript:alert(1)')).toBe(true)
    })

    it('returns true for JAVASCRIPT: scheme (case insensitive)', () => {
      expect(hasMaliciousScheme('JAVASCRIPT:alert(1)')).toBe(true)
    })

    it('returns true for data: scheme', () => {
      expect(hasMaliciousScheme('data:text/html,<script>alert(1)</script>')).toBe(true)
    })

    it('returns true for vbscript: scheme', () => {
      expect(hasMaliciousScheme('vbscript:msgbox("hello")')).toBe(true)
    })

    it('returns true for file: scheme', () => {
      expect(hasMaliciousScheme('file:///etc/passwd')).toBe(true)
    })

    it('returns false for http: scheme', () => {
      expect(hasMaliciousScheme('http://example.com')).toBe(false)
    })

    it('returns false for https: scheme', () => {
      expect(hasMaliciousScheme('https://example.com')).toBe(false)
    })

    it('returns false for regular text', () => {
      expect(hasMaliciousScheme('regular text')).toBe(false)
    })

    it('handles leading whitespace', () => {
      expect(hasMaliciousScheme('  javascript:alert(1)')).toBe(true)
    })
  })

  describe('sanitizeUrl', () => {
    it('trims whitespace from URL', () => {
      expect(sanitizeUrl('  https://example.com  ')).toBe('https://example.com')
    })

    it('returns empty string for null/undefined', () => {
      expect(sanitizeUrl(null as any)).toBe('')
      expect(sanitizeUrl(undefined as any)).toBe('')
    })

    it('returns empty string for empty input', () => {
      expect(sanitizeUrl('')).toBe('')
    })

    it('preserves valid URL characters', () => {
      expect(sanitizeUrl('https://example.com/path?query=value#fragment')).toBe(
        'https://example.com/path?query=value#fragment'
      )
    })
  })

  describe('getUrlValidationError', () => {
    it('returns error for empty string', () => {
      expect(getUrlValidationError('')).toBe('Please enter a URL')
    })

    it('returns error for whitespace only', () => {
      expect(getUrlValidationError('   ')).toBe('Please enter a URL')
    })

    it('returns error for javascript: scheme', () => {
      const error = getUrlValidationError('javascript:alert(1)')
      expect(error).toContain('javascript')
      expect(error).toContain('not allowed')
    })

    it('returns error for data: scheme', () => {
      const error = getUrlValidationError('data:text/html,test')
      expect(error).toContain('data')
      expect(error).toContain('not allowed')
    })

    it('returns error for invalid URL format', () => {
      const error = getUrlValidationError('not-a-url')
      expect(error).toContain('valid URL')
      expect(error).toContain('http')
    })

    it('returns null for valid http URL', () => {
      expect(getUrlValidationError('http://example.com')).toBeNull()
    })

    it('returns null for valid https URL', () => {
      expect(getUrlValidationError('https://example.com/path')).toBeNull()
    })
  })
})
