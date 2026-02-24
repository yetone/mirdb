/**
 * URL Validation Tests
 * Owner: Scenario 4 - URL Validation
 *
 * Tests for URL validation utilities
 */

import { describe, it, expect } from 'vitest'
import { isValidUrl, validateUrl, URL_REGEX } from '@/utils/validation'

describe('URL Validation Utilities', () => {
  describe('validateUrl', () => {
    // Test Case 1: Invalid URL format
    describe('invalid URL format', () => {
      it('should reject "not-a-valid-url" with error message', () => {
        const result = validateUrl('not-a-valid-url')
        expect(result.valid).toBe(false)
        expect(result.error).toBe('Please enter a valid URL')
      })

      it('should reject URL without protocol', () => {
        const result = validateUrl('example.com')
        expect(result.valid).toBe(false)
        expect(result.error).toBe('Please enter a valid URL')
      })

      it('should reject malformed URLs', () => {
        const result = validateUrl('http://')
        expect(result.valid).toBe(false)
        expect(result.error).toBe('Please enter a valid URL')
      })
    })

    // Test Case 2: Empty string
    describe('empty input', () => {
      it('should reject empty string', () => {
        const result = validateUrl('')
        expect(result.valid).toBe(false)
        expect(result.error).toBe('Please enter a URL')
      })

      it('should reject whitespace-only string', () => {
        const result = validateUrl('   ')
        expect(result.valid).toBe(false)
        expect(result.error).toBe('Please enter a URL')
      })
    })

    // Test Case 3: Security validation - javascript: protocol
    describe('security validation', () => {
      it('should reject javascript: protocol', () => {
        const result = validateUrl('javascript:alert(1)')
        expect(result.valid).toBe(false)
        expect(result.error).toBe('Please enter a valid URL')
      })

      it('should reject JavaScript: with mixed case', () => {
        const result = validateUrl('JavaScript:alert(1)')
        expect(result.valid).toBe(false)
        expect(result.error).toBe('Please enter a valid URL')
      })

      it('should reject data: protocol', () => {
        const result = validateUrl('data:text/html,<script>alert(1)</script>')
        expect(result.valid).toBe(false)
        expect(result.error).toBe('Please enter a valid URL')
      })

      it('should reject vbscript: protocol', () => {
        const result = validateUrl('vbscript:msgbox("XSS")')
        expect(result.valid).toBe(false)
        expect(result.error).toBe('Please enter a valid URL')
      })

      it('should reject file: protocol', () => {
        const result = validateUrl('file:///etc/passwd')
        expect(result.valid).toBe(false)
        expect(result.error).toBe('Please enter a valid URL')
      })
    })

    // Test Case 4: Valid URL after previous error
    describe('valid URL acceptance', () => {
      it('should accept valid https:// URL', () => {
        const result = validateUrl('https://example.com')
        expect(result.valid).toBe(true)
        expect(result.error).toBeUndefined()
      })

      it('should accept valid http:// URL', () => {
        const result = validateUrl('http://example.com')
        expect(result.valid).toBe(true)
        expect(result.error).toBeUndefined()
      })

      it('should accept URL with path', () => {
        const result = validateUrl('https://example.com/path/to/page')
        expect(result.valid).toBe(true)
      })

      it('should accept URL with query parameters', () => {
        const result = validateUrl('https://example.com?foo=bar&baz=qux')
        expect(result.valid).toBe(true)
      })

      it('should accept URL with fragment', () => {
        const result = validateUrl('https://example.com#section')
        expect(result.valid).toBe(true)
      })

      it('should accept URL with port', () => {
        const result = validateUrl('https://example.com:8080/page')
        expect(result.valid).toBe(true)
      })
    })

    // Test Case 5: Protocol validation
    describe('protocol validation', () => {
      it('should accept http:// URLs', () => {
        expect(validateUrl('http://example.com').valid).toBe(true)
      })

      it('should accept https:// URLs', () => {
        expect(validateUrl('https://example.com').valid).toBe(true)
      })

      it('should reject ftp:// URLs', () => {
        const result = validateUrl('ftp://files.example.com/file.zip')
        expect(result.valid).toBe(false)
        expect(result.error).toBe('Please enter a valid URL')
      })

      it('should reject mailto: URLs', () => {
        const result = validateUrl('mailto:test@example.com')
        expect(result.valid).toBe(false)
        expect(result.error).toBe('Please enter a valid URL')
      })

      it('should reject tel: URLs', () => {
        const result = validateUrl('tel:+1234567890')
        expect(result.valid).toBe(false)
        expect(result.error).toBe('Please enter a valid URL')
      })
    })
  })

  describe('isValidUrl', () => {
    it('should return true for valid URLs', () => {
      expect(isValidUrl('https://example.com')).toBe(true)
      expect(isValidUrl('http://example.org/page')).toBe(true)
    })

    it('should return false for invalid URLs', () => {
      expect(isValidUrl('')).toBe(false)
      expect(isValidUrl('not-a-url')).toBe(false)
      expect(isValidUrl('javascript:alert(1)')).toBe(false)
    })
  })

  describe('URL_REGEX', () => {
    it('should match valid http URLs', () => {
      expect(URL_REGEX.test('http://example.com')).toBe(true)
    })

    it('should match valid https URLs', () => {
      expect(URL_REGEX.test('https://example.com')).toBe(true)
    })

    it('should not match URLs without protocol', () => {
      expect(URL_REGEX.test('example.com')).toBe(false)
    })

    it('should not match empty strings', () => {
      expect(URL_REGEX.test('')).toBe(false)
    })
  })
})
