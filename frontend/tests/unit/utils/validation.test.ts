/**
 * Unit tests for URL validation utilities.
 * Owner: Scenario 6 - URL Input Validation and Error Feedback
 *
 * Tests:
 * - isValidUrl function
 * - getUrlError function
 * - normalizeUrl function
 */

import { describe, it, expect } from 'vitest'
import { isValidUrl, getUrlError, normalizeUrl } from '@/utils/validation'

describe('URL Validation Utilities', () => {
  describe('isValidUrl', () => {
    // Test Case 1: Invalid URL without protocol
    describe('when given invalid URL format', () => {
      it('returns false for "not-a-valid-url"', () => {
        expect(isValidUrl('not-a-valid-url')).toBe(false)
      })

      it('returns false for random text', () => {
        expect(isValidUrl('random text here')).toBe(false)
      })

      it('returns false for URL without protocol', () => {
        expect(isValidUrl('example.com')).toBe(false)
      })
    })

    // Test Case 2: Incomplete URL with protocol only
    describe('when given incomplete URL', () => {
      it('returns false for "http://"', () => {
        expect(isValidUrl('http://')).toBe(false)
      })

      it('returns false for "https://"', () => {
        expect(isValidUrl('https://')).toBe(false)
      })
    })

    // Test Case 4: Empty URL
    describe('when given empty input', () => {
      it('returns false for empty string', () => {
        expect(isValidUrl('')).toBe(false)
      })

      it('returns false for whitespace only', () => {
        expect(isValidUrl('   ')).toBe(false)
      })

      it('returns false for null-like values', () => {
        expect(isValidUrl(undefined as unknown as string)).toBe(false)
      })
    })

    // Test Case 3: Valid URL
    describe('when given valid URL', () => {
      it('returns true for "https://example.com"', () => {
        expect(isValidUrl('https://example.com')).toBe(true)
      })

      it('returns true for "http://example.com"', () => {
        expect(isValidUrl('http://example.com')).toBe(true)
      })

      it('returns true for URL with path', () => {
        expect(isValidUrl('https://example.com/path/to/page')).toBe(true)
      })

      it('returns true for URL with query params', () => {
        expect(isValidUrl('https://example.com?foo=bar')).toBe(true)
      })

      it('returns true for URL with port', () => {
        expect(isValidUrl('http://localhost:3000')).toBe(true)
      })
    })

    describe('when given non-http protocols', () => {
      it('returns false for ftp:// protocol', () => {
        expect(isValidUrl('ftp://files.example.com')).toBe(false)
      })

      it('returns false for javascript: protocol', () => {
        expect(isValidUrl('javascript:alert(1)')).toBe(false)
      })

      it('returns false for file:// protocol', () => {
        expect(isValidUrl('file:///etc/passwd')).toBe(false)
      })
    })
  })

  describe('getUrlError', () => {
    // Test Case 1: Invalid URL error message
    describe('when given invalid URL format', () => {
      it('returns error message for "not-a-valid-url"', () => {
        const error = getUrlError('not-a-valid-url')
        expect(error).not.toBeNull()
        expect(error).toContain('URL')
      })

      it('returns error about protocol for URL without protocol', () => {
        const error = getUrlError('example.com')
        expect(error).toBe('URL must start with http:// or https://')
      })
    })

    // Test Case 2: Incomplete URL error message
    describe('when given incomplete URL', () => {
      it('returns error message for "http://"', () => {
        const error = getUrlError('http://')
        expect(error).toBe('Please enter a complete URL')
      })

      it('returns error message for "https://"', () => {
        const error = getUrlError('https://')
        expect(error).toBe('Please enter a complete URL')
      })
    })

    // Test Case 4: Empty URL error message
    describe('when given empty input', () => {
      it('returns error message for empty string', () => {
        const error = getUrlError('')
        expect(error).toBe('Please enter a URL')
      })

      it('returns error message for whitespace only', () => {
        const error = getUrlError('   ')
        expect(error).toBe('Please enter a URL')
      })
    })

    // Test Case 3: Valid URL returns null (error clears)
    describe('when given valid URL', () => {
      it('returns null for "https://example.com"', () => {
        expect(getUrlError('https://example.com')).toBeNull()
      })

      it('returns null for "http://example.com"', () => {
        expect(getUrlError('http://example.com')).toBeNull()
      })

      it('returns null for URL with path', () => {
        expect(getUrlError('https://example.com/some/path')).toBeNull()
      })
    })

    describe('error message transitions (simulates error clearing)', () => {
      it('transitions from error to null when URL becomes valid', () => {
        // Simulate: user types invalid URL
        const errorBeforeCorrection = getUrlError('not-a-valid-url')
        expect(errorBeforeCorrection).not.toBeNull()

        // Simulate: user corrects to valid URL
        const errorAfterCorrection = getUrlError('https://example.com')
        expect(errorAfterCorrection).toBeNull()
      })

      it('transitions from error to null when incomplete URL becomes complete', () => {
        // Simulate: user types incomplete URL
        const errorIncomplete = getUrlError('http://')
        expect(errorIncomplete).toBe('Please enter a complete URL')

        // Simulate: user completes the URL
        const errorComplete = getUrlError('http://example.com')
        expect(errorComplete).toBeNull()
      })

      it('transitions from empty error to null when URL is entered', () => {
        // Simulate: user has empty input
        const errorEmpty = getUrlError('')
        expect(errorEmpty).toBe('Please enter a URL')

        // Simulate: user enters valid URL
        const errorFilled = getUrlError('https://example.com')
        expect(errorFilled).toBeNull()
      })
    })
  })

  describe('normalizeUrl', () => {
    describe('when URL has no protocol', () => {
      it('adds https:// to URL without protocol', () => {
        expect(normalizeUrl('example.com')).toBe('https://example.com')
      })

      it('adds https:// to URL with path', () => {
        expect(normalizeUrl('example.com/path')).toBe('https://example.com/path')
      })
    })

    describe('when URL already has protocol', () => {
      it('returns https:// URL unchanged', () => {
        expect(normalizeUrl('https://example.com')).toBe('https://example.com')
      })

      it('returns http:// URL unchanged', () => {
        expect(normalizeUrl('http://example.com')).toBe('http://example.com')
      })
    })

    describe('when given empty input', () => {
      it('returns empty string for empty input', () => {
        expect(normalizeUrl('')).toBe('')
      })

      it('returns empty string for whitespace only', () => {
        expect(normalizeUrl('   ')).toBe('')
      })

      it('returns empty string for undefined', () => {
        expect(normalizeUrl(undefined as unknown as string)).toBe('')
      })
    })

    describe('when URL has whitespace', () => {
      it('trims whitespace and normalizes', () => {
        expect(normalizeUrl('  example.com  ')).toBe('https://example.com')
      })

      it('trims whitespace from URL with protocol', () => {
        expect(normalizeUrl('  https://example.com  ')).toBe('https://example.com')
      })
    })
  })
})
