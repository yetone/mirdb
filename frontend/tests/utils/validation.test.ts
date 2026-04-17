/**
 * URL Validation Utilities Tests
 * Owner: Scenario 2 - Inline URL Shortening Demo
 */

import { describe, it, expect } from 'vitest'
import {
  isValidUrl,
  isSafeUrl,
  formatUrlForDisplay,
  sanitizeUrl,
  validateUrlWithMessage,
} from '../../src/utils/validation'

describe('isValidUrl', () => {
  it('should return true for valid http URLs', () => {
    expect(isValidUrl('http://example.com')).toBe(true)
    expect(isValidUrl('http://www.example.com')).toBe(true)
    expect(isValidUrl('http://example.com/path')).toBe(true)
    expect(isValidUrl('http://example.com:8080')).toBe(true)
  })

  it('should return true for valid https URLs', () => {
    expect(isValidUrl('https://example.com')).toBe(true)
    expect(isValidUrl('https://www.example.com')).toBe(true)
    expect(isValidUrl('https://example.com/path')).toBe(true)
    expect(isValidUrl('https://example.com:443')).toBe(true)
  })

  it('should return false for invalid URLs', () => {
    expect(isValidUrl('not-a-url')).toBe(false)
    expect(isValidUrl('example.com')).toBe(false)
    expect(isValidUrl('www.example.com')).toBe(false)
    expect(isValidUrl('')).toBe(false)
    expect(isValidUrl('   ')).toBe(false)
  })

  it('should return false for non-http/https protocols', () => {
    expect(isValidUrl('ftp://example.com')).toBe(false)
    expect(isValidUrl('javascript:alert(1)')).toBe(false)
    expect(isValidUrl('data:text/html,<h1>test</h1>')).toBe(false)
  })

  it('should return false for null or undefined', () => {
    expect(isValidUrl(null as any)).toBe(false)
    expect(isValidUrl(undefined as any)).toBe(false)
  })

  it('should handle URLs with whitespace by trimming', () => {
    expect(isValidUrl('  https://example.com  ')).toBe(true)
  })
})

describe('isSafeUrl', () => {
  it('should return true for safe URLs', () => {
    expect(isSafeUrl('https://example.com')).toBe(true)
    expect(isSafeUrl('http://example.com')).toBe(true)
    expect(isSafeUrl('example.com')).toBe(true)
  })

  it('should return false for javascript: URLs', () => {
    expect(isSafeUrl('javascript:alert(1)')).toBe(false)
    expect(isSafeUrl('JAVASCRIPT:alert(1)')).toBe(false)
    expect(isSafeUrl('JavaScript:void(0)')).toBe(false)
  })

  it('should return false for data: URLs', () => {
    expect(isSafeUrl('data:text/html,<script>alert(1)</script>')).toBe(false)
    expect(isSafeUrl('DATA:text/plain,test')).toBe(false)
  })

  it('should return false for vbscript: URLs', () => {
    expect(isSafeUrl('vbscript:msgbox("test")')).toBe(false)
  })

  it('should return false for file: URLs', () => {
    expect(isSafeUrl('file:///etc/passwd')).toBe(false)
  })

  it('should return false for empty or null inputs', () => {
    expect(isSafeUrl('')).toBe(false)
    expect(isSafeUrl(null as any)).toBe(false)
    expect(isSafeUrl(undefined as any)).toBe(false)
  })
})

describe('formatUrlForDisplay', () => {
  it('should return URL unchanged if shorter than maxLength', () => {
    expect(formatUrlForDisplay('https://example.com', 50)).toBe('https://example.com')
  })

  it('should truncate URL if longer than maxLength', () => {
    const longUrl = 'https://example.com/very/long/path/that/exceeds/the/maximum/length'
    const result = formatUrlForDisplay(longUrl, 30)
    expect(result).toBe('https://example.com/very/lo...')
    expect(result.length).toBe(30)
  })

  it('should use default maxLength of 50', () => {
    const longUrl = 'https://example.com/very/long/path/that/is/quite/long/and/exceeds/fifty/characters'
    const result = formatUrlForDisplay(longUrl)
    expect(result.length).toBe(50)
    expect(result.endsWith('...')).toBe(true)
  })

  it('should return empty string for null or undefined', () => {
    expect(formatUrlForDisplay(null as any)).toBe('')
    expect(formatUrlForDisplay(undefined as any)).toBe('')
  })

  it('should trim whitespace', () => {
    expect(formatUrlForDisplay('  https://example.com  ')).toBe('https://example.com')
  })
})

describe('sanitizeUrl', () => {
  it('should return URL unchanged if it has http protocol', () => {
    expect(sanitizeUrl('http://example.com')).toBe('http://example.com')
  })

  it('should return URL unchanged if it has https protocol', () => {
    expect(sanitizeUrl('https://example.com')).toBe('https://example.com')
  })

  it('should add https:// to URLs without protocol', () => {
    expect(sanitizeUrl('example.com')).toBe('https://example.com')
    expect(sanitizeUrl('www.example.com')).toBe('https://www.example.com')
    expect(sanitizeUrl('example.com/path')).toBe('https://example.com/path')
  })

  it('should trim whitespace', () => {
    expect(sanitizeUrl('  https://example.com  ')).toBe('https://example.com')
    expect(sanitizeUrl('  example.com  ')).toBe('https://example.com')
  })

  it('should return empty string for null or undefined', () => {
    expect(sanitizeUrl(null as any)).toBe('')
    expect(sanitizeUrl(undefined as any)).toBe('')
  })

  it('should return empty string for empty input', () => {
    expect(sanitizeUrl('')).toBe('')
  })

  it('should handle HTTP protocol case-insensitively', () => {
    expect(sanitizeUrl('HTTP://example.com')).toBe('HTTP://example.com')
    expect(sanitizeUrl('HTTPS://example.com')).toBe('HTTPS://example.com')
  })

  it('should not add https:// to URLs with other protocols', () => {
    expect(sanitizeUrl('ftp://example.com')).toBe('ftp://example.com')
    expect(sanitizeUrl('mailto://example.com')).toBe('mailto://example.com')
  })
})

describe('validateUrlWithMessage', () => {
  it('should return isValid true for valid URLs', () => {
    const result = validateUrlWithMessage('https://example.com')
    expect(result.isValid).toBe(true)
    expect(result.error).toBeUndefined()
  })

  it('should return error for empty URL', () => {
    const result = validateUrlWithMessage('')
    expect(result.isValid).toBe(false)
    expect(result.error).toBe('Please enter a URL')
  })

  it('should return error for whitespace-only URL', () => {
    const result = validateUrlWithMessage('   ')
    expect(result.isValid).toBe(false)
    expect(result.error).toBe('Please enter a URL')
  })

  it('should return error for unsafe URL', () => {
    const result = validateUrlWithMessage('javascript:alert(1)')
    expect(result.isValid).toBe(false)
    expect(result.error).toBe('URL contains an unsafe protocol')
  })

  it('should return error for invalid URL format', () => {
    // Note: sanitizeUrl adds https://, so we test with a URL that has an invalid protocol
    // that isn't caught by isSafeUrl but also isn't http/https
    const result = validateUrlWithMessage('ftp://example.com')
    expect(result.isValid).toBe(false)
    expect(result.error).toBe('Please enter a valid URL')
  })

  it('should auto-add https:// and validate', () => {
    const result = validateUrlWithMessage('example.com')
    expect(result.isValid).toBe(true)
  })
})
