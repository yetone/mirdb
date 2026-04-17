/**
 * URL Validation Utilities Tests
 * Owner: Scenario 2 - Inline URL Shortening Demo
 * Enhanced: Scenario 15 - Error States and Edge Cases
 */

import { describe, it, expect } from 'vitest'
import {
  isValidUrl,
  isSafeUrl,
  formatUrlForDisplay,
  sanitizeUrl,
  validateUrlWithMessage,
  hasInvalidCharacters,
  isUrlTooLong,
  encodeUrlPath,
  getApiErrorMessage,
  MAX_URL_LENGTH,
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

  it('should return error for URL with spaces', () => {
    const result = validateUrlWithMessage('https://exa mple.com')
    expect(result.isValid).toBe(false)
    expect(result.error).toBe('URL contains invalid characters')
  })

  it('should return error for URL exceeding max length', () => {
    const longPath = 'a'.repeat(MAX_URL_LENGTH + 1)
    const result = validateUrlWithMessage(`https://example.com/${longPath}`)
    expect(result.isValid).toBe(false)
    expect(result.error).toContain('exceeds maximum length')
  })
})

/**
 * Error States and Edge Cases Tests (Scenario 15)
 */
describe('hasInvalidCharacters', () => {
  it('should return true for URL with unencoded spaces', () => {
    expect(hasInvalidCharacters('https://exa mple.com')).toBe(true)
    expect(hasInvalidCharacters('https://example.com/path with spaces')).toBe(true)
  })

  it('should return false for valid URLs', () => {
    expect(hasInvalidCharacters('https://example.com')).toBe(false)
    expect(hasInvalidCharacters('https://example.com/path%20with%20encoded%20spaces')).toBe(false)
    expect(hasInvalidCharacters('https://example.com/path?query=value')).toBe(false)
  })

  it('should return true for URLs with control characters', () => {
    expect(hasInvalidCharacters('https://example.com\x00')).toBe(true)
    expect(hasInvalidCharacters('https://example.com\x1F')).toBe(true)
  })

  it('should return true for URLs with other invalid characters', () => {
    expect(hasInvalidCharacters('https://example.com/<script>')).toBe(true)
    expect(hasInvalidCharacters('https://example.com/{path}')).toBe(true)
    expect(hasInvalidCharacters('https://example.com/path|other')).toBe(true)
  })

  it('should return true for null or undefined', () => {
    expect(hasInvalidCharacters(null as any)).toBe(true)
    expect(hasInvalidCharacters(undefined as any)).toBe(true)
    expect(hasInvalidCharacters('')).toBe(true)
  })
})

describe('isUrlTooLong', () => {
  it('should return false for URLs within limit', () => {
    expect(isUrlTooLong('https://example.com')).toBe(false)
    expect(isUrlTooLong('https://example.com/' + 'a'.repeat(100))).toBe(false)
  })

  it('should return true for URLs exceeding default limit (2048)', () => {
    const longUrl = 'https://example.com/' + 'a'.repeat(MAX_URL_LENGTH)
    expect(isUrlTooLong(longUrl)).toBe(true)
  })

  it('should respect custom max length parameter', () => {
    expect(isUrlTooLong('https://example.com/longpath', 20)).toBe(true)
    expect(isUrlTooLong('https://example.com', 100)).toBe(false)
  })

  it('should return false for null or undefined', () => {
    expect(isUrlTooLong(null as any)).toBe(false)
    expect(isUrlTooLong(undefined as any)).toBe(false)
  })

  it('should handle exactly at limit', () => {
    const exactUrl = 'a'.repeat(MAX_URL_LENGTH)
    expect(isUrlTooLong(exactUrl)).toBe(false)

    const overLimitUrl = 'a'.repeat(MAX_URL_LENGTH + 1)
    expect(isUrlTooLong(overLimitUrl)).toBe(true)
  })
})

describe('encodeUrlPath', () => {
  it('should properly encode special characters in path', () => {
    const result = encodeUrlPath('https://example.com/path/with spaces')
    expect(result).toBe('https://example.com/path/with%20spaces')
  })

  it('should handle already encoded paths', () => {
    const result = encodeUrlPath('https://example.com/path/with%20spaces')
    expect(result).toBe('https://example.com/path/with%20spaces')
  })

  it('should handle URLs with query parameters', () => {
    const result = encodeUrlPath('https://example.com/path?query=value')
    expect(result).toContain('query=value')
  })

  it('should return empty string for invalid input', () => {
    expect(encodeUrlPath(null as any)).toBe('')
    expect(encodeUrlPath(undefined as any)).toBe('')
    expect(encodeUrlPath('')).toBe('')
  })

  it('should return original URL if parsing fails', () => {
    expect(encodeUrlPath('not-a-valid-url')).toBe('not-a-valid-url')
  })

  it('should handle URLs with unicode characters', () => {
    const result = encodeUrlPath('https://example.com/日本語')
    expect(result).toContain('example.com')
  })
})

describe('getApiErrorMessage', () => {
  it('should return network error message for network failures', () => {
    const error = new Error('Network Error')
    expect(getApiErrorMessage(error)).toBe('Unable to connect. Please check your internet connection and try again.')
  })

  it('should return network error message for fetch failures', () => {
    const error = new Error('Failed to fetch')
    expect(getApiErrorMessage(error)).toBe('Unable to connect. Please check your internet connection and try again.')
  })

  it('should return timeout message for timeout errors', () => {
    const error = new Error('Request timeout')
    expect(getApiErrorMessage(error)).toBe('Request timed out. Please try again.')

    const error2 = new Error('Request timed out')
    expect(getApiErrorMessage(error2)).toBe('Request timed out. Please try again.')
  })

  it('should return rate limit message for rate limiting errors', () => {
    const error = new Error('Rate limit exceeded')
    expect(getApiErrorMessage(error)).toBe('Too many requests. Please wait a moment and try again.')

    const error2 = new Error('Too many requests')
    expect(getApiErrorMessage(error2)).toBe('Too many requests. Please wait a moment and try again.')
  })

  it('should return server error message for 500 errors', () => {
    const error = new Error('500 Internal Server Error')
    expect(getApiErrorMessage(error)).toBe('Something went wrong on our end. Please try again later.')

    const error2 = new Error('Internal server error occurred')
    expect(getApiErrorMessage(error2)).toBe('Something went wrong on our end. Please try again later.')
  })

  it('should return original error message for other errors', () => {
    const error = new Error('Custom error message')
    expect(getApiErrorMessage(error)).toBe('Custom error message')
  })

  it('should return generic message for non-Error exceptions', () => {
    expect(getApiErrorMessage('string error')).toBe('Failed to shorten URL. Please try again.')
    expect(getApiErrorMessage(null)).toBe('Failed to shorten URL. Please try again.')
    expect(getApiErrorMessage(undefined)).toBe('Failed to shorten URL. Please try again.')
    expect(getApiErrorMessage({ message: 'object' })).toBe('Failed to shorten URL. Please try again.')
  })
})

/**
 * Scenario 15 Edge Case Tests - Malicious URL Schemes
 */
describe('Malicious URL validation (Scenario 15 TC2)', () => {
  it('should reject javascript: URLs', () => {
    const result = validateUrlWithMessage('javascript:alert(1)')
    expect(result.isValid).toBe(false)
    expect(result.error).toBe('URL contains an unsafe protocol')
  })

  it('should reject javascript: URLs with different casing', () => {
    expect(validateUrlWithMessage('JavaScript:alert(1)').isValid).toBe(false)
    expect(validateUrlWithMessage('JAVASCRIPT:void(0)').isValid).toBe(false)
  })

  it('should reject data: URLs', () => {
    const result = validateUrlWithMessage('data:text/html,<script>alert(1)</script>')
    expect(result.isValid).toBe(false)
    expect(result.error).toBe('URL contains an unsafe protocol')
  })

  it('should reject vbscript: URLs', () => {
    const result = validateUrlWithMessage('vbscript:msgbox("test")')
    expect(result.isValid).toBe(false)
    expect(result.error).toBe('URL contains an unsafe protocol')
  })

  it('should reject file: URLs', () => {
    const result = validateUrlWithMessage('file:///etc/passwd')
    expect(result.isValid).toBe(false)
    expect(result.error).toBe('URL contains an unsafe protocol')
  })
})

/**
 * Scenario 15 Edge Case Tests - URLs with Spaces (TC3)
 */
describe('URL with spaces validation (Scenario 15 TC3)', () => {
  it('should reject URLs with spaces in domain', () => {
    const result = validateUrlWithMessage('https://exa mple.com')
    expect(result.isValid).toBe(false)
    expect(result.error).toBe('URL contains invalid characters')
  })

  it('should reject URLs with spaces in path', () => {
    const result = validateUrlWithMessage('https://example.com/path with spaces')
    expect(result.isValid).toBe(false)
    expect(result.error).toBe('URL contains invalid characters')
  })

  it('should reject URLs with multiple spaces', () => {
    const result = validateUrlWithMessage('https://example.com/   multiple   spaces')
    expect(result.isValid).toBe(false)
    expect(result.error).toBe('URL contains invalid characters')
  })

  it('should accept URLs with encoded spaces', () => {
    const result = validateUrlWithMessage('https://example.com/path%20with%20spaces')
    expect(result.isValid).toBe(true)
  })
})

/**
 * Scenario 15 Edge Case Tests - Long URLs (TC4)
 */
describe('Long URL handling (Scenario 15 TC4)', () => {
  it('should accept URLs at exactly the limit', () => {
    // Create a URL that's exactly at the limit
    const baseUrl = 'https://example.com/'
    const pathLength = MAX_URL_LENGTH - baseUrl.length
    const url = baseUrl + 'a'.repeat(pathLength)
    expect(url.length).toBe(MAX_URL_LENGTH)
    const result = validateUrlWithMessage(url)
    expect(result.isValid).toBe(true)
  })

  it('should reject URLs exceeding the limit', () => {
    const baseUrl = 'https://example.com/'
    const url = baseUrl + 'a'.repeat(MAX_URL_LENGTH)
    expect(url.length).toBeGreaterThan(MAX_URL_LENGTH)
    const result = validateUrlWithMessage(url)
    expect(result.isValid).toBe(false)
    expect(result.error).toContain('exceeds maximum length')
  })

  it('should handle URLs with 2000+ characters gracefully', () => {
    const longUrl = 'https://example.com/' + 'a'.repeat(2000)
    const result = validateUrlWithMessage(longUrl)
    // The URL is valid as it's under 2048
    expect(result.isValid).toBe(true)
  })

  it('should reject URLs over 2048 characters', () => {
    const veryLongUrl = 'https://example.com/' + 'a'.repeat(2100)
    const result = validateUrlWithMessage(veryLongUrl)
    expect(result.isValid).toBe(false)
  })
})

/**
 * Scenario 15 Edge Case Tests - Special Characters (TC6)
 */
describe('Special characters in URL (Scenario 15 TC6)', () => {
  it('should accept URLs with common special characters in path', () => {
    expect(validateUrlWithMessage('https://example.com/path-with-dashes').isValid).toBe(true)
    expect(validateUrlWithMessage('https://example.com/path_with_underscores').isValid).toBe(true)
    expect(validateUrlWithMessage('https://example.com/path.with.dots').isValid).toBe(true)
  })

  it('should accept URLs with query parameters', () => {
    expect(validateUrlWithMessage('https://example.com/path?key=value').isValid).toBe(true)
    expect(validateUrlWithMessage('https://example.com/path?key1=value1&key2=value2').isValid).toBe(true)
  })

  it('should accept URLs with fragment identifiers', () => {
    expect(validateUrlWithMessage('https://example.com/path#section').isValid).toBe(true)
  })

  it('should accept URLs with encoded special characters', () => {
    expect(validateUrlWithMessage('https://example.com/path%2Fwith%2Fslashes').isValid).toBe(true)
    expect(validateUrlWithMessage('https://example.com/path%3Fquestion').isValid).toBe(true)
  })

  it('should accept URLs with unicode in path (encoded)', () => {
    // URL with encoded unicode characters
    expect(validateUrlWithMessage('https://example.com/%E6%97%A5%E6%9C%AC%E8%AA%9E').isValid).toBe(true)
  })
})
