/**
 * URL Validation Utilities
 * Owner: Scenario 2 - Inline URL Shortening Demo
 * Enhanced: Scenario 15 - Error States and Edge Cases
 *
 * URL validation for the shortener form:
 * - isValidUrl(url: string): boolean
 * - isSafeUrl(url: string): boolean (rejects javascript:, data:, etc.)
 * - formatUrlForDisplay(url: string): string
 * - sanitizeUrl(url: string): string
 * - hasInvalidCharacters(url: string): boolean
 * - isUrlTooLong(url: string, maxLength?: number): boolean
 * - encodeUrlPath(url: string): string
 */

// Maximum URL length (common browser/server limit)
export const MAX_URL_LENGTH = 2048

/**
 * Validates if a string is a valid URL with http or https protocol
 */
export function isValidUrl(url: string): boolean {
  if (!url || typeof url !== 'string') {
    return false
  }

  const trimmed = url.trim()
  if (trimmed === '') {
    return false
  }

  try {
    const parsed = new URL(trimmed)
    return parsed.protocol === 'http:' || parsed.protocol === 'https:'
  } catch {
    return false
  }
}

/**
 * Checks if a URL is safe (not javascript:, data:, vbscript:, etc.)
 */
export function isSafeUrl(url: string): boolean {
  if (!url || typeof url !== 'string') {
    return false
  }

  const trimmed = url.trim().toLowerCase()
  const dangerousProtocols = ['javascript:', 'data:', 'vbscript:', 'file:']

  for (const protocol of dangerousProtocols) {
    if (trimmed.startsWith(protocol)) {
      return false
    }
  }

  return true
}

/**
 * Formats a URL for display by truncating if too long
 */
export function formatUrlForDisplay(url: string, maxLength = 50): string {
  if (!url || typeof url !== 'string') {
    return ''
  }

  const trimmed = url.trim()
  if (trimmed.length <= maxLength) {
    return trimmed
  }

  return trimmed.substring(0, maxLength - 3) + '...'
}

/**
 * Sanitizes a URL by trimming whitespace and ensuring protocol
 */
export function sanitizeUrl(url: string): string {
  if (!url || typeof url !== 'string') {
    return ''
  }

  let trimmed = url.trim()

  // Check if URL has any protocol (scheme://)
  const hasProtocol = /^[a-zA-Z][a-zA-Z0-9+.-]*:\/\//i.test(trimmed)

  // Add https:// only if no protocol is provided
  if (trimmed && !hasProtocol) {
    trimmed = 'https://' + trimmed
  }

  return trimmed
}

/**
 * Checks if URL contains invalid characters (like unencoded spaces)
 */
export function hasInvalidCharacters(url: string): boolean {
  if (!url || typeof url !== 'string') {
    return true
  }

  // Check for unencoded spaces in the URL
  // Spaces should be encoded as %20 or + in URLs
  if (url.includes(' ')) {
    return true
  }

  // Check for other invalid characters that are not typically allowed in URLs
  // Control characters (0x00-0x1F and 0x7F) and some special chars
  const invalidCharsPattern = /[\x00-\x1F\x7F<>{}|\\^`]/
  return invalidCharsPattern.test(url)
}

/**
 * Checks if URL exceeds the maximum allowed length
 */
export function isUrlTooLong(url: string, maxLength: number = MAX_URL_LENGTH): boolean {
  if (!url || typeof url !== 'string') {
    return false
  }

  return url.length > maxLength
}

/**
 * Properly encodes special characters in URL path
 */
export function encodeUrlPath(url: string): string {
  if (!url || typeof url !== 'string') {
    return ''
  }

  try {
    const parsed = new URL(url)
    // Encode the pathname to handle special characters
    const encodedPath = parsed.pathname
      .split('/')
      .map((segment) => encodeURIComponent(decodeURIComponent(segment)))
      .join('/')
    parsed.pathname = encodedPath
    return parsed.toString()
  } catch {
    return url
  }
}

/**
 * Validates URL and returns an error message if invalid
 */
export function validateUrlWithMessage(url: string): { isValid: boolean; error?: string } {
  if (!url || url.trim() === '') {
    return { isValid: false, error: 'Please enter a URL' }
  }

  const trimmed = url.trim()

  // Check safety BEFORE sanitization to catch javascript:, data:, etc.
  if (!isSafeUrl(trimmed)) {
    return { isValid: false, error: 'URL contains an unsafe protocol' }
  }

  // Check for invalid characters (like unencoded spaces)
  if (hasInvalidCharacters(trimmed)) {
    return { isValid: false, error: 'URL contains invalid characters' }
  }

  // Check URL length
  if (isUrlTooLong(trimmed)) {
    return { isValid: false, error: `URL exceeds maximum length of ${MAX_URL_LENGTH} characters` }
  }

  const sanitized = sanitizeUrl(trimmed)

  if (!isValidUrl(sanitized)) {
    return { isValid: false, error: 'Please enter a valid URL' }
  }

  return { isValid: true }
}

/**
 * Returns a user-friendly error message for API errors
 */
export function getApiErrorMessage(error: unknown): string {
  if (error instanceof Error) {
    const message = error.message.toLowerCase()

    // Network/connection errors
    if (message.includes('network') || message.includes('failed to fetch') || message.includes('net::')) {
      return 'Unable to connect. Please check your internet connection and try again.'
    }

    // Timeout errors
    if (message.includes('timeout') || message.includes('timed out')) {
      return 'Request timed out. Please try again.'
    }

    // Rate limiting
    if (message.includes('rate limit') || message.includes('too many requests')) {
      return 'Too many requests. Please wait a moment and try again.'
    }

    // Server errors (500)
    if (message.includes('500') || message.includes('internal server error')) {
      return 'Something went wrong on our end. Please try again later.'
    }

    // Return the original error message if it's meaningful
    return error.message
  }

  return 'Failed to shorten URL. Please try again.'
}
