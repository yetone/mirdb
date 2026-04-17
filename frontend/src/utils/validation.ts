/**
 * URL Validation Utilities
 * Owner: Scenario 2 - Inline URL Shortening Demo
 *
 * URL validation for the shortener form:
 * - isValidUrl(url: string): boolean
 * - isSafeUrl(url: string): boolean (rejects javascript:, data:, etc.)
 * - formatUrlForDisplay(url: string): string
 * - sanitizeUrl(url: string): string
 */

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

  const sanitized = sanitizeUrl(trimmed)

  if (!isValidUrl(sanitized)) {
    return { isValid: false, error: 'Please enter a valid URL' }
  }

  return { isValid: true }
}
