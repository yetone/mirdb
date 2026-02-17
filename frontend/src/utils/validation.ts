/**
 * Input validation utilities.
 * Owner: Scenario 5 - Interactive URL Shortening Demo
 *
 * Provides URL validation for the demo section:
 * - URL format validation
 * - Scheme whitelist (http, https only)
 * - Malicious scheme detection
 */

/** List of allowed URL schemes */
const ALLOWED_SCHEMES = ['http:', 'https:']

/** List of malicious schemes to block */
const BLOCKED_SCHEMES = ['javascript:', 'data:', 'vbscript:', 'file:']

/**
 * Validates if a URL has a safe, allowed scheme (http or https only)
 * @param url - The URL string to validate
 * @returns true if the URL uses http or https scheme
 */
export function validateUrlScheme(url: string): boolean {
  try {
    const parsedUrl = new URL(url)
    return ALLOWED_SCHEMES.includes(parsedUrl.protocol.toLowerCase())
  } catch {
    return false
  }
}

/**
 * Checks if a URL uses a potentially malicious scheme
 * @param url - The URL string to check
 * @returns true if the URL uses a dangerous scheme (javascript:, data:, etc.)
 */
export function hasMaliciousScheme(url: string): boolean {
  const trimmedUrl = url.trim().toLowerCase()
  return BLOCKED_SCHEMES.some(scheme => trimmedUrl.startsWith(scheme))
}

/**
 * Validates if a string is a valid URL with allowed scheme
 * @param url - The URL string to validate
 * @returns true if the URL is valid and uses http/https scheme
 */
export function isValidUrl(url: string): boolean {
  if (!url || typeof url !== 'string') {
    return false
  }

  const trimmedUrl = url.trim()

  // Check for malicious schemes first
  if (hasMaliciousScheme(trimmedUrl)) {
    return false
  }

  // Validate URL format and scheme
  return validateUrlScheme(trimmedUrl)
}

/**
 * Sanitizes a URL by trimming whitespace and normalizing
 * @param url - The URL string to sanitize
 * @returns Sanitized URL string
 */
export function sanitizeUrl(url: string): string {
  if (!url || typeof url !== 'string') {
    return ''
  }
  return url.trim()
}

/**
 * Returns a validation error message for a URL, or null if valid
 * @param url - The URL string to validate
 * @returns Error message string or null if valid
 */
export function getUrlValidationError(url: string): string | null {
  if (!url || url.trim() === '') {
    return 'Please enter a URL'
  }

  const trimmedUrl = url.trim()

  if (hasMaliciousScheme(trimmedUrl)) {
    return 'URLs with javascript: or data: schemes are not allowed'
  }

  if (!validateUrlScheme(trimmedUrl)) {
    return 'Please enter a valid URL starting with http:// or https://'
  }

  return null
}
