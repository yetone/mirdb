/**
 * URL validation utilities.
 * Owner: Scenario 6 - URL Input Validation and Error Feedback
 *
 * Provides:
 * - URL format validation
 * - Error message generation
 * - Protocol validation (http/https)
 *
 * Expected exports:
 * - isValidUrl: (url: string) => boolean
 * - getUrlError: (url: string) => string | null
 * - normalizeUrl: (url: string) => string
 */

/**
 * Validates if a string is a valid URL with http or https protocol.
 * @param url - The URL string to validate
 * @returns true if the URL is valid, false otherwise
 */
export function isValidUrl(url: string): boolean {
  if (!url || url.trim() === '') {
    return false
  }

  const trimmedUrl = url.trim()

  // Check for incomplete protocol-only URLs
  if (trimmedUrl === 'http://' || trimmedUrl === 'https://') {
    return false
  }

  try {
    const urlObj = new URL(trimmedUrl)
    // Only allow http and https protocols
    if (urlObj.protocol !== 'http:' && urlObj.protocol !== 'https:') {
      return false
    }
    // Ensure there's a valid hostname
    if (!urlObj.hostname || urlObj.hostname.length === 0) {
      return false
    }
    // Basic hostname validation - must have at least one character
    // and shouldn't be just a protocol
    return urlObj.hostname.length > 0
  } catch {
    return false
  }
}

/**
 * Returns an error message for invalid URLs, or null if the URL is valid.
 * @param url - The URL string to validate
 * @returns Error message string or null if valid
 */
export function getUrlError(url: string): string | null {
  if (!url || url.trim() === '') {
    return 'Please enter a URL'
  }

  const trimmedUrl = url.trim()

  // Check for incomplete protocol-only URLs
  if (trimmedUrl === 'http://' || trimmedUrl === 'https://') {
    return 'Please enter a complete URL'
  }

  // Check if URL has a protocol
  if (!trimmedUrl.startsWith('http://') && !trimmedUrl.startsWith('https://')) {
    return 'URL must start with http:// or https://'
  }

  try {
    const urlObj = new URL(trimmedUrl)

    // Only allow http and https protocols
    if (urlObj.protocol !== 'http:' && urlObj.protocol !== 'https:') {
      return 'URL must use http or https protocol'
    }

    // Ensure there's a valid hostname
    if (!urlObj.hostname || urlObj.hostname.length === 0) {
      return 'Please enter a valid URL'
    }

    return null
  } catch {
    return 'Please enter a valid URL'
  }
}

/**
 * Normalizes a URL by adding https:// protocol if missing.
 * @param url - The URL string to normalize
 * @returns Normalized URL string
 */
export function normalizeUrl(url: string): string {
  if (!url) {
    return ''
  }

  const trimmedUrl = url.trim()

  if (!trimmedUrl) {
    return ''
  }

  // If URL already has a protocol, return as is
  if (trimmedUrl.startsWith('http://') || trimmedUrl.startsWith('https://')) {
    return trimmedUrl
  }

  // Add https:// by default
  return `https://${trimmedUrl}`
}
