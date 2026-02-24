/**
 * URL Validation Utilities
 * Owner: Scenario 4 - URL Validation
 *
 * Provides client-side URL validation functions.
 *
 * Validation Rules:
 * - Must be valid http:// or https:// URL
 * - Reject javascript: and data: protocols
 * - Basic format validation
 */

import { ValidationResult } from '@/types/home'

/**
 * Allowed URL protocols for security
 */
const ALLOWED_PROTOCOLS = ['http:', 'https:']

/**
 * Blocked protocols for security (XSS prevention)
 */
const BLOCKED_PROTOCOLS = ['javascript:', 'data:', 'vbscript:', 'file:']

/**
 * Regular expression for basic URL format validation
 * Matches URLs starting with http:// or https:// followed by valid domain characters
 */
export const URL_REGEX = /^https?:\/\/[^\s/$.?#].[^\s]*$/i

/**
 * Check if a URL string is valid
 *
 * @param url - The URL string to validate
 * @returns true if the URL is valid, false otherwise
 */
export function isValidUrl(url: string): boolean {
  return validateUrl(url).valid
}

/**
 * Validate a URL string and return detailed result
 *
 * @param url - The URL string to validate
 * @returns ValidationResult with valid status and optional error message
 */
export function validateUrl(url: string): ValidationResult {
  // Check for empty or whitespace-only input
  const trimmedUrl = url.trim()
  if (!trimmedUrl) {
    return {
      valid: false,
      error: 'Please enter a URL',
    }
  }

  // Check for blocked protocols (security check before parsing)
  const lowerUrl = trimmedUrl.toLowerCase()
  for (const protocol of BLOCKED_PROTOCOLS) {
    if (lowerUrl.startsWith(protocol)) {
      return {
        valid: false,
        error: 'Please enter a valid URL',
      }
    }
  }

  // Try to parse the URL
  let parsedUrl: URL
  try {
    parsedUrl = new URL(trimmedUrl)
  } catch {
    return {
      valid: false,
      error: 'Please enter a valid URL',
    }
  }

  // Check if protocol is allowed (http or https only)
  if (!ALLOWED_PROTOCOLS.includes(parsedUrl.protocol)) {
    return {
      valid: false,
      error: 'Please enter a valid URL',
    }
  }

  // Additional regex check for URL format
  if (!URL_REGEX.test(trimmedUrl)) {
    return {
      valid: false,
      error: 'Please enter a valid URL',
    }
  }

  return { valid: true }
}
