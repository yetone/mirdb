/**
 * URL validation utilities.
 * Owner: Scenario 3 - URL Input Validation
 *
 * Expected exports:
 * - validateUrl(url: string): UrlValidationResult
 * - isValidUrlFormat(url: string): boolean
 * - getUrlValidationError(url: string): string | null
 *
 * Validation rules:
 * - Must be a valid URL format
 * - Must include protocol (http:// or https://)
 * - Must have valid domain
 * - Returns appropriate error messages
 */

import type { UrlValidationResult } from '../types/homepage';

/**
 * Validates a URL string and returns a validation result.
 *
 * @param url - The URL string to validate
 * @returns UrlValidationResult with isValid flag and optional errorMessage
 */
export function validateUrl(url: string): UrlValidationResult {
  // Check for empty input
  if (!url || url.trim() === '') {
    return {
      isValid: false,
      errorMessage: 'Please enter a URL',
    };
  }

  const trimmedUrl = url.trim();

  // Check if URL has a protocol
  const hasProtocol = /^[a-zA-Z][a-zA-Z0-9+.-]*:\/\//.test(trimmedUrl);

  // Check if it looks like a URL without protocol (has domain-like pattern)
  const looksLikeUrlWithoutProtocol = /^[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?(\.[a-zA-Z]{2,})+/.test(trimmedUrl);

  if (!hasProtocol && looksLikeUrlWithoutProtocol) {
    return {
      isValid: false,
      errorMessage: 'URL must include protocol (http:// or https://)',
    };
  }

  // Check for valid protocol (only http or https allowed)
  const hasValidProtocol = /^https?:\/\//.test(trimmedUrl);

  if (!hasValidProtocol) {
    return {
      isValid: false,
      errorMessage: 'Please enter a valid URL',
    };
  }

  // Try to parse as URL to check validity
  try {
    const parsedUrl = new URL(trimmedUrl);

    // Ensure we have a valid hostname
    if (!parsedUrl.hostname || parsedUrl.hostname.length === 0) {
      return {
        isValid: false,
        errorMessage: 'Please enter a valid URL',
      };
    }

    return { isValid: true };
  } catch {
    return {
      isValid: false,
      errorMessage: 'Please enter a valid URL',
    };
  }
}

/**
 * Checks if a URL string has a valid format.
 *
 * @param url - The URL string to check
 * @returns true if the URL format is valid, false otherwise
 */
export function isValidUrlFormat(url: string): boolean {
  return validateUrl(url).isValid;
}

/**
 * Gets the validation error message for a URL, or null if valid.
 *
 * @param url - The URL string to validate
 * @returns Error message string or null if valid
 */
export function getUrlValidationError(url: string): string | null {
  const result = validateUrl(url);
  return result.errorMessage ?? null;
}
