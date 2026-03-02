/**
 * URL Validation Utilities.
 * Owner: Scenario 16 - URL Input Validation
 *
 * Validation functions for URL input:
 * - isValidUrl(url: string): boolean
 * - normalizeUrl(url: string): string
 * - getValidationError(url: string): string | null
 *
 * Handles:
 * - Protocol detection and auto-prefixing
 * - Special characters
 * - Internationalized domain names
 * - Length limits
 */

// Maximum URL length - most browsers support up to 2083 characters
export const MAX_URL_LENGTH = 2048;

// URL pattern that supports:
// - Optional protocol (http:// or https://)
// - Domain names with subdomains
// - IDN (internationalized domain names) via punycode or unicode
// - Paths, query strings, and fragments
// - IP addresses
// - Query params directly after domain (no path required)
const URL_PATTERN =
  /^(https?:\/\/)?([a-zA-Z0-9\u00A0-\uD7FF\uF900-\uFDCF\uFDF0-\uFFEF]([a-zA-Z0-9\u00A0-\uD7FF\uF900-\uFDCF\uFDF0-\uFFEF\-]*[a-zA-Z0-9\u00A0-\uD7FF\uF900-\uFDCF\uFDF0-\uFFEF])?\.)+[a-zA-Z\u00A0-\uD7FF\uF900-\uFDCF\uFDF0-\uFFEF]{2,}(:\d+)?([/?#][^\s]*)?$/;

// Pattern for localhost URLs with optional port and path
const LOCALHOST_PATTERN = /^(https?:\/\/)?localhost(:\d+)?([/?#][^\s]*)?$/;

/**
 * Checks if a string represents a valid URL.
 *
 * @param url - The URL string to validate
 * @returns true if the URL is valid, false otherwise
 */
export function isValidUrl(url: string): boolean {
  if (!url || url.trim() === '') {
    return false;
  }

  const trimmed = url.trim();

  // Check for obvious non-URLs (spaces)
  if (trimmed.includes(' ')) {
    return false;
  }

  // Allow localhost URLs
  if (LOCALHOST_PATTERN.test(trimmed)) {
    return true;
  }

  // Check for dots in the main part (required for non-localhost URLs)
  if (!trimmed.includes('.')) {
    return false;
  }

  // Try to construct a URL object for validation
  try {
    const urlToTest = normalizeUrl(trimmed);
    new URL(urlToTest);

    // Additional pattern check to catch edge cases
    return URL_PATTERN.test(trimmed);
  } catch {
    return false;
  }
}

/**
 * Normalizes a URL by adding the https:// protocol if missing.
 *
 * @param url - The URL string to normalize
 * @returns The normalized URL with protocol
 */
export function normalizeUrl(url: string): string {
  const trimmed = url.trim();

  if (!trimmed) {
    return trimmed;
  }

  // If the URL already has a protocol, return it as-is
  if (trimmed.startsWith('http://') || trimmed.startsWith('https://')) {
    return trimmed;
  }

  // Add https:// as the default protocol
  return `https://${trimmed}`;
}

/**
 * Returns a validation error message for an invalid URL, or null if valid.
 *
 * @param url - The URL string to validate
 * @returns Error message string or null if the URL is valid
 */
export function getValidationError(url: string): string | null {
  // Check for empty input
  if (!url || url.trim() === '') {
    return 'Please enter a URL';
  }

  const trimmed = url.trim();

  // Check for excessive length
  if (trimmed.length > MAX_URL_LENGTH) {
    return `URL is too long (max ${MAX_URL_LENGTH} characters)`;
  }

  // Check for valid URL format
  if (!isValidUrl(trimmed)) {
    return 'Please enter a valid URL';
  }

  return null;
}

/**
 * Validates a URL and returns a result object with normalized URL if valid.
 *
 * @param url - The URL string to validate
 * @returns Object with isValid flag, normalizedUrl, and optional error
 */
export function validateUrl(url: string): {
  isValid: boolean;
  normalizedUrl: string | null;
  error: string | null;
} {
  const error = getValidationError(url);

  if (error) {
    return {
      isValid: false,
      normalizedUrl: null,
      error,
    };
  }

  return {
    isValid: true,
    normalizedUrl: normalizeUrl(url),
    error: null,
  };
}
