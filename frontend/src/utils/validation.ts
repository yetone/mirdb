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

/** Maximum allowed URL length (browsers typically support up to 2083 characters) */
export const MAX_URL_LENGTH = 2048;

/**
 * Pattern to detect URLs without protocol that look like valid domains.
 * Used for auto-prefixing with https://
 * Supports:
 * - Domain names with TLDs (example.com)
 * - Subdomains (www.example.com)
 * - Internationalized domain names (IDN)
 * - Port numbers
 * - Path, query, and fragment components
 */
const DOMAIN_LIKE_PATTERN = /^([a-zA-Z0-9\u00A0-\uD7FF\uF900-\uFDCF\uFDF0-\uFFEF]([a-zA-Z0-9\u00A0-\uD7FF\uF900-\uFDCF\uFDF0-\uFFEF-]*[a-zA-Z0-9\u00A0-\uD7FF\uF900-\uFDCF\uFDF0-\uFFEF])?\.)+[a-zA-Z\u00A0-\uD7FF\uF900-\uFDCF\uFDF0-\uFFEF]{2,}(:\d{1,5})?([/?#][^\s]*)?$/i;

/**
 * Validates a URL using the native URL constructor.
 * Only allows http and https protocols.
 *
 * @param url - The URL string to validate (must have protocol)
 * @returns true if valid http/https URL, false otherwise
 */
function isValidUrlWithProtocol(url: string): boolean {
  try {
    const parsed = new URL(url);
    return parsed.protocol === 'http:' || parsed.protocol === 'https:';
  } catch {
    return false;
  }
}

/**
 * Checks if a string is a valid URL.
 *
 * @param url - The URL string to validate
 * @returns true if the URL is valid (including URLs that can be normalized), false otherwise
 *
 * @example
 * isValidUrl('https://example.com') // true
 * isValidUrl('http://example.com/path?query=value') // true
 * isValidUrl('example.com') // true (can be normalized)
 * isValidUrl('not a url') // false
 */
export function isValidUrl(url: string): boolean {
  if (!url || typeof url !== 'string') {
    return false;
  }

  const trimmed = url.trim();

  if (trimmed.length === 0) {
    return false;
  }

  // Check length limit
  if (trimmed.length > MAX_URL_LENGTH) {
    return false;
  }

  // Reject other protocol schemes (file:, javascript:, data:, ftp:, etc.)
  if (/^[a-zA-Z][a-zA-Z0-9+.-]*:/i.test(trimmed) && !/^https?:\/\//i.test(trimmed)) {
    return false;
  }

  // Check if it already has http/https protocol
  if (/^https?:\/\//i.test(trimmed)) {
    return isValidUrlWithProtocol(trimmed);
  }

  // Check if it looks like a domain that can be auto-prefixed
  // Must have at least one dot and a valid TLD pattern
  if (DOMAIN_LIKE_PATTERN.test(trimmed)) {
    // Verify it's valid when normalized
    const normalized = `https://${trimmed}`;
    return isValidUrlWithProtocol(normalized);
  }

  // No fallback for strings that don't look like domains
  // This rejects single words, incomplete protocols, etc.
  return false;
}

/**
 * Normalizes a URL by adding https:// protocol if missing.
 *
 * @param url - The URL string to normalize
 * @returns The normalized URL with protocol, or the original if already has one
 *
 * @example
 * normalizeUrl('https://example.com') // 'https://example.com'
 * normalizeUrl('http://example.com') // 'http://example.com'
 * normalizeUrl('example.com') // 'https://example.com'
 * normalizeUrl('example.com/path') // 'https://example.com/path'
 */
export function normalizeUrl(url: string): string {
  if (!url || typeof url !== 'string') {
    return '';
  }

  const trimmed = url.trim();

  if (trimmed.length === 0) {
    return '';
  }

  // Already has protocol
  if (/^https?:\/\//i.test(trimmed)) {
    return trimmed;
  }

  // Add https:// protocol
  return `https://${trimmed}`;
}

/**
 * Gets a validation error message for a URL, or null if valid.
 *
 * @param url - The URL string to validate
 * @returns Error message string if invalid, null if valid
 *
 * @example
 * getValidationError('') // 'Please enter a URL'
 * getValidationError('not a url') // 'Please enter a valid URL'
 * getValidationError('https://example.com') // null
 */
export function getValidationError(url: string): string | null {
  if (!url || typeof url !== 'string') {
    return 'Please enter a URL';
  }

  const trimmed = url.trim();

  if (trimmed.length === 0) {
    return 'Please enter a URL';
  }

  // Check for excessive length
  if (trimmed.length > MAX_URL_LENGTH) {
    return `URL is too long. Maximum length is ${MAX_URL_LENGTH} characters`;
  }

  // Check if it's a valid URL or can be normalized to one
  if (!isValidUrl(trimmed)) {
    return 'Please enter a valid URL';
  }

  return null;
}

/**
 * Result type for URL validation and submission.
 */
export interface UrlValidationResult {
  isValid: boolean;
  normalizedUrl: string;
  error: string | null;
}

/**
 * Validates a URL for form submission, returning both validation status
 * and the normalized URL ready for API submission.
 *
 * @param url - The URL string to validate
 * @returns Object containing validation result, normalized URL, and any error message
 *
 * @example
 * validateUrlForSubmission('example.com')
 * // { isValid: true, normalizedUrl: 'https://example.com', error: null }
 *
 * validateUrlForSubmission('not a url')
 * // { isValid: false, normalizedUrl: '', error: 'Please enter a valid URL' }
 */
export function validateUrlForSubmission(url: string): UrlValidationResult {
  const error = getValidationError(url);

  if (error !== null) {
    return {
      isValid: false,
      normalizedUrl: '',
      error,
    };
  }

  return {
    isValid: true,
    normalizedUrl: normalizeUrl(url),
    error: null,
  };
}
