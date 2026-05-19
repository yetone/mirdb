/**
 * URL validation utilities.
 * Owner: Scenario 9 - Error Handling and Edge Cases
 *
 * Expected exports:
 * - isValidHttpUrl(value: string): boolean
 * - URL must use http/https scheme and be under URL_MAX_LENGTH
 */

import { URL_MAX_LENGTH } from './constants';

const BLOCKED_SCHEMES = ['javascript:', 'data:', 'vbscript:', 'file:'];

/**
 * Validates that a string is a well-formed HTTP/HTTPS URL,
 * does not exceed the maximum allowed length,
 * and does not use a blocked (potentially malicious) scheme.
 */
export function isValidHttpUrl(value: string): boolean {
  if (!value || typeof value !== 'string') {
    return false;
  }

  const trimmed = value.trim();

  if (trimmed.length === 0 || trimmed.length > URL_MAX_LENGTH) {
    return false;
  }

  const lower = trimmed.toLowerCase();
  for (const scheme of BLOCKED_SCHEMES) {
    if (lower.startsWith(scheme)) {
      return false;
    }
  }

  try {
    const url = new URL(trimmed);
    return url.protocol === 'http:' || url.protocol === 'https:';
  } catch {
    return false;
  }
}

/**
 * Returns a user-facing validation message for an invalid URL,
 * or null when the URL is valid.
 */
export function getUrlValidationError(value: string): string | null {
  if (!value || value.trim().length === 0) {
    return 'Please enter a URL';
  }

  if (value.trim().length > URL_MAX_LENGTH) {
    return `URL must be under ${URL_MAX_LENGTH} characters`;
  }

  const lower = value.trim().toLowerCase();
  for (const scheme of BLOCKED_SCHEMES) {
    if (lower.startsWith(scheme)) {
      return 'Unsafe URL scheme is not allowed';
    }
  }

  try {
    const url = new URL(value.trim());
    if (url.protocol !== 'http:' && url.protocol !== 'https:') {
      return 'Please enter a valid URL';
    }
  } catch {
    return 'Please enter a valid URL';
  }

  return null;
}
