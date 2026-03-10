/**
 * URL Validation Utilities
 * Owner: Scenario 2 - URL Input Validation and Error Handling
 *
 * Validation functions for URL input.
 */

import { ValidationResult } from '../types/landing';
import { ERROR_MESSAGES } from '../constants/landingContent';

/**
 * Validates if a string is a valid HTTP or HTTPS URL.
 * Checks for proper protocol and valid domain structure.
 * @param url - The URL string to validate
 * @returns boolean - true if valid HTTP/HTTPS URL, false otherwise
 */
export function isValidHttpUrl(url: string): boolean {
  if (!url || typeof url !== 'string') {
    return false;
  }

  try {
    const urlObj = new URL(url);

    // Must be http or https
    if (urlObj.protocol !== 'http:' && urlObj.protocol !== 'https:') {
      return false;
    }

    // Hostname must have a valid structure:
    // - localhost is allowed
    // - IP addresses are allowed (e.g., 192.168.1.1)
    // - Domain names must contain at least one dot (e.g., example.com)
    const hostname = urlObj.hostname;
    if (!hostname) {
      return false;
    }

    // Allow localhost
    if (hostname === 'localhost') {
      return true;
    }

    // Allow IP addresses (simple check for digits and dots)
    if (/^[\d.]+$/.test(hostname)) {
      // Basic IPv4 validation
      const parts = hostname.split('.');
      if (parts.length === 4 && parts.every(p => {
        const num = parseInt(p, 10);
        return num >= 0 && num <= 255;
      })) {
        return true;
      }
    }

    // For domain names, require at least one dot (e.g., example.com)
    // This prevents single-word "domains" like "invalid" from being accepted
    if (!hostname.includes('.')) {
      return false;
    }

    return true;
  } catch {
    return false;
  }
}

/**
 * Normalizes a URL by adding https:// if no protocol is present.
 * If the URL already has a valid protocol, it returns the URL unchanged.
 * @param url - The URL string to normalize
 * @returns The normalized URL string
 */
export function normalizeUrl(url: string): string {
  if (!url || typeof url !== 'string') {
    return url;
  }

  const trimmedUrl = url.trim();

  // If it already starts with http:// or https://, return as is
  if (/^https?:\/\//i.test(trimmedUrl)) {
    return trimmedUrl;
  }

  // If it starts with another protocol, don't modify
  if (/^[a-z][a-z0-9+.-]*:\/\//i.test(trimmedUrl)) {
    return trimmedUrl;
  }

  // Add https:// if no protocol present
  return `https://${trimmedUrl}`;
}

/**
 * Validates a URL and returns detailed validation result.
 * @param url - The URL string to validate
 * @returns ValidationResult with valid status, optional error message, and normalized URL
 */
export function validateUrl(url: string): ValidationResult {
  // Check for empty input
  if (!url || url.trim() === '') {
    return {
      valid: false,
      error: ERROR_MESSAGES.emptyUrl,
    };
  }

  const trimmedUrl = url.trim();

  // Try to normalize the URL first
  const normalizedUrl = normalizeUrl(trimmedUrl);

  // Validate the normalized URL
  if (!isValidHttpUrl(normalizedUrl)) {
    return {
      valid: false,
      error: ERROR_MESSAGES.invalidUrl,
    };
  }

  return {
    valid: true,
    normalizedUrl,
  };
}

/**
 * Quick check if URL is valid (returns boolean only).
 * This is a convenience wrapper around validateUrl.
 * @param url - The URL string to check
 * @returns boolean - true if URL is valid, false otherwise
 */
export function isValidUrl(url: string): boolean {
  return validateUrl(url).valid;
}
