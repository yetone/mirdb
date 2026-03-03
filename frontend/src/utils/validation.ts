/**
 * URL Validation Utilities
 * Owner: Scenario 6 - Guest URL Shortening Error Handling
 *
 * Provides URL validation for guest shortening feature.
 *
 * Expected exports:
 * - isValidUrl(url: string): boolean - Check if URL is valid format
 * - validateUrl(url: string): { valid: boolean, error?: string } - Detailed validation
 * - sanitizeUrl(url: string): string - Clean and normalize URL
 *
 * Requirements:
 * - Validate URL format (protocol, domain, etc.)
 * - Handle edge cases (empty, whitespace, special chars)
 * - Return user-friendly error messages
 */

export interface ValidationResult {
  valid: boolean;
  error?: string;
}

/**
 * Check if a URL string is a valid URL format.
 * @param url - The URL string to validate
 * @returns true if valid, false otherwise
 */
export function isValidUrl(url: string): boolean {
  if (!url || typeof url !== 'string') {
    return false;
  }

  const trimmed = url.trim();
  if (trimmed.length === 0) {
    return false;
  }

  try {
    const urlObj = new URL(trimmed);
    // Only allow http and https protocols
    return urlObj.protocol === 'http:' || urlObj.protocol === 'https:';
  } catch {
    return false;
  }
}

/**
 * Validate a URL and return detailed validation result.
 * @param url - The URL string to validate
 * @returns ValidationResult with valid status and optional error message
 */
export function validateUrl(url: string): ValidationResult {
  // Check for null/undefined
  if (url === null || url === undefined) {
    return {
      valid: false,
      error: 'Please enter a URL',
    };
  }

  // Check for non-string types
  if (typeof url !== 'string') {
    return {
      valid: false,
      error: 'Please enter a URL',
    };
  }

  // Trim and check for empty string
  const trimmed = url.trim();
  if (trimmed.length === 0) {
    return {
      valid: false,
      error: 'Please enter a URL',
    };
  }

  // Check for URL format
  try {
    const urlObj = new URL(trimmed);

    // Only allow http and https protocols
    if (urlObj.protocol !== 'http:' && urlObj.protocol !== 'https:') {
      return {
        valid: false,
        error: 'URL must start with http:// or https://',
      };
    }

    // Check for valid hostname
    if (!urlObj.hostname || urlObj.hostname.length === 0) {
      return {
        valid: false,
        error: 'Please enter a valid URL with a domain',
      };
    }

    return { valid: true };
  } catch {
    // URL constructor failed - invalid URL format
    return {
      valid: false,
      error: 'Please enter a valid URL (e.g., https://example.com)',
    };
  }
}

/**
 * Sanitize and normalize a URL string.
 * @param url - The URL string to sanitize
 * @returns Sanitized URL string
 */
export function sanitizeUrl(url: string): string {
  if (!url || typeof url !== 'string') {
    return '';
  }

  // Trim whitespace
  let sanitized = url.trim();

  // If no protocol, add https://
  if (sanitized && !sanitized.match(/^https?:\/\//i)) {
    sanitized = 'https://' + sanitized;
  }

  return sanitized;
}

/**
 * Error codes for API errors
 */
export type ApiErrorCode =
  | 'VALIDATION_ERROR'
  | 'RATE_LIMIT'
  | 'SERVER_ERROR'
  | 'NETWORK_ERROR'
  | 'TIMEOUT'
  | 'UNKNOWN';

export interface ApiError {
  code: ApiErrorCode;
  message: string;
  retryable: boolean;
}

/**
 * Parse an error from the API and return a user-friendly error object.
 * @param error - The error from axios or the API
 * @returns ApiError with code, message, and retryable status
 */
export function parseApiError(error: unknown): ApiError {
  // Check for axios error structure
  if (error && typeof error === 'object' && 'isAxiosError' in error) {
    const axiosError = error as {
      isAxiosError: boolean;
      response?: { status: number; data?: { detail?: string } };
      code?: string;
      message?: string;
    };

    // Check for network timeout
    if (axiosError.code === 'ECONNABORTED' || axiosError.code === 'ETIMEDOUT') {
      return {
        code: 'TIMEOUT',
        message: 'Request timed out. Please check your connection and try again.',
        retryable: true,
      };
    }

    // Check for network errors (no response)
    if (!axiosError.response) {
      return {
        code: 'NETWORK_ERROR',
        message: 'Unable to connect. Please check your internet connection and try again.',
        retryable: true,
      };
    }

    const status = axiosError.response.status;

    // Rate limit error (429)
    if (status === 429) {
      return {
        code: 'RATE_LIMIT',
        message: 'Too many requests. Please wait a moment and try again.',
        retryable: true,
      };
    }

    // Server error (5xx)
    if (status >= 500 && status < 600) {
      return {
        code: 'SERVER_ERROR',
        message: 'Something went wrong on our end. Please try again.',
        retryable: true,
      };
    }

    // Client validation error (400)
    if (status === 400) {
      const detail = axiosError.response.data?.detail;
      return {
        code: 'VALIDATION_ERROR',
        message: detail || 'Invalid URL provided. Please check and try again.',
        retryable: false,
      };
    }
  }

  // Check for Error object with message
  if (error instanceof Error) {
    // Check for timeout in error message
    if (error.message.toLowerCase().includes('timeout')) {
      return {
        code: 'TIMEOUT',
        message: 'Request timed out. Please check your connection and try again.',
        retryable: true,
      };
    }

    // Check for network in error message
    if (error.message.toLowerCase().includes('network')) {
      return {
        code: 'NETWORK_ERROR',
        message: 'Unable to connect. Please check your internet connection and try again.',
        retryable: true,
      };
    }
  }

  // Unknown error
  return {
    code: 'UNKNOWN',
    message: 'An unexpected error occurred. Please try again.',
    retryable: true,
  };
}
