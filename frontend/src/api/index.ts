/**
 * API Client
 * Owner: Scenario 9 - Error Handling and API Failure Resilience
 *
 * Centralized API client for URL shortening with comprehensive
 * error handling, timeout support, and user-friendly error messages.
 */

export type ApiErrorType =
  | 'network'
  | 'timeout'
  | 'server'
  | 'rate_limit'
  | 'validation'
  | 'cors'
  | 'unknown';

export interface ApiErrorResponse {
  type: ApiErrorType;
  message: string;
  statusCode?: number;
  retryAfter?: number;
  details?: string;
}

export class ApiError extends Error {
  type: ApiErrorType;
  statusCode?: number;
  retryAfter?: number;
  details?: string;

  constructor(response: ApiErrorResponse) {
    super(response.message);
    this.name = 'ApiError';
    this.type = response.type;
    this.statusCode = response.statusCode;
    this.retryAfter = response.retryAfter;
    this.details = response.details;
  }
}

const API_BASE_URL = import.meta.env.VITE_API_URL || '';
const REQUEST_TIMEOUT_MS = 30000;

function getUserFriendlyMessage(type: ApiErrorType, statusCode?: number, details?: string): string {
  switch (type) {
    case 'network':
      return 'Unable to connect. Please try again later.';
    case 'timeout':
      return 'Request timed out. Please try again.';
    case 'server':
      return 'Something went wrong. Please try again.';
    case 'rate_limit':
      return 'Too many requests. Please wait a moment before trying again.';
    case 'validation':
      return details || 'Invalid input. Please check your URL and try again.';
    case 'cors':
      return 'Unable to connect. Please try again later.';
    case 'unknown':
    default:
      return 'Something went wrong. Please try again.';
  }
}

async function fetchWithTimeout(
  url: string,
  options: RequestInit,
  timeoutMs: number
): Promise<Response> {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const response = await fetch(url, {
      ...options,
      signal: controller.signal,
    });
    return response;
  } catch (error) {
    if (error instanceof DOMException && error.name === 'AbortError') {
      throw new ApiError({
        type: 'timeout',
        message: getUserFriendlyMessage('timeout'),
      });
    }
    throw error;
  } finally {
    clearTimeout(timeoutId);
  }
}

export async function shortenUrl(originalUrl: string): Promise<{ shortUrl: string }> {
  const url = `${API_BASE_URL}/api/urls/shorten`;

  try {
    const response = await fetchWithTimeout(
      url,
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ original_url: originalUrl }),
      },
      REQUEST_TIMEOUT_MS
    );

    if (response.ok) {
      const data = await response.json();
      return { shortUrl: data.short_url || data.short_code };
    }

    const statusCode = response.status;

    if (statusCode === 429) {
      const retryAfter = response.headers.get('Retry-After');
      throw new ApiError({
        type: 'rate_limit',
        message: getUserFriendlyMessage('rate_limit'),
        statusCode,
        retryAfter: retryAfter ? parseInt(retryAfter, 10) : undefined,
      });
    }

    if (statusCode === 422) {
      let details: string | undefined;
      try {
        const errorData = await response.json();
        details = errorData.detail || errorData.message;
      } catch {
        // Response body is not JSON
      }
      throw new ApiError({
        type: 'validation',
        message: getUserFriendlyMessage('validation', statusCode, details),
        statusCode,
        details,
      });
    }

    if (statusCode >= 500) {
      throw new ApiError({
        type: 'server',
        message: getUserFriendlyMessage('server', statusCode),
        statusCode,
      });
    }

    throw new ApiError({
      type: 'unknown',
      message: getUserFriendlyMessage('unknown', statusCode),
      statusCode,
    });
  } catch (error) {
    if (error instanceof ApiError) {
      throw error;
    }

    if (error instanceof TypeError) {
      if (error.message.includes('Failed to fetch') || error.message.includes('NetworkError')) {
        throw new ApiError({
          type: 'network',
          message: getUserFriendlyMessage('network'),
        });
      }
      if (error.message.includes('CORS') || error.message.includes('cross-origin')) {
        throw new ApiError({
          type: 'cors',
          message: getUserFriendlyMessage('cors'),
        });
      }
    }

    throw new ApiError({
      type: 'unknown',
      message: getUserFriendlyMessage('unknown'),
    });
  }
}

export function isApiError(error: unknown): error is ApiError {
  return error instanceof ApiError;
}
