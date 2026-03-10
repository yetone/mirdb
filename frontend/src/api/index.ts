/**
 * API Client for URL Shortening Service
 *
 * This file provides the axios client instance and API methods
 * for interacting with the backend.
 */

import axios, { AxiosError } from 'axios';
import type { AxiosInstance } from 'axios';
import type { ShortenUrlRequest, ShortenUrlResponse, ApiError } from '../types/landing';

// API base URL - can be configured via environment variables
const API_BASE_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000/api';

// Request timeout in milliseconds
const REQUEST_TIMEOUT = 10000;

/**
 * Create axios instance with default configuration
 */
export const apiClient: AxiosInstance = axios.create({
  baseURL: API_BASE_URL,
  timeout: REQUEST_TIMEOUT,
  headers: {
    'Content-Type': 'application/json',
  },
});

/**
 * Check if an error is an axios-like error (supports both real axios errors and mocks)
 */
function isAxiosLikeError(error: unknown): error is AxiosError {
  return axios.isAxiosError(error) ||
    (typeof error === 'object' && error !== null && 'isAxiosError' in error && (error as { isAxiosError: boolean }).isAxiosError === true);
}

/**
 * Parse API error into a user-friendly format
 */
export function parseApiError(error: unknown): ApiError {
  if (isAxiosLikeError(error)) {
    const axiosError = error as AxiosError<{ detail?: string }>;

    // Network error (no response)
    if (!axiosError.response) {
      if (axiosError.code === 'ECONNABORTED') {
        return {
          message: 'Request timed out. Please try again.',
          code: 'TIMEOUT',
        };
      }
      return {
        message: 'Unable to connect. Please check your internet connection and try again.',
        code: 'NETWORK_ERROR',
      };
    }

    // Server error response
    const status = axiosError.response.status;
    const detail = axiosError.response.data?.detail;

    if (status >= 500) {
      return {
        message: detail || 'Something went wrong. Please try again later.',
        code: 'SERVER_ERROR',
        status,
      };
    }

    if (status >= 400) {
      return {
        message: detail || 'Invalid request. Please check your input.',
        code: 'CLIENT_ERROR',
        status,
      };
    }

    return {
      message: detail || 'An error occurred.',
      code: 'UNKNOWN',
      status,
    };
  }

  // Non-axios error
  return {
    message: error instanceof Error ? error.message : 'An unexpected error occurred.',
    code: 'UNKNOWN',
  };
}

/**
 * Shorten a URL anonymously (no authentication required)
 */
export async function shortenUrlAnonymous(url: string): Promise<ShortenUrlResponse> {
  const request: ShortenUrlRequest = { url };
  const response = await apiClient.post<ShortenUrlResponse>('/urls/anonymous', request);
  return response.data;
}
