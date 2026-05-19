/**
 * Unit tests for API error mapping utility.
 */

import { describe, it, expect } from 'vitest';
import { mapApiError } from '../../../src/utils/errorHandler';

describe('mapApiError', () => {
  describe('network errors', () => {
    it('returns a user-friendly message for a generic network error', () => {
      const error = new Error('Failed to fetch');
      const result = mapApiError(error);
      expect(result.message).toBe('Unable to connect to the server. Please check your internet connection and try again.');
      expect(result.retryable).toBe(true);
    });

    it('returns a user-friendly message for a TypeError with network message', () => {
      const error = new TypeError('NetworkError when attempting to fetch resource.');
      const result = mapApiError(error);
      expect(result.message).toBe('Unable to connect to the server. Please check your internet connection and try again.');
      expect(result.retryable).toBe(true);
    });

    it('returns a user-friendly message for CORS error', () => {
      const error = new TypeError('CORS error');
      const result = mapApiError(error);
      expect(result.message).toBe('Unable to connect to the server. Please check your internet connection and try again.');
      expect(result.retryable).toBe(true);
    });

    it('returns a user-friendly message for timeout error', () => {
      const error = new Error('Request timeout');
      const result = mapApiError(error);
      expect(result.message).toBe('The request timed out. Please check your connection and try again.');
      expect(result.retryable).toBe(true);
    });

    it('returns a user-friendly message for abort error', () => {
      const error = new Error('Request aborted');
      const result = mapApiError(error);
      expect(result.message).toBe('The request timed out. Please check your connection and try again.');
      expect(result.retryable).toBe(true);
    });
  });

  describe('HTTP 400 Bad Request', () => {
    it('maps 400 from a Response object to validation message', () => {
      const response = new Response(null, { status: 400, statusText: 'Bad Request' });
      const result = mapApiError(response);
      expect(result.message).toBe('Please enter a valid URL');
      expect(result.retryable).toBe(true);
    });

    it('maps 400 from an Error message string', () => {
      const error = new Error('HTTP 400: Bad Request');
      const result = mapApiError(error);
      expect(result.message).toBe('Please enter a valid URL');
      expect(result.retryable).toBe(true);
    });
  });

  describe('HTTP 401/403 Unauthorized', () => {
    it('maps 401 to auth message', () => {
      const response = new Response(null, { status: 401, statusText: 'Unauthorized' });
      const result = mapApiError(response);
      expect(result.message).toBe('You are not authorized to perform this action. Please sign in and try again.');
      expect(result.retryable).toBe(true);
    });

    it('maps 403 to auth message', () => {
      const response = new Response(null, { status: 403, statusText: 'Forbidden' });
      const result = mapApiError(response);
      expect(result.message).toBe('You are not authorized to perform this action. Please sign in and try again.');
      expect(result.retryable).toBe(true);
    });
  });

  describe('HTTP 429 Rate Limit', () => {
    it('maps 429 from a Response object to rate-limit message', () => {
      const response = new Response(null, { status: 429, statusText: 'Too Many Requests' });
      const result = mapApiError(response);
      expect(result.message).toBe('Too many requests. Please wait a moment and try again.');
      expect(result.retryable).toBe(true);
    });

    it('maps 429 from an Error message string', () => {
      const error = new Error('HTTP 429: Too Many Requests');
      const result = mapApiError(error);
      expect(result.message).toBe('Too many requests. Please wait a moment and try again.');
      expect(result.retryable).toBe(true);
    });
  });

  describe('HTTP 5xx Server Errors', () => {
    it('maps 500 to server error message', () => {
      const response = new Response(null, { status: 500, statusText: 'Internal Server Error' });
      const result = mapApiError(response);
      expect(result.message).toBe('Something went wrong on our end. Please try again in a moment.');
      expect(result.retryable).toBe(true);
    });

    it('maps 502 to server error message', () => {
      const response = new Response(null, { status: 502, statusText: 'Bad Gateway' });
      const result = mapApiError(response);
      expect(result.message).toBe('Something went wrong on our end. Please try again in a moment.');
      expect(result.retryable).toBe(true);
    });

    it('maps 503 to server error message', () => {
      const response = new Response(null, { status: 503, statusText: 'Service Unavailable' });
      const result = mapApiError(response);
      expect(result.message).toBe('Something went wrong on our end. Please try again in a moment.');
      expect(result.retryable).toBe(true);
    });

    it('maps 504 to server error message', () => {
      const response = new Response(null, { status: 504, statusText: 'Gateway Timeout' });
      const result = mapApiError(response);
      expect(result.message).toBe('Something went wrong on our end. Please try again in a moment.');
      expect(result.retryable).toBe(true);
    });

    it('maps 500 from an Error message string', () => {
      const error = new Error('HTTP 500: Internal Server Error');
      const result = mapApiError(error);
      expect(result.message).toBe('Something went wrong on our end. Please try again in a moment.');
      expect(result.retryable).toBe(true);
    });

    it('maps 502 from an Error message string', () => {
      const error = new Error('HTTP 502: Bad Gateway');
      const result = mapApiError(error);
      expect(result.message).toBe('Something went wrong on our end. Please try again in a moment.');
      expect(result.retryable).toBe(true);
    });
  });

  describe('unknown / generic errors', () => {
    it('maps an arbitrary Error to generic message', () => {
      const error = new Error('Something random happened');
      const result = mapApiError(error);
      expect(result.message).toBe('An unexpected error occurred. Please try again.');
      expect(result.retryable).toBe(true);
    });

    it('maps a string to generic message', () => {
      const result = mapApiError('oops');
      expect(result.message).toBe('An unexpected error occurred. Please try again.');
      expect(result.retryable).toBe(true);
    });

    it('maps null to generic message', () => {
      const result = mapApiError(null);
      expect(result.message).toBe('An unexpected error occurred. Please try again.');
      expect(result.retryable).toBe(true);
    });

    it('maps undefined to generic message', () => {
      const result = mapApiError(undefined);
      expect(result.message).toBe('An unexpected error occurred. Please try again.');
      expect(result.retryable).toBe(true);
    });

    it('maps an object to generic message', () => {
      const result = mapApiError({ code: 123 });
      expect(result.message).toBe('An unexpected error occurred. Please try again.');
      expect(result.retryable).toBe(true);
    });
  });
});
