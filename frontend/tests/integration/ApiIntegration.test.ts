/**
 * API Integration Tests
 * Owner: Scenario 2 - URL Input Validation and Error Handling
 *
 * Tests for API error handling and network error scenarios.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { http, HttpResponse, delay } from 'msw';
import { server } from '../mocks/server';
import { shortenUrlAnonymous, parseApiError, apiClient } from '../../src/api';

const API_BASE_URL = 'http://localhost:8000/api';

describe('API Integration', () => {
  describe('shortenUrlAnonymous', () => {
    it('successfully shortens a valid URL', async () => {
      const result = await shortenUrlAnonymous('https://example.com/very-long-path');

      expect(result).toEqual({
        short_url: 'http://localhost:8000/abc123',
        short_code: 'abc123',
        original_url: 'https://example.com/very-long-path',
      });
    });

    // Test Case 4: API returns 500 error
    it('handles server 500 error with user-friendly message', async () => {
      // Override the default handler with a 500 error
      server.use(
        http.post(`${API_BASE_URL}/urls/anonymous`, () => {
          return HttpResponse.json(
            { detail: 'Internal server error' },
            { status: 500 }
          );
        })
      );

      try {
        await shortenUrlAnonymous('https://example.com');
        expect.fail('Should have thrown an error');
      } catch (error) {
        const parsedError = parseApiError(error);
        expect(parsedError.message).toBe('Internal server error');
        expect(parsedError.code).toBe('SERVER_ERROR');
        expect(parsedError.status).toBe(500);
      }
    });

    it('handles generic server error without detail', async () => {
      server.use(
        http.post(`${API_BASE_URL}/urls/anonymous`, () => {
          return HttpResponse.json({}, { status: 500 });
        })
      );

      try {
        await shortenUrlAnonymous('https://example.com');
        expect.fail('Should have thrown an error');
      } catch (error) {
        const parsedError = parseApiError(error);
        expect(parsedError.message).toBe('Something went wrong. Please try again later.');
        expect(parsedError.code).toBe('SERVER_ERROR');
      }
    });

    // Test Case 5: Network timeout during submission
    it('handles network timeout with timeout message', () => {
      // Create a mock axios error that simulates a timeout
      // This tests the error parsing logic without relying on actual network timing
      const timeoutError = {
        isAxiosError: true,
        code: 'ECONNABORTED',
        message: 'timeout of 10000ms exceeded',
        response: undefined,
        config: {},
        toJSON: () => ({}),
      };

      // Test that parseApiError handles timeout errors correctly
      const parsedError = parseApiError(timeoutError);
      expect(parsedError.message).toBe('Request timed out. Please try again.');
      expect(parsedError.code).toBe('TIMEOUT');
    });

    it('handles network error (no response)', async () => {
      server.use(
        http.post(`${API_BASE_URL}/urls/anonymous`, () => {
          return HttpResponse.error();
        })
      );

      try {
        await shortenUrlAnonymous('https://example.com');
        expect.fail('Should have thrown an error');
      } catch (error) {
        const parsedError = parseApiError(error);
        expect(parsedError.message).toBe(
          'Unable to connect. Please check your internet connection and try again.'
        );
        expect(parsedError.code).toBe('NETWORK_ERROR');
      }
    });

    it('handles 400 bad request error', async () => {
      server.use(
        http.post(`${API_BASE_URL}/urls/anonymous`, () => {
          return HttpResponse.json(
            { detail: 'Invalid URL format' },
            { status: 400 }
          );
        })
      );

      try {
        await shortenUrlAnonymous('invalid-url');
        expect.fail('Should have thrown an error');
      } catch (error) {
        const parsedError = parseApiError(error);
        expect(parsedError.message).toBe('Invalid URL format');
        expect(parsedError.code).toBe('CLIENT_ERROR');
        expect(parsedError.status).toBe(400);
      }
    });

    it('handles 404 not found error', async () => {
      server.use(
        http.post(`${API_BASE_URL}/urls/anonymous`, () => {
          return HttpResponse.json(
            { detail: 'Endpoint not found' },
            { status: 404 }
          );
        })
      );

      try {
        await shortenUrlAnonymous('https://example.com');
        expect.fail('Should have thrown an error');
      } catch (error) {
        const parsedError = parseApiError(error);
        expect(parsedError.message).toBe('Endpoint not found');
        expect(parsedError.code).toBe('CLIENT_ERROR');
        expect(parsedError.status).toBe(404);
      }
    });

    it('handles 503 service unavailable error', async () => {
      server.use(
        http.post(`${API_BASE_URL}/urls/anonymous`, () => {
          return HttpResponse.json(
            { detail: 'Service temporarily unavailable' },
            { status: 503 }
          );
        })
      );

      try {
        await shortenUrlAnonymous('https://example.com');
        expect.fail('Should have thrown an error');
      } catch (error) {
        const parsedError = parseApiError(error);
        expect(parsedError.message).toBe('Service temporarily unavailable');
        expect(parsedError.code).toBe('SERVER_ERROR');
        expect(parsedError.status).toBe(503);
      }
    });
  });

  describe('parseApiError', () => {
    it('handles non-axios errors', () => {
      const error = new Error('Generic error');
      const result = parseApiError(error);

      expect(result.message).toBe('Generic error');
      expect(result.code).toBe('UNKNOWN');
    });

    it('handles non-error objects', () => {
      const result = parseApiError('string error');

      expect(result.message).toBe('An unexpected error occurred.');
      expect(result.code).toBe('UNKNOWN');
    });

    it('handles null/undefined', () => {
      expect(parseApiError(null).message).toBe('An unexpected error occurred.');
      expect(parseApiError(undefined).message).toBe('An unexpected error occurred.');
    });
  });
});
