/**
 * Unit tests for API error handling utilities.
 * Owner: Scenario 17 - Error Handling and Graceful Degradation
 */

import { describe, it, expect } from 'vitest';
import {
  ApiError,
  NetworkError,
  TimeoutError,
  ServerError,
  ClientError,
  NotFoundError,
  responseToError,
  getUserFriendlyMessage,
  ApiErrorCode,
} from '../../../src/api/errors';

describe('ApiError', () => {
  it('creates a basic error with code', () => {
    const error = new ApiError('Something failed', ApiErrorCode.UNKNOWN);
    expect(error.message).toBe('Something failed');
    expect(error.code).toBe(ApiErrorCode.UNKNOWN);
    expect(error.name).toBe('ApiError');
    expect(error.retryable).toBe(false);
  });

  it('creates error with status code', () => {
    const error = new ApiError('Not found', ApiErrorCode.NOT_FOUND, 404);
    expect(error.status).toBe(404);
  });

  it('creates retryable error', () => {
    const error = new ApiError('Retry me', ApiErrorCode.NETWORK_ERROR, undefined, true);
    expect(error.retryable).toBe(true);
  });
});

describe('NetworkError', () => {
  it('has NETWORK_ERROR code and is retryable', () => {
    const error = new NetworkError();
    expect(error.code).toBe(ApiErrorCode.NETWORK_ERROR);
    expect(error.retryable).toBe(true);
    expect(error.message).toBe('Server unreachable. Please check your connection.');
  });

  it('accepts custom message', () => {
    const error = new NetworkError('Custom network message');
    expect(error.message).toBe('Custom network message');
  });
});

describe('TimeoutError', () => {
  it('has TIMEOUT code and is retryable', () => {
    const error = new TimeoutError();
    expect(error.code).toBe(ApiErrorCode.TIMEOUT);
    expect(error.retryable).toBe(true);
    expect(error.message).toBe('Request timed out. Please try again.');
  });
});

describe('ServerError', () => {
  it('has SERVER_ERROR code and is retryable for 5xx', () => {
    const error = new ServerError(500);
    expect(error.code).toBe(ApiErrorCode.SERVER_ERROR);
    expect(error.status).toBe(500);
    expect(error.retryable).toBe(true);
  });

  it('is not retryable for 4xx server errors', () => {
    // Edge case: shouldn't happen but test the logic
    const error = new ServerError(400);
    expect(error.retryable).toBe(false);
  });
});

describe('ClientError', () => {
  it('has BAD_REQUEST code and is not retryable', () => {
    const error = new ClientError(400, 'Bad input');
    expect(error.code).toBe(ApiErrorCode.BAD_REQUEST);
    expect(error.status).toBe(400);
    expect(error.retryable).toBe(false);
    expect(error.message).toBe('Bad input');
  });
});

describe('NotFoundError', () => {
  it('has NOT_FOUND code and 404 status', () => {
    const error = new NotFoundError();
    expect(error.code).toBe(ApiErrorCode.NOT_FOUND);
    expect(error.status).toBe(404);
    expect(error.retryable).toBe(false);
  });
});

describe('responseToError', () => {
  it('returns ClientError for 400', () => {
    const response = { status: 400, statusText: 'Bad Request' } as Response;
    const error = responseToError(response, 'Invalid JSON');
    expect(error).toBeInstanceOf(ClientError);
    expect(error.status).toBe(400);
    expect(error.message).toBe('API error: Invalid JSON');
  });

  it('returns NotFoundError for 404', () => {
    const response = { status: 404, statusText: 'Not Found' } as Response;
    const error = responseToError(response);
    expect(error).toBeInstanceOf(NotFoundError);
    expect(error.status).toBe(404);
    expect(error.message).toBe('API error: 404 Not Found');
  });

  it('returns ServerError for 500', () => {
    const response = { status: 500, statusText: 'Internal Server Error' } as Response;
    const error = responseToError(response, 'DB error');
    expect(error).toBeInstanceOf(ServerError);
    expect(error.status).toBe(500);
    expect(error.message).toBe('API error: DB error');
  });

  it('returns generic ApiError for unknown status', () => {
    const response = { status: 418, statusText: "I'm a teapot" } as Response;
    const error = responseToError(response);
    expect(error).toBeInstanceOf(ApiError);
    expect(error.status).toBe(418);
  });
});

describe('getUserFriendlyMessage', () => {
  it('returns NetworkError message', () => {
    const error = new NetworkError();
    expect(getUserFriendlyMessage(error)).toBe('Server unreachable. Please check your connection.');
  });

  it('returns TimeoutError message', () => {
    const error = new TimeoutError();
    expect(getUserFriendlyMessage(error)).toBe('Request timed out. Please try again.');
  });

  it('returns generic Error message', () => {
    const error = new Error('Generic error');
    expect(getUserFriendlyMessage(error)).toBe('Generic error');
  });

  it('returns default message for non-Error values', () => {
    expect(getUserFriendlyMessage(null)).toBe('An unexpected error occurred. Please try again.');
    expect(getUserFriendlyMessage(42)).toBe('An unexpected error occurred. Please try again.');
  });
});
