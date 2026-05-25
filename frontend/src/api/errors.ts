/**
 * API Error types and utilities.
 * Owner: Scenario 17 - Error Handling and Graceful Degradation
 *
 * Provides structured error classes for different API failure modes.
 * Covers NFR-4 (clear error feedback).
 */

export enum ApiErrorCode {
  NETWORK_ERROR = 'NETWORK_ERROR',
  TIMEOUT = 'TIMEOUT',
  SERVER_ERROR = 'SERVER_ERROR',
  BAD_REQUEST = 'BAD_REQUEST',
  NOT_FOUND = 'NOT_FOUND',
  PARSE_ERROR = 'PARSE_ERROR',
  UNKNOWN = 'UNKNOWN',
}

export class ApiError extends Error {
  public readonly code: ApiErrorCode;
  public readonly status?: number;
  public readonly retryable: boolean;

  constructor(
    message: string,
    code: ApiErrorCode = ApiErrorCode.UNKNOWN,
    status?: number,
    retryable = false
  ) {
    super(message);
    this.name = 'ApiError';
    this.code = code;
    this.status = status;
    this.retryable = retryable;

    if ((Error as unknown as { captureStackTrace?: (obj: Error, ctor: unknown) => void }).captureStackTrace) {
      (Error as unknown as { captureStackTrace: (obj: Error, ctor: unknown) => void }).captureStackTrace(this, ApiError);
    }
  }
}

export class NetworkError extends ApiError {
  constructor(message = 'Server unreachable. Please check your connection.') {
    super(message, ApiErrorCode.NETWORK_ERROR, undefined, true);
    this.name = 'NetworkError';
  }
}

export class TimeoutError extends ApiError {
  constructor(message = 'Request timed out. Please try again.') {
    super(message, ApiErrorCode.TIMEOUT, undefined, true);
    this.name = 'TimeoutError';
  }
}

export class ServerError extends ApiError {
  constructor(status: number, message = `Server error (${status})`) {
    super(message, ApiErrorCode.SERVER_ERROR, status, status >= 500);
    this.name = 'ServerError';
  }
}

export class ClientError extends ApiError {
  constructor(status: number, message: string) {
    super(message, ApiErrorCode.BAD_REQUEST, status, false);
    this.name = 'ClientError';
  }
}

export class NotFoundError extends ApiError {
  constructor(message = 'Resource not found.') {
    super(message, ApiErrorCode.NOT_FOUND, 404, false);
    this.name = 'NotFoundError';
  }
}

/**
 * Map a fetch response to an appropriate ApiError.
 * Preserves backward-compatible 'API error: NNN ...' message format.
 */
export function responseToError(response: Response, bodyText?: string): ApiError {
  const rawMessage = bodyText || `${response.status} ${response.statusText}`;
  const message = `API error: ${rawMessage}`;

  if (response.status === 400) {
    return new ClientError(400, message);
  }
  if (response.status === 404) {
    return new NotFoundError(message);
  }
  if (response.status >= 500) {
    return new ServerError(response.status, message);
  }
  return new ApiError(message, ApiErrorCode.UNKNOWN, response.status);
}

/**
 * Get a user-friendly message for any error.
 */
export function getUserFriendlyMessage(error: unknown): string {
  if (error instanceof NetworkError) {
    return error.message;
  }
  if (error instanceof TimeoutError) {
    return error.message;
  }
  if (error instanceof ApiError) {
    return error.message;
  }
  if (error instanceof Error) {
    return error.message;
  }
  return 'An unexpected error occurred. Please try again.';
}
