/**
 * Maps API errors to user-friendly messages.
 * Owner: Scenario 9 - Error Handling and Edge Cases
 *
 * Expected exports:
 * - mapApiError(error: unknown): { message: string; retryable: boolean }
 *   handles network failure, 400/429/5xx codes
 */

export interface ApiErrorResult {
  message: string;
  retryable: boolean;
}

/**
 * Maps various API / network errors into a consistent, user-friendly
 * message and a flag indicating whether the user should be allowed to retry.
 */
export function mapApiError(error: unknown): ApiErrorResult {
  if (error instanceof Response) {
    const status = error.status;

    if (status === 400) {
      return {
        message: 'Please enter a valid URL',
        retryable: true,
      };
    }

    if (status === 401 || status === 403) {
      return {
        message: 'You are not authorized to perform this action. Please sign in and try again.',
        retryable: true,
      };
    }

    if (status === 429) {
      return {
        message: 'Too many requests. Please wait a moment and try again.',
        retryable: true,
      };
    }

    if (status >= 500 && status < 600) {
      return {
        message: 'Something went wrong on our end. Please try again in a moment.',
        retryable: true,
      };
    }

    return {
      message: 'An unexpected error occurred. Please try again.',
      retryable: true,
    };
  }

  if (error instanceof TypeError) {
    const msg = error.message.toLowerCase();
    if (
      msg.includes('fetch') ||
      msg.includes('network') ||
      msg.includes('failed to fetch') ||
      msg.includes('internet') ||
      msg.includes('cors')
    ) {
      return {
        message: 'Unable to connect to the server. Please check your internet connection and try again.',
        retryable: true,
      };
    }
  }

  if (error instanceof Error) {
    const msg = error.message.toLowerCase();

    if (
      msg.includes('fetch') ||
      msg.includes('network') ||
      msg.includes('failed to fetch') ||
      msg.includes('internet')
    ) {
      return {
        message: 'Unable to connect to the server. Please check your internet connection and try again.',
        retryable: true,
      };
    }

    if (msg.includes('timeout') || msg.includes('abort')) {
      return {
        message: 'The request timed out. Please check your connection and try again.',
        retryable: true,
      };
    }

    if (msg.startsWith('http ')) {
      const statusMatch = msg.match(/http (\d+)/);
      if (statusMatch) {
        const status = parseInt(statusMatch[1], 10);

        if (status === 400) {
          return {
            message: 'Please enter a valid URL',
            retryable: true,
          };
        }

        if (status === 429) {
          return {
            message: 'Too many requests. Please wait a moment and try again.',
            retryable: true,
          };
        }

        if (status >= 500) {
          return {
            message: 'Something went wrong on our end. Please try again in a moment.',
            retryable: true,
          };
        }
      }
    }

    return {
      message: 'An unexpected error occurred. Please try again.',
      retryable: true,
    };
  }

  return {
    message: 'An unexpected error occurred. Please try again.',
    retryable: true,
  };
}
