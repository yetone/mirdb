/**
 * Test setup utilities for homepage tests
 *
 * This file provides common setup for all homepage component tests.
 */

import '@testing-library/jest-dom';

// Note: Clipboard API is mocked by @testing-library/user-event
// Do not override navigator.clipboard here as it conflicts with user-event's internal handling

// Global test utilities
export const mockApiResponse = <T>(data: T, delay = 0): Promise<T> => {
  return new Promise((resolve) => {
    setTimeout(() => resolve(data), delay);
  });
};

export const mockApiError = (message: string, delay = 0): Promise<never> => {
  return new Promise((_, reject) => {
    setTimeout(() => reject(new Error(message)), delay);
  });
};

// Reset mocks before each test
beforeEach(() => {
  vi.clearAllMocks();
});
