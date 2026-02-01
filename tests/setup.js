/**
 * Test setup file for all tests.
 * Owner: First Builder
 *
 * Setup includes:
 * - Jest DOM matchers
 * - Common test utilities
 * - Mock configurations
 */
require('@testing-library/jest-dom');

// Global test timeout
jest.setTimeout(10000);

// Mock console.error to prevent noise in tests (optional)
// const originalError = console.error;
// beforeAll(() => {
//   console.error = jest.fn();
// });
// afterAll(() => {
//   console.error = originalError;
// });
