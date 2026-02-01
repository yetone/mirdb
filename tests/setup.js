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

// Mock clipboard API for copy functionality tests
Object.defineProperty(navigator, 'clipboard', {
  value: {
    writeText: jest.fn().mockImplementation(() => Promise.resolve()),
    readText: jest.fn().mockImplementation(() => Promise.resolve('')),
  },
  writable: true,
});

// Mock window.matchMedia for responsive tests
Object.defineProperty(window, 'matchMedia', {
  writable: true,
  value: jest.fn().mockImplementation(query => ({
    matches: false,
    media: query,
    onchange: null,
    addListener: jest.fn(),
    removeListener: jest.fn(),
    addEventListener: jest.fn(),
    removeEventListener: jest.fn(),
    dispatchEvent: jest.fn(),
  })),
});
