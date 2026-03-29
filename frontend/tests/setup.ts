/**
 * Test Setup
 * Owner: First scenario builder
 *
 * Global test setup for Vitest:
 * - Import testing-library matchers
 * - Mock window.matchMedia for theme tests
 * - Configure JSDOM environment
 */
import '@testing-library/jest-dom';

// Mock window.matchMedia for theme tests
Object.defineProperty(window, 'matchMedia', {
  writable: true,
  value: (query: string) => ({
    matches: false,
    media: query,
    onchange: null,
    addListener: () => {},
    removeListener: () => {},
    addEventListener: () => {},
    removeEventListener: () => {},
    dispatchEvent: () => false,
  }),
});

// Mock scrollTo for smooth scroll tests
Object.defineProperty(window, 'scrollTo', {
  writable: true,
  value: () => {},
});
