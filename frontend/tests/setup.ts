/**
 * Test setup file for Vitest with React Testing Library
 *
 * This file is executed before each test file and sets up:
 * - Jest-DOM matchers for DOM assertions
 * - Global cleanup after each test
 * - Mock for window.matchMedia (not available in jsdom)
 */
import '@testing-library/jest-dom';
import { cleanup } from '@testing-library/react';
import { afterEach, vi } from 'vitest';

// Mock window.matchMedia - not implemented in jsdom
Object.defineProperty(window, 'matchMedia', {
  writable: true,
  value: vi.fn().mockImplementation((query: string) => ({
    matches: false,
    media: query,
    onchange: null,
    addListener: vi.fn(), // deprecated
    removeListener: vi.fn(), // deprecated
    addEventListener: vi.fn(),
    removeEventListener: vi.fn(),
    dispatchEvent: vi.fn(),
  })),
});

// Cleanup after each test case
afterEach(() => {
  cleanup();
});
