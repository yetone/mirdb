/**
 * Test Setup Configuration
 * Owner: First builder (shared resource)
 *
 * Global test setup for Vitest.
 *
 * Includes:
 * - @testing-library/jest-dom matchers
 * - Mock for localStorage
 * - Mock for clipboard API
 * - Test environment configuration
 */

import '@testing-library/jest-dom/vitest'
import { afterEach, beforeAll, vi } from 'vitest'
import { cleanup } from '@testing-library/react'

// Cleanup after each test
afterEach(() => {
  cleanup()
})

// Mock localStorage
const localStorageMock = {
  getItem: vi.fn(),
  setItem: vi.fn(),
  removeItem: vi.fn(),
  clear: vi.fn(),
  length: 0,
  key: vi.fn(),
}
Object.defineProperty(window, 'localStorage', { value: localStorageMock })

// Note: Don't mock clipboard API here as it conflicts with @testing-library/user-event
// Tests that need clipboard mocking should handle it individually

// Mock matchMedia
Object.defineProperty(window, 'matchMedia', {
  writable: true,
  value: vi.fn().mockImplementation((query: string) => ({
    matches: false,
    media: query,
    onchange: null,
    addListener: vi.fn(),
    removeListener: vi.fn(),
    addEventListener: vi.fn(),
    removeEventListener: vi.fn(),
    dispatchEvent: vi.fn(),
  })),
})

// Reset mocks before all tests
beforeAll(() => {
  vi.clearAllMocks()
})
