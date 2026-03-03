/**
 * Test Setup Configuration
 *
 * This file is created by the first scenario builder and
 * should set up the testing environment.
 *
 * Expected setup:
 * - Mock providers (ThemeContext, AuthContext)
 * - Testing library configuration
 * - Global test utilities
 * - Mock implementations for API
 */
import '@testing-library/jest-dom'
import { vi } from 'vitest'

// Mock window.matchMedia for theme tests
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

// Mock clipboard API - configurable allows userEvent to override
Object.defineProperty(navigator, 'clipboard', {
  value: {
    writeText: vi.fn().mockResolvedValue(undefined),
    readText: vi.fn().mockResolvedValue(''),
  },
  writable: true,
  configurable: true,
})
