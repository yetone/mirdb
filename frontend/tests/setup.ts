import '@testing-library/jest-dom'
import { cleanup } from '@testing-library/react'
import { afterEach, beforeEach, vi } from 'vitest'

// Reset document theme attribute before each test
beforeEach(() => {
  document.documentElement.removeAttribute('data-theme')
  localStorage.clear()
})

// Cleanup after each test
afterEach(() => {
  cleanup()
  document.documentElement.removeAttribute('data-theme')
})

// Mock window.matchMedia for theme tests
Object.defineProperty(window, 'matchMedia', {
  writable: true,
  value: vi.fn().mockImplementation(query => ({
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
