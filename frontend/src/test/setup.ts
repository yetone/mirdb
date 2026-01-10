import '@testing-library/jest-dom'
import * as vitestAxeMatchers from 'vitest-axe/matchers'
import { expect } from 'vitest'

expect.extend(vitestAxeMatchers)

// Mock window.matchMedia for all tests
// This is needed because jsdom doesn't implement matchMedia
Object.defineProperty(window, 'matchMedia', {
  writable: true,
  value: (query: string) => ({
    matches: false, // Default to no reduced motion preference
    media: query,
    onchange: null,
    addListener: () => {},
    removeListener: () => {},
    addEventListener: () => {},
    removeEventListener: () => {},
    dispatchEvent: () => false,
  }),
})
