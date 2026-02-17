/**
 * Test setup file for Vitest.
 *
 * Configures testing environment with React Testing Library.
 */

import '@testing-library/jest-dom'
import { vi } from 'vitest'
import React from 'react'

// Mock framer-motion to avoid animation timing issues in tests
vi.mock('framer-motion', () => ({
  motion: {
    div: React.forwardRef(({ children, initial, animate, transition, whileHover, whileTap, ...props }: React.HTMLAttributes<HTMLDivElement> & Record<string, unknown>, ref: React.Ref<HTMLDivElement>) => {
      return React.createElement('div', { ...props, ref }, children)
    }),
    button: React.forwardRef(({ children, initial, animate, transition, whileHover, whileTap, ...props }: React.ButtonHTMLAttributes<HTMLButtonElement> & Record<string, unknown>, ref: React.Ref<HTMLButtonElement>) => {
      return React.createElement('button', { ...props, ref }, children)
    }),
  },
  AnimatePresence: ({ children }: { children: React.ReactNode }) => React.createElement(React.Fragment, null, children),
}))

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

// Mock localStorage
const localStorageMock = {
  getItem: vi.fn(),
  setItem: vi.fn(),
  removeItem: vi.fn(),
  clear: vi.fn(),
}
Object.defineProperty(window, 'localStorage', { value: localStorageMock })

// Reset all mocks before each test
beforeEach(() => {
  vi.clearAllMocks()
  localStorageMock.getItem.mockReturnValue(null)
})
