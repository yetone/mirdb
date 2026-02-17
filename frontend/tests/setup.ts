import '@testing-library/jest-dom'
import { vi } from 'vitest'
import React from 'react'

// Mock framer-motion to avoid animation timing issues in tests
vi.mock('framer-motion', () => ({
  motion: {
    div: React.forwardRef(({ children, initial, animate, transition, whileHover, whileTap, ...props }: any, ref: any) => {
      return React.createElement('div', { ...props, ref }, children)
    }),
    button: React.forwardRef(({ children, initial, animate, transition, whileHover, whileTap, ...props }: any, ref: any) => {
      return React.createElement('button', { ...props, ref }, children)
    }),
    section: React.forwardRef(({ children, initial, animate, transition, whileHover, whileTap, ...props }: any, ref: any) => {
      return React.createElement('section', { ...props, ref }, children)
    }),
    p: React.forwardRef(({ children, initial, animate, transition, whileHover, whileTap, ...props }: any, ref: any) => {
      return React.createElement('p', { ...props, ref }, children)
    }),
    span: React.forwardRef(({ children, initial, animate, transition, whileHover, whileTap, ...props }: any, ref: any) => {
      return React.createElement('span', { ...props, ref }, children)
    }),
  },
  AnimatePresence: ({ children }: any) => React.createElement(React.Fragment, null, children),
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

// Mock Clipboard API
const mockClipboard = {
  writeText: vi.fn(() => Promise.resolve()),
  readText: vi.fn(() => Promise.resolve('')),
}

Object.defineProperty(navigator, 'clipboard', {
  value: mockClipboard,
  writable: true,
  configurable: true,
})

// Helper to access clipboard mock in tests
;(global as any).mockClipboard = mockClipboard
