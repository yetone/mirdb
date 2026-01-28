/**
 * Test Setup and Configuration
 * Owner: First builder
 *
 * Global test setup for Vitest:
 * - @testing-library/jest-dom matchers
 * - Mock configurations
 * - Global test utilities
 */
import '@testing-library/jest-dom'
import React from 'react'

// Filter out framer-motion specific props
const filterMotionProps = (props: Record<string, unknown>) => {
  const motionPropPrefixes = ['while', 'animate', 'initial', 'exit', 'transition', 'variants', 'drag', 'viewport', 'onAnimation', 'onDrag', 'onHover', 'onTap', 'onPan']
  const filtered: Record<string, unknown> = {}
  for (const [key, value] of Object.entries(props)) {
    const isMotionProp = motionPropPrefixes.some(prefix => key.startsWith(prefix))
    if (!isMotionProp) {
      filtered[key] = value
    }
  }
  return filtered
}

// Mock framer-motion to avoid animation issues in tests
vi.mock('framer-motion', () => ({
  motion: {
    div: (props: Record<string, unknown>) =>
      React.createElement('div', filterMotionProps(props)),
    h1: (props: Record<string, unknown>) =>
      React.createElement('h1', filterMotionProps(props)),
    p: (props: Record<string, unknown>) =>
      React.createElement('p', filterMotionProps(props)),
    button: (props: Record<string, unknown>) =>
      React.createElement('button', filterMotionProps(props)),
    section: (props: Record<string, unknown>) =>
      React.createElement('section', filterMotionProps(props)),
  },
  AnimatePresence: ({ children }: { children: React.ReactNode }) => children,
}))

// Mock localStorage
const localStorageMock = {
  getItem: vi.fn(() => null),
  setItem: vi.fn(),
  removeItem: vi.fn(),
  clear: vi.fn(),
  length: 0,
  key: vi.fn(),
}
Object.defineProperty(window, 'localStorage', { value: localStorageMock })
