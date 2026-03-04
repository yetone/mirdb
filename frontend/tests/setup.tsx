/**
 * Test Setup File
 *
 * Configures the testing environment with required matchers and mocks.
 */
import React from 'react'
import '@testing-library/jest-dom'
import { afterEach, vi } from 'vitest'
import { cleanup } from '@testing-library/react'

// Cleanup after each test
afterEach(() => {
  cleanup()
})

// Mock Framer Motion to render immediately without animations
vi.mock('framer-motion', async () => {
  const actual = await vi.importActual('framer-motion')
  return {
    ...actual,
    motion: {
      div: ({ children, ...props }: React.HTMLAttributes<HTMLDivElement>) => {
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        const { initial, animate, variants, whileHover, whileTap, ...rest } = props as Record<string, unknown>
        return <div {...rest}>{children}</div>
      },
      h1: ({ children, ...props }: React.HTMLAttributes<HTMLHeadingElement>) => {
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        const { initial, animate, variants, whileHover, whileTap, ...rest } = props as Record<string, unknown>
        return <h1 {...rest}>{children}</h1>
      },
      p: ({ children, ...props }: React.HTMLAttributes<HTMLParagraphElement>) => {
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        const { initial, animate, variants, whileHover, whileTap, ...rest } = props as Record<string, unknown>
        return <p {...rest}>{children}</p>
      },
      button: ({ children, ...props }: React.ButtonHTMLAttributes<HTMLButtonElement>) => {
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        const { initial, animate, variants, whileHover, whileTap, ...rest } = props as Record<string, unknown>
        return <button {...rest}>{children}</button>
      },
    },
  }
})

// Mock matchMedia for responsive tests
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
})

// Mock IntersectionObserver for animations
class MockIntersectionObserver {
  observe = () => null
  disconnect = () => null
  unobserve = () => null
}

Object.defineProperty(window, 'IntersectionObserver', {
  writable: true,
  value: MockIntersectionObserver,
})

// Mock scrollTo
window.scrollTo = () => {}
