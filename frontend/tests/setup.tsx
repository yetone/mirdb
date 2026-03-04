/**
 * Test Setup File
 *
 * Configures the testing environment with required matchers and mocks.
 */
import React from 'react'
import '@testing-library/jest-dom'
import { afterEach, vi, expect } from 'vitest'
import { cleanup } from '@testing-library/react'
import * as axeMatchers from 'vitest-axe/matchers'

// Extend expect with axe-core matchers
expect.extend(axeMatchers)

// Cleanup after each test
afterEach(() => {
  cleanup()
})

// Helper function to filter framer-motion specific props
const filterMotionProps = (props: Record<string, unknown>) => {
  const {
    initial, animate, variants, whileHover, whileTap, whileInView,
    viewport, transition, exit, onAnimationComplete,
    ...rest
  } = props
  return rest
}

// Mock Framer Motion to render immediately without animations
vi.mock('framer-motion', async () => {
  const actual = await vi.importActual('framer-motion')
  return {
    ...actual,
    AnimatePresence: ({ children }: { children: React.ReactNode }) => <>{children}</>,
    motion: {
      div: React.forwardRef<HTMLDivElement, React.HTMLAttributes<HTMLDivElement>>(
        ({ children, ...props }, ref) => {
          const rest = filterMotionProps(props as Record<string, unknown>)
          return <div ref={ref} {...rest}>{children}</div>
        }
      ),
      nav: React.forwardRef<HTMLElement, React.HTMLAttributes<HTMLElement>>(
        ({ children, ...props }, ref) => {
          const rest = filterMotionProps(props as Record<string, unknown>)
          return <nav ref={ref} {...rest}>{children}</nav>
        }
      ),
      section: React.forwardRef<HTMLElement, React.HTMLAttributes<HTMLElement>>(
        ({ children, ...props }, ref) => {
          const rest = filterMotionProps(props as Record<string, unknown>)
          return <section ref={ref} {...rest}>{children}</section>
        }
      ),
      main: React.forwardRef<HTMLElement, React.HTMLAttributes<HTMLElement>>(
        ({ children, ...props }, ref) => {
          const rest = filterMotionProps(props as Record<string, unknown>)
          return <main ref={ref} {...rest}>{children}</main>
        }
      ),
      h1: React.forwardRef<HTMLHeadingElement, React.HTMLAttributes<HTMLHeadingElement>>(
        ({ children, ...props }, ref) => {
          const rest = filterMotionProps(props as Record<string, unknown>)
          return <h1 ref={ref} {...rest}>{children}</h1>
        }
      ),
      h2: React.forwardRef<HTMLHeadingElement, React.HTMLAttributes<HTMLHeadingElement>>(
        ({ children, ...props }, ref) => {
          const rest = filterMotionProps(props as Record<string, unknown>)
          return <h2 ref={ref} {...rest}>{children}</h2>
        }
      ),
      p: React.forwardRef<HTMLParagraphElement, React.HTMLAttributes<HTMLParagraphElement>>(
        ({ children, ...props }, ref) => {
          const rest = filterMotionProps(props as Record<string, unknown>)
          return <p ref={ref} {...rest}>{children}</p>
        }
      ),
      button: React.forwardRef<HTMLButtonElement, React.ButtonHTMLAttributes<HTMLButtonElement>>(
        ({ children, ...props }, ref) => {
          const rest = filterMotionProps(props as Record<string, unknown>)
          return <button ref={ref} {...rest}>{children}</button>
        }
      ),
      span: React.forwardRef<HTMLSpanElement, React.HTMLAttributes<HTMLSpanElement>>(
        ({ children, ...props }, ref) => {
          const rest = filterMotionProps(props as Record<string, unknown>)
          return <span ref={ref} {...rest}>{children}</span>
        }
      ),
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
