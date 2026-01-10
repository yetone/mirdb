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

// Mock IntersectionObserver for Framer Motion's whileInView
// jsdom doesn't implement IntersectionObserver
class MockIntersectionObserver implements IntersectionObserver {
  readonly root: Element | Document | null = null
  readonly rootMargin: string = ''
  readonly thresholds: ReadonlyArray<number> = []

  constructor(
    private callback: IntersectionObserverCallback,
    _options?: IntersectionObserverInit
  ) {
    // Immediately trigger callback with all observed elements as intersecting
    // This simulates elements being in view for testing purposes
  }

  observe(target: Element): void {
    // Immediately trigger the callback indicating the element is in view
    const entry: IntersectionObserverEntry = {
      boundingClientRect: target.getBoundingClientRect(),
      intersectionRatio: 1,
      intersectionRect: target.getBoundingClientRect(),
      isIntersecting: true,
      rootBounds: null,
      target,
      time: Date.now(),
    }
    this.callback([entry], this)
  }

  unobserve(_target: Element): void {
    // No-op for tests
  }

  disconnect(): void {
    // No-op for tests
  }

  takeRecords(): IntersectionObserverEntry[] {
    return []
  }
}

// Assign the mock to the global window object
Object.defineProperty(window, 'IntersectionObserver', {
  writable: true,
  configurable: true,
  value: MockIntersectionObserver,
})

Object.defineProperty(global, 'IntersectionObserver', {
  writable: true,
  configurable: true,
  value: MockIntersectionObserver,
})
