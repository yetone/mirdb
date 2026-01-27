/**
 * Test setup file for Vitest.
 *
 * Configures:
 * - @testing-library/jest-dom matchers
 * - Mock for window.matchMedia (for responsive tests)
 * - Mock for IntersectionObserver (for scroll animations)
 * - Mock for localStorage (for theme persistence)
 *
 * This file is referenced in vitest.config.ts setupFiles
 */

import '@testing-library/jest-dom'

// Mock window.matchMedia for responsive tests
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

// Mock IntersectionObserver for scroll animations
class MockIntersectionObserver implements IntersectionObserver {
  readonly root: Element | Document | null = null
  readonly rootMargin: string = ''
  readonly thresholds: ReadonlyArray<number> = []

  private callback: IntersectionObserverCallback

  constructor(callback: IntersectionObserverCallback, _options?: IntersectionObserverInit) {
    this.callback = callback
  }

  observe = vi.fn((target: Element) => {
    // Immediately trigger callback with isIntersecting: true for testing
    this.callback([{
      isIntersecting: true,
      target,
      boundingClientRect: {} as DOMRectReadOnly,
      intersectionRatio: 1,
      intersectionRect: {} as DOMRectReadOnly,
      rootBounds: null,
      time: Date.now(),
    }], this)
  })

  unobserve = vi.fn()
  disconnect = vi.fn()
  takeRecords = vi.fn(() => [] as IntersectionObserverEntry[])
}

window.IntersectionObserver = MockIntersectionObserver as unknown as typeof IntersectionObserver

// Mock localStorage
const localStorageMock = {
  getItem: vi.fn(),
  setItem: vi.fn(),
  removeItem: vi.fn(),
  clear: vi.fn(),
}
Object.defineProperty(window, 'localStorage', {
  value: localStorageMock,
})
