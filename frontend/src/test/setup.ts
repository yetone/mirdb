import '@testing-library/jest-dom'

// Mock IntersectionObserver for framer-motion viewport animations
class MockIntersectionObserver implements IntersectionObserver {
  readonly root: Element | null = null
  readonly rootMargin: string = ''
  readonly thresholds: readonly number[] = []

  constructor(callback: IntersectionObserverCallback) {
    // Immediately call callback with entries that are all intersecting
    setTimeout(() => {
      const entries = [{
        isIntersecting: true,
        boundingClientRect: {} as DOMRectReadOnly,
        intersectionRatio: 1,
        intersectionRect: {} as DOMRectReadOnly,
        rootBounds: null,
        target: document.body,
        time: Date.now(),
      }] as IntersectionObserverEntry[]
      callback(entries, this)
    }, 0)
  }

  disconnect(): void {}
  observe(): void {}
  unobserve(): void {}
  takeRecords(): IntersectionObserverEntry[] {
    return []
  }
}

global.IntersectionObserver = MockIntersectionObserver

// Mock ResizeObserver
class MockResizeObserver implements ResizeObserver {
  observe(): void {}
  unobserve(): void {}
  disconnect(): void {}
}

global.ResizeObserver = MockResizeObserver

// Mock clipboard API
Object.defineProperty(navigator, 'clipboard', {
  value: {
    writeText: async () => Promise.resolve(),
    readText: async () => Promise.resolve(''),
  },
  writable: true,
  configurable: true,
})
