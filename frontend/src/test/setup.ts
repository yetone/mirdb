import '@testing-library/jest-dom'
import 'vitest-axe/extend-expect'
import * as matchers from 'vitest-axe/matchers'
import { expect } from 'vitest'

expect.extend(matchers)

// Mock IntersectionObserver for framer-motion's whileInView
class MockIntersectionObserver implements IntersectionObserver {
  root: Document | Element | null = null
  rootMargin: string = ''
  thresholds: ReadonlyArray<number> = []

  constructor(
    private callback: IntersectionObserverCallback,
    _options?: IntersectionObserverInit
  ) {
    // Immediately call the callback with all observed elements as intersecting
    // This simulates elements being immediately visible
  }

  observe(target: Element): void {
    // Simulate immediate intersection
    const entry: IntersectionObserverEntry = {
      isIntersecting: true,
      boundingClientRect: target.getBoundingClientRect(),
      intersectionRatio: 1,
      intersectionRect: target.getBoundingClientRect(),
      rootBounds: null,
      target,
      time: Date.now(),
    }
    this.callback([entry], this)
  }

  unobserve(): void {}
  disconnect(): void {}
  takeRecords(): IntersectionObserverEntry[] {
    return []
  }
}

global.IntersectionObserver = MockIntersectionObserver
