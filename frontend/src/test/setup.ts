import '@testing-library/jest-dom'

// Mock IntersectionObserver for Framer Motion's whileInView
class MockIntersectionObserver implements IntersectionObserver {
  readonly root: Element | null = null
  readonly rootMargin: string = ''
  readonly thresholds: ReadonlyArray<number> = []

  constructor(
    private callback: IntersectionObserverCallback,
    _options?: IntersectionObserverInit
  ) {}

  observe(_target: Element): void {
    // Immediately trigger with isIntersecting: true for all observed elements
    const entries: IntersectionObserverEntry[] = [
      {
        isIntersecting: true,
        boundingClientRect: {} as DOMRectReadOnly,
        intersectionRatio: 1,
        intersectionRect: {} as DOMRectReadOnly,
        rootBounds: null,
        target: document.createElement('div'),
        time: Date.now(),
      },
    ]
    this.callback(entries, this)
  }

  unobserve(_target: Element): void {}
  disconnect(): void {}
  takeRecords(): IntersectionObserverEntry[] {
    return []
  }
}

// @ts-expect-error - IntersectionObserver mock for testing
global.IntersectionObserver = MockIntersectionObserver
