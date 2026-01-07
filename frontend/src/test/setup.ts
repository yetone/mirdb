import '@testing-library/jest-dom'

// Set up SEO meta tags from index.html for testing
document.title = 'URL Shortener - Shorten Links & Track Analytics'

// Add meta description tag
const metaDescription = document.createElement('meta')
metaDescription.setAttribute('name', 'description')
metaDescription.setAttribute('content', 'Shorten URLs and track clicks with our powerful URL shortener. Create short links, view analytics, and manage your links from one dashboard.')
document.head.appendChild(metaDescription)

// Set lang attribute on html element
document.documentElement.setAttribute('lang', 'en')

// Mock IntersectionObserver for framer-motion
class MockIntersectionObserver implements IntersectionObserver {
  readonly root: Element | Document | null = null
  readonly rootMargin: string = ''
  readonly thresholds: ReadonlyArray<number> = []

  constructor(
    private callback: IntersectionObserverCallback,
    _options?: IntersectionObserverInit
  ) {}

  observe(_target: Element): void {
    // Trigger callback immediately with a mock entry
    this.callback(
      [
        {
          isIntersecting: true,
          boundingClientRect: {} as DOMRectReadOnly,
          intersectionRatio: 1,
          intersectionRect: {} as DOMRectReadOnly,
          rootBounds: null,
          target: document.createElement('div'),
          time: Date.now(),
        },
      ],
      this
    )
  }

  unobserve(_target: Element): void {}

  disconnect(): void {}

  takeRecords(): IntersectionObserverEntry[] {
    return []
  }
}

global.IntersectionObserver = MockIntersectionObserver
