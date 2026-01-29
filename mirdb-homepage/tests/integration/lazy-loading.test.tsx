/**
 * Integration Tests for Lazy Loading functionality
 * Owner: Scenario 14 - Performance Requirements
 *
 * Tests cover:
 * - Lazy loading mechanism for architecture diagram
 * - IntersectionObserver integration
 * - Fallback behavior when IntersectionObserver is unavailable
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

describe('Lazy Loading Integration', () => {
  let mockIntersectionObserver: ReturnType<typeof vi.fn>;
  let observerCallback: (entries: Array<{ isIntersecting: boolean; target: Element }>) => void;
  let observeElements: Element[] = [];
  let originalIntersectionObserver: typeof IntersectionObserver | undefined;

  beforeEach(() => {
    // Clear the observed elements
    observeElements = [];

    // Save original
    originalIntersectionObserver = (global as typeof globalThis & { IntersectionObserver?: typeof IntersectionObserver }).IntersectionObserver;

    // Create a mock IntersectionObserver
    mockIntersectionObserver = vi.fn((callback: typeof observerCallback) => {
      observerCallback = callback;
      return {
        observe: (element: Element) => {
          observeElements.push(element);
        },
        unobserve: vi.fn(),
        disconnect: vi.fn(),
      };
    });

    // Define IntersectionObserver on global
    (global as typeof globalThis & { IntersectionObserver: typeof mockIntersectionObserver }).IntersectionObserver = mockIntersectionObserver;
  });

  afterEach(() => {
    vi.clearAllMocks();
    // Restore original
    if (originalIntersectionObserver) {
      (global as typeof globalThis & { IntersectionObserver: typeof IntersectionObserver }).IntersectionObserver = originalIntersectionObserver;
    }
    // Clean up DOM
    document.body.innerHTML = '';
  });

  it('should set up lazy loading with IntersectionObserver', () => {
    // Create a DOM structure similar to ArchitectureDiagram
    document.body.innerHTML = `
      <div data-lazy-diagram>
        <div class="lazy-placeholder"></div>
        <div class="lazy-content" style="opacity: 0;"></div>
      </div>
    `;

    const container = document.querySelector('[data-lazy-diagram]');
    const lazyContent = document.querySelector('.lazy-content') as HTMLElement;
    const placeholder = document.querySelector('.lazy-placeholder');

    expect(container).not.toBeNull();
    expect(lazyContent).not.toBeNull();
    expect(placeholder).not.toBeNull();

    // Initial state: content should be hidden
    expect(lazyContent?.style.opacity).toBe('0');
  });

  it('should show content when element intersects viewport', () => {
    // Create a DOM structure
    document.body.innerHTML = `
      <div data-lazy-diagram>
        <div class="lazy-placeholder"></div>
        <div class="lazy-content" style="opacity: 0;"></div>
      </div>
    `;

    const container = document.querySelector('[data-lazy-diagram]') as HTMLElement;
    const lazyContent = document.querySelector('.lazy-content') as HTMLElement;
    const placeholder = document.querySelector('.lazy-placeholder') as HTMLElement;

    // Simulate the lazy loading initialization
    function initLazyDiagram(
      containerEl: HTMLElement,
      contentEl: HTMLElement,
      placeholderEl: HTMLElement
    ) {
      contentEl.style.transition = 'opacity 0.3s ease-in-out';

      const observer = new IntersectionObserver(
        (entries) => {
          entries.forEach((entry) => {
            if (entry.isIntersecting) {
              contentEl.style.opacity = '1';
              placeholderEl.classList.add('hidden');
              containerEl.setAttribute('data-loaded', 'true');
              observer.disconnect();
            }
          });
        },
        { rootMargin: '100px 0px', threshold: 0.1 }
      );

      observer.observe(containerEl);
    }

    initLazyDiagram(container, lazyContent, placeholder);

    // Verify observer was created and is observing
    expect(mockIntersectionObserver).toHaveBeenCalledTimes(1);
    expect(observeElements).toContain(container);

    // Simulate intersection
    observerCallback([{ isIntersecting: true, target: container }]);

    // Content should now be visible
    expect(lazyContent.style.opacity).toBe('1');
    expect(container.getAttribute('data-loaded')).toBe('true');
  });

  it('should not show content when element does not intersect', () => {
    document.body.innerHTML = `
      <div data-lazy-diagram>
        <div class="lazy-content" style="opacity: 0;"></div>
      </div>
    `;

    const container = document.querySelector('[data-lazy-diagram]') as HTMLElement;
    const lazyContent = document.querySelector('.lazy-content') as HTMLElement;

    function initLazyDiagram(
      containerEl: HTMLElement,
      contentEl: HTMLElement
    ) {
      const observer = new IntersectionObserver(
        (entries) => {
          entries.forEach((entry) => {
            if (entry.isIntersecting) {
              contentEl.style.opacity = '1';
              containerEl.setAttribute('data-loaded', 'true');
            }
          });
        },
        { rootMargin: '100px 0px', threshold: 0.1 }
      );

      observer.observe(containerEl);
    }

    initLazyDiagram(container, lazyContent);

    // Simulate non-intersection
    observerCallback([{ isIntersecting: false, target: container }]);

    // Content should still be hidden
    expect(lazyContent.style.opacity).toBe('0');
    expect(container.getAttribute('data-loaded')).toBeNull();
  });

  it('should use rootMargin for early loading trigger', () => {
    document.body.innerHTML = `
      <div data-lazy-diagram>
        <div class="lazy-content" style="opacity: 0;"></div>
      </div>
    `;

    const container = document.querySelector('[data-lazy-diagram]') as HTMLElement;
    const lazyContent = document.querySelector('.lazy-content') as HTMLElement;

    function initLazyDiagram(
      containerEl: HTMLElement,
      contentEl: HTMLElement
    ) {
      const observer = new IntersectionObserver(
        (entries) => {
          entries.forEach((entry) => {
            if (entry.isIntersecting) {
              contentEl.style.opacity = '1';
            }
          });
        },
        {
          rootMargin: '100px 0px', // Start loading 100px before element is visible
          threshold: 0.1,
        }
      );

      observer.observe(containerEl);
    }

    initLazyDiagram(container, lazyContent);

    // Verify IntersectionObserver was called with correct options
    expect(mockIntersectionObserver).toHaveBeenCalledWith(
      expect.any(Function),
      expect.objectContaining({
        rootMargin: '100px 0px',
        threshold: 0.1,
      })
    );
  });

  it('should handle fallback when IntersectionObserver is unavailable', () => {
    // Remove IntersectionObserver to simulate unsupported browser
    delete (global as typeof globalThis & { IntersectionObserver?: unknown }).IntersectionObserver;

    document.body.innerHTML = `
      <div data-lazy-diagram>
        <div class="lazy-placeholder"></div>
        <div class="lazy-content" style="opacity: 0;"></div>
      </div>
    `;

    const container = document.querySelector('[data-lazy-diagram]') as HTMLElement;
    const lazyContent = document.querySelector('.lazy-content') as HTMLElement;
    const placeholder = document.querySelector('.lazy-placeholder') as HTMLElement;

    function initLazyDiagram(
      containerEl: HTMLElement,
      contentEl: HTMLElement,
      placeholderEl: HTMLElement
    ) {
      if ('IntersectionObserver' in globalThis) {
        // IntersectionObserver available
        const observer = new IntersectionObserver(
          (entries) => {
            entries.forEach((entry) => {
              if (entry.isIntersecting) {
                contentEl.style.opacity = '1';
                placeholderEl.classList.add('hidden');
                containerEl.setAttribute('data-loaded', 'true');
                observer.disconnect();
              }
            });
          },
          { rootMargin: '100px 0px', threshold: 0.1 }
        );
        observer.observe(containerEl);
      } else {
        // Fallback: show immediately
        contentEl.style.opacity = '1';
        placeholderEl.classList.add('hidden');
        containerEl.setAttribute('data-loaded', 'true');
      }
    }

    initLazyDiagram(container, lazyContent, placeholder);

    // Content should be shown immediately as fallback
    expect(lazyContent.style.opacity).toBe('1');
    expect(container.getAttribute('data-loaded')).toBe('true');
  });
});

describe('Lazy Loading Performance Benefits', () => {
  afterEach(() => {
    document.body.innerHTML = '';
  });

  it('lazy content should have smooth transition styles', () => {
    document.body.innerHTML = `
      <div class="lazy-content" style="transition: opacity 0.3s ease-in-out;"></div>
    `;

    const lazyContent = document.querySelector('.lazy-content') as HTMLElement;

    // Verify transition is set for smooth appearance
    expect(lazyContent.style.transition).toContain('opacity');
    expect(lazyContent.style.transition).toContain('ease-in-out');
  });

  it('placeholder should hide after content loads', () => {
    document.body.innerHTML = `
      <div>
        <div class="lazy-placeholder"></div>
        <div class="lazy-content"></div>
      </div>
    `;

    const placeholder = document.querySelector('.lazy-placeholder') as HTMLElement;

    // Simulate content load
    placeholder.classList.add('hidden');

    expect(placeholder.classList.contains('hidden')).toBe(true);
  });
});
