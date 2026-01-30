/**
 * Smooth Scroll Utility Unit Tests.
 * Owner: Scenario 14 - Navigation and Smooth Scroll
 *
 * Tests:
 * - initSmoothScroll initializes smooth scroll behavior
 * - scrollToElement scrolls to target element
 * - scroll-behavior: smooth is applied to html element
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';

describe('Smooth Scroll Utility', () => {
  beforeEach(() => {
    // Reset modules
    vi.resetModules();

    // Reset DOM before each test
    document.documentElement.innerHTML = `
      <html>
        <head></head>
        <body>
          <nav id="navigation"></nav>
          <section id="features" style="height: 500px;">Features</section>
          <section id="getting-started" style="height: 500px;">Getting Started</section>
        </body>
      </html>
    `;

    // Clear window.location.hash
    Object.defineProperty(window, 'location', {
      writable: true,
      value: { hash: '', ...window.location },
    });
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('scroll-behavior CSS property', () => {
    it('should have scroll-behavior: smooth applied to html element', async () => {
      // Import and initialize the smooth scroll module
      const { initSmoothScroll } = await import('@/utils/smooth-scroll');
      initSmoothScroll();

      const htmlElement = document.documentElement;
      const computedStyle = window.getComputedStyle(htmlElement);

      // Check that scroll-behavior is set to smooth
      expect(computedStyle.scrollBehavior).toBe('smooth');
    });
  });

  describe('scrollToElement', () => {
    it('should scroll to the target element by ID', async () => {
      const { scrollToElement } = await import('@/utils/smooth-scroll');

      // Mock scrollIntoView
      const mockScrollIntoView = vi.fn();
      const targetElement = document.getElementById('features');
      if (targetElement) {
        targetElement.scrollIntoView = mockScrollIntoView;
      }

      scrollToElement('features');

      expect(mockScrollIntoView).toHaveBeenCalledWith({
        behavior: 'smooth',
        block: 'start',
      });
    });

    it('should not throw error when element does not exist', async () => {
      const { scrollToElement } = await import('@/utils/smooth-scroll');

      expect(() => scrollToElement('nonexistent')).not.toThrow();
    });

    it('should handle element ID with hash prefix', async () => {
      const { scrollToElement } = await import('@/utils/smooth-scroll');

      const mockScrollIntoView = vi.fn();
      const targetElement = document.getElementById('features');
      if (targetElement) {
        targetElement.scrollIntoView = mockScrollIntoView;
      }

      scrollToElement('#features');

      expect(mockScrollIntoView).toHaveBeenCalledWith({
        behavior: 'smooth',
        block: 'start',
      });
    });
  });

  describe('initSmoothScroll', () => {
    it('should set scroll-behavior on document element', async () => {
      const { initSmoothScroll } = await import('@/utils/smooth-scroll');

      initSmoothScroll();

      expect(document.documentElement.style.scrollBehavior).toBe('smooth');
    });

    it('should handle anchor links in the page', async () => {
      const { initSmoothScroll } = await import('@/utils/smooth-scroll');

      // Mock scrollIntoView before it gets used
      const mockScrollIntoView = vi.fn();

      // Create anchor link in document
      document.body.innerHTML = `
        <a href="#features" class="nav-link">Features</a>
        <section id="features">Features Section</section>
      `;

      const targetElement = document.getElementById('features');
      if (targetElement) {
        targetElement.scrollIntoView = mockScrollIntoView;
      }

      initSmoothScroll();

      const link = document.querySelector('a[href="#features"]') as HTMLAnchorElement;

      // Create and dispatch click event
      const clickEvent = new MouseEvent('click', {
        bubbles: true,
        cancelable: true,
      });

      link.dispatchEvent(clickEvent);

      expect(mockScrollIntoView).toHaveBeenCalledWith({
        behavior: 'smooth',
        block: 'start',
      });
    });
  });
});
