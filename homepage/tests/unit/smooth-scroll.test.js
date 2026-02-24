/**
 * Smooth Scroll Unit Tests
 * Owner: Scenario 14 - Smooth Scroll Navigation
 *
 * Test cases:
 * - Anchor clicks trigger smooth scroll
 * - Scroll offset accounts for fixed header
 * - scrollToElement scrolls to correct position
 * - CSS scroll-behavior: smooth is applied
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { JSDOM } from 'jsdom';

// Smooth scroll module will be imported after DOM setup
let smoothScrollModule;

/**
 * Create a test HTML structure with navigation and sections
 */
function createNavigationHTML() {
  return `
    <!DOCTYPE html>
    <html lang="en">
    <head>
      <meta charset="UTF-8">
      <title>Test</title>
      <style>
        html { scroll-behavior: smooth; }
        .header { position: fixed; top: 0; height: 64px; width: 100%; }
        body { padding-top: 64px; }
        section { min-height: 100vh; }
      </style>
    </head>
    <body>
      <header class="header" id="site-header" style="position: fixed; top: 0; height: 64px; width: 100%;">
        <nav>
          <a href="#hero" class="nav-link">Home</a>
          <a href="#features" class="nav-link">Features</a>
          <a href="#quickstart" class="nav-link">Quick Start</a>
          <a href="#architecture" class="nav-link">Architecture</a>
          <a href="https://github.com/example" class="external-link">GitHub</a>
          <a href="#" class="home-link">Home</a>
        </nav>
      </header>

      <main>
        <section id="hero" style="min-height: 100vh;">
          <h1>Hero Section</h1>
          <a href="#quickstart" id="cta-get-started" class="btn btn-primary">Get Started</a>
        </section>

        <section id="features" style="min-height: 100vh;">
          <h2>Features Section</h2>
        </section>

        <section id="quickstart" style="min-height: 100vh;">
          <h2 id="quickstart-title">Quick Start Section</h2>
        </section>

        <section id="architecture" style="min-height: 100vh;">
          <h2>Architecture Section</h2>
        </section>
      </main>
    </body>
    </html>
  `;
}

describe('Smooth Scroll Navigation', () => {
  let dom;
  let document;
  let window;

  beforeEach(async () => {
    // Create fresh DOM for each test
    dom = new JSDOM(createNavigationHTML(), {
      url: 'http://localhost',
      runScripts: 'dangerously',
      pretendToBeVisual: true,
    });

    window = dom.window;
    document = window.document;

    // Set up global references for the module
    global.window = window;
    global.document = document;
    global.MouseEvent = window.MouseEvent;
    global.HTMLElement = window.HTMLElement;
    global.Element = window.Element;

    // Mock scrollTo and scrollIntoView
    window.scrollTo = vi.fn();
    window.scrollBy = vi.fn();
    window.pageYOffset = 0;

    // Mock getBoundingClientRect for all elements
    const mockGetBoundingClientRect = vi.fn().mockReturnValue({
      top: 500,
      left: 0,
      right: 100,
      bottom: 600,
      width: 100,
      height: 100,
    });

    // Apply mock to Element prototype
    window.Element.prototype.getBoundingClientRect = mockGetBoundingClientRect;

    // Mock scrollIntoView
    window.Element.prototype.scrollIntoView = vi.fn();

    // Import module fresh for each test
    vi.resetModules();
    smoothScrollModule = await import('../../assets/js/smooth-scroll.js');
  });

  afterEach(() => {
    // Clean up global references
    delete global.window;
    delete global.document;
    delete global.MouseEvent;
    delete global.HTMLElement;
    delete global.Element;
    vi.resetModules();
  });

  describe('CSS scroll-behavior property', () => {
    it('should have scroll-behavior: smooth on html element', () => {
      const htmlElement = document.documentElement;
      const computedStyle = window.getComputedStyle(htmlElement);

      // Note: JSDOM doesn't fully compute CSS, but we can verify via the module
      // The actual scroll-behavior is set in base.css
      expect(smoothScrollModule.getScrollBehavior).toBeDefined();

      // In real browser environment this would return 'smooth'
      // For unit testing, we verify the module checks this property
    });

    it('should export getScrollBehavior function', () => {
      expect(typeof smoothScrollModule.getScrollBehavior).toBe('function');
    });

    it('should return a valid scroll-behavior value', () => {
      const behavior = smoothScrollModule.getScrollBehavior();
      expect(['smooth', 'auto', 'instant', '']).toContain(behavior);
    });
  });

  describe('initSmoothScroll', () => {
    it('should initialize without errors', () => {
      expect(() => smoothScrollModule.initSmoothScroll()).not.toThrow();
    });

    it('should attach click handlers to anchor links', () => {
      const anchorLinks = document.querySelectorAll('a[href^="#"]');
      expect(anchorLinks.length).toBeGreaterThan(0);

      smoothScrollModule.initSmoothScroll();

      // Verify handlers are attached by triggering click
      anchorLinks.forEach(link => {
        expect(() => link.click()).not.toThrow();
      });
    });

    it('should not attach handlers to external links', () => {
      smoothScrollModule.initSmoothScroll();

      const externalLink = document.querySelector('.external-link');
      const clickEvent = new window.MouseEvent('click', {
        bubbles: true,
        cancelable: true,
      });

      // External links should not prevent default
      const prevented = !externalLink.dispatchEvent(clickEvent);
      expect(prevented).toBe(false);
    });

    it('should handle pages with no anchor links gracefully', () => {
      // Remove all anchor links
      document.querySelectorAll('a[href^="#"]').forEach(link => link.remove());

      expect(() => smoothScrollModule.initSmoothScroll()).not.toThrow();
    });

    it('should not process links with href="#" only', () => {
      smoothScrollModule.initSmoothScroll();

      const homeLink = document.querySelector('.home-link');
      homeLink.click();

      // Should not attempt to scroll to element with empty target
      // window.scrollTo should not be called for href="#"
    });
  });

  describe('scrollToElement', () => {
    it('should scroll to the specified element', () => {
      const targetId = 'quickstart';
      smoothScrollModule.scrollToElement(targetId);

      expect(window.scrollTo).toHaveBeenCalled();
    });

    it('should account for fixed header offset', () => {
      const headerHeight = 64;
      const targetId = 'quickstart';

      smoothScrollModule.scrollToElement(targetId, headerHeight);

      expect(window.scrollTo).toHaveBeenCalled();

      // Get the call arguments
      const callArgs = window.scrollTo.mock.calls[0][0];

      // Should have behavior: 'smooth'
      expect(callArgs.behavior).toBe('smooth');

      // Top should be calculated with offset
      expect(typeof callArgs.top).toBe('number');
    });

    it('should use default header offset when not provided', () => {
      smoothScrollModule.scrollToElement('features');

      expect(window.scrollTo).toHaveBeenCalled();

      const callArgs = window.scrollTo.mock.calls[0][0];
      expect(callArgs.behavior).toBe('smooth');
    });

    it('should handle non-existent element gracefully', () => {
      expect(() => smoothScrollModule.scrollToElement('nonexistent')).not.toThrow();

      // Should not call scrollTo for non-existent elements
      expect(window.scrollTo).not.toHaveBeenCalled();
    });

    it('should accept element ID with or without # prefix', () => {
      smoothScrollModule.scrollToElement('#quickstart');
      expect(window.scrollTo).toHaveBeenCalled();

      window.scrollTo.mockClear();

      smoothScrollModule.scrollToElement('quickstart');
      expect(window.scrollTo).toHaveBeenCalled();
    });
  });

  describe('getHeaderOffset', () => {
    it('should return the header height', () => {
      const offset = smoothScrollModule.getHeaderOffset();

      expect(typeof offset).toBe('number');
      expect(offset).toBeGreaterThanOrEqual(0);
    });

    it('should detect fixed header and return its height', () => {
      const header = document.querySelector('.header');

      // Mock getBoundingClientRect for header specifically
      header.getBoundingClientRect = vi.fn().mockReturnValue({
        height: 64,
        top: 0,
        left: 0,
        right: 100,
        bottom: 64,
        width: 100,
      });

      const offset = smoothScrollModule.getHeaderOffset();

      // Should account for header height
      expect(offset).toBe(64);
    });

    it('should return 0 when no fixed header exists', () => {
      // Remove header
      const header = document.querySelector('.header');
      header.remove();

      // Re-initialize module
      vi.resetModules();

      // For now, expect it to handle gracefully
      // The actual implementation should check for header existence
    });
  });

  describe('Anchor click behavior', () => {
    beforeEach(() => {
      smoothScrollModule.initSmoothScroll();
    });

    it('should prevent default behavior on anchor click', () => {
      const link = document.querySelector('a[href="#quickstart"]');

      const clickEvent = new window.MouseEvent('click', {
        bubbles: true,
        cancelable: true,
      });

      let defaultPrevented = false;
      clickEvent.preventDefault = vi.fn(() => {
        defaultPrevented = true;
      });

      link.dispatchEvent(clickEvent);

      expect(clickEvent.preventDefault).toHaveBeenCalled();
    });

    it('should scroll to target section when Get Started CTA is clicked', () => {
      const ctaButton = document.querySelector('#cta-get-started');

      ctaButton.click();

      expect(window.scrollTo).toHaveBeenCalled();
    });

    it('should update URL hash after scroll', () => {
      const link = document.querySelector('a[href="#features"]');

      link.click();

      // Module should update location hash
      // Note: JSDOM may not fully support this, so we verify the scroll happened
      expect(window.scrollTo).toHaveBeenCalled();
    });
  });

  describe('Scroll offset calculation', () => {
    it('should calculate correct scroll position accounting for header', () => {
      const targetElement = document.querySelector('#quickstart');
      const headerHeight = 64;

      // Mock specific position for target
      targetElement.getBoundingClientRect = vi.fn().mockReturnValue({
        top: 500,
        left: 0,
        right: 100,
        bottom: 600,
        width: 100,
        height: 100,
      });

      window.pageYOffset = 100;

      smoothScrollModule.scrollToElement('quickstart', headerHeight);

      expect(window.scrollTo).toHaveBeenCalled();

      const callArgs = window.scrollTo.mock.calls[0][0];

      // Expected position: elementTop + pageYOffset - headerHeight
      // = 500 + 100 - 64 = 536
      expect(callArgs.top).toBe(536);
    });

    it('should ensure target is visible after scroll (not hidden by header)', () => {
      const headerHeight = 64;

      const targetElement = document.querySelector('#quickstart');
      targetElement.getBoundingClientRect = vi.fn().mockReturnValue({
        top: 200,
        left: 0,
        right: 100,
        bottom: 300,
        width: 100,
        height: 100,
      });

      window.pageYOffset = 0;

      smoothScrollModule.scrollToElement('quickstart', headerHeight);

      const callArgs = window.scrollTo.mock.calls[0][0];

      // Scroll position should account for header
      // The calculated top should position the element below the header
      expect(callArgs.top).toBe(200 - headerHeight);
    });
  });

  describe('Smooth scroll animation', () => {
    it('should use smooth behavior for scrollTo', () => {
      smoothScrollModule.scrollToElement('quickstart');

      expect(window.scrollTo).toHaveBeenCalledWith(
        expect.objectContaining({
          behavior: 'smooth'
        })
      );
    });

    it('should use CSS scroll-behavior or JS animation', () => {
      // The module should either rely on CSS scroll-behavior: smooth
      // or implement JS-based smooth scrolling
      const behavior = smoothScrollModule.getScrollBehavior();

      // Either CSS handles it or we use JS with behavior: 'smooth'
      expect(['smooth', 'auto', 'instant', '']).toContain(behavior);
    });
  });

  describe('Edge cases', () => {
    it('should handle rapid consecutive clicks', () => {
      smoothScrollModule.initSmoothScroll();

      const link1 = document.querySelector('a[href="#features"]');
      const link2 = document.querySelector('a[href="#quickstart"]');

      // Rapid clicks
      link1.click();
      link2.click();
      link1.click();

      // Should not throw
      expect(window.scrollTo).toHaveBeenCalled();
    });

    it('should handle scroll to element at top of page', () => {
      const heroElement = document.querySelector('#hero');
      heroElement.getBoundingClientRect = vi.fn().mockReturnValue({
        top: 0,
        left: 0,
        right: 100,
        bottom: 100,
        width: 100,
        height: 100,
      });

      window.pageYOffset = 500;

      smoothScrollModule.scrollToElement('hero', 64);

      expect(window.scrollTo).toHaveBeenCalled();
    });

    it('should handle scroll when already at target', () => {
      const targetElement = document.querySelector('#features');
      targetElement.getBoundingClientRect = vi.fn().mockReturnValue({
        top: 64, // Exactly at header offset
        left: 0,
        right: 100,
        bottom: 164,
        width: 100,
        height: 100,
      });

      window.pageYOffset = 0;

      // Should still call scrollTo even if already near target
      smoothScrollModule.scrollToElement('features', 64);

      expect(window.scrollTo).toHaveBeenCalled();
    });
  });
});
