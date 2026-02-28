/**
 * Unit tests for navigation functionality
 * Owner: Scenario 4 - Navigation and External Links
 *
 * Test cases:
 * - toggleMenu shows/hides mobile menu
 * - initMobileMenu attaches click handler
 * - handleSmoothScroll scrolls to target
 * - initExternalLinks sets correct attributes
 */

// Import navigation functions
const {
  toggleMenu,
  initMobileMenu,
  handleSmoothScroll,
  initExternalLinks
} = require('../../src/js/navigation.js');

describe('Navigation Functionality', () => {
  beforeEach(() => {
    // Reset DOM before each test
    document.body.innerHTML = '';
  });

  describe('toggleMenu', () => {
    test('TC12: toggleMenu toggles menu visibility class', () => {
      document.body.innerHTML = `
        <header>
          <nav class="header-nav"></nav>
          <button class="mobile-menu-toggle" aria-label="Toggle menu"></button>
        </header>
      `;

      const nav = document.querySelector('.header-nav');
      const toggle = document.querySelector('.mobile-menu-toggle');

      // Initially nav should not have 'open' class
      expect(nav.classList.contains('open')).toBe(false);
      expect(toggle.getAttribute('aria-expanded')).not.toBe('true');

      // Toggle should add 'open' class
      toggleMenu();
      expect(nav.classList.contains('open')).toBe(true);
      expect(toggle.getAttribute('aria-expanded')).toBe('true');

      // Toggle again should remove 'open' class
      toggleMenu();
      expect(nav.classList.contains('open')).toBe(false);
      expect(toggle.getAttribute('aria-expanded')).toBe('false');
    });
  });

  describe('initMobileMenu', () => {
    test('initMobileMenu attaches click handler to toggle button', () => {
      document.body.innerHTML = `
        <header>
          <nav class="header-nav"></nav>
          <button class="mobile-menu-toggle" aria-label="Toggle menu"></button>
        </header>
      `;

      initMobileMenu();

      const nav = document.querySelector('.header-nav');
      const toggle = document.querySelector('.mobile-menu-toggle');

      // Click should toggle the menu
      toggle.click();
      expect(nav.classList.contains('open')).toBe(true);

      toggle.click();
      expect(nav.classList.contains('open')).toBe(false);
    });
  });

  describe('initExternalLinks', () => {
    test('TC11: initExternalLinks adds target and rel attributes to external links', () => {
      document.body.innerHTML = `
        <a href="https://github.com/yetone/mirdb">GitHub</a>
        <a href="https://example.com">Example</a>
        <a href="#features">Internal</a>
        <a href="/about">Relative</a>
      `;

      initExternalLinks();

      const githubLink = document.querySelector('a[href*="github.com"]');
      const exampleLink = document.querySelector('a[href*="example.com"]');
      const internalLink = document.querySelector('a[href="#features"]');
      const relativeLink = document.querySelector('a[href="/about"]');

      // External links should have target and rel attributes
      expect(githubLink.getAttribute('target')).toBe('_blank');
      expect(githubLink.getAttribute('rel')).toContain('noopener');
      expect(githubLink.getAttribute('rel')).toContain('noreferrer');

      expect(exampleLink.getAttribute('target')).toBe('_blank');
      expect(exampleLink.getAttribute('rel')).toContain('noopener');

      // Internal links should not have target="_blank"
      expect(internalLink.getAttribute('target')).toBeNull();
      expect(relativeLink.getAttribute('target')).toBeNull();
    });

    test('initExternalLinks does not modify links that already have attributes', () => {
      document.body.innerHTML = `
        <a href="https://github.com/yetone/mirdb" target="_blank" rel="noopener noreferrer">GitHub</a>
      `;

      initExternalLinks();

      const githubLink = document.querySelector('a[href*="github.com"]');

      // Should preserve existing attributes
      expect(githubLink.getAttribute('target')).toBe('_blank');
      expect(githubLink.getAttribute('rel')).toBe('noopener noreferrer');
    });
  });

  describe('handleSmoothScroll', () => {
    test('handleSmoothScroll attaches click handlers to anchor links', () => {
      document.body.innerHTML = `
        <nav>
          <a href="#features">Features</a>
          <a href="#status">Status</a>
        </nav>
        <section id="features" style="margin-top: 1000px;">Features</section>
        <section id="status" style="margin-top: 500px;">Status</section>
      `;

      // Mock scrollIntoView
      const scrollIntoViewMock = jest.fn();
      Element.prototype.scrollIntoView = scrollIntoViewMock;

      handleSmoothScroll();

      const featuresLink = document.querySelector('a[href="#features"]');
      featuresLink.click();

      expect(scrollIntoViewMock).toHaveBeenCalledWith({ behavior: 'smooth' });
    });
  });
});
