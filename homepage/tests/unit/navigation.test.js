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

const fs = require('fs');
const path = require('path');
const vm = require('vm');

// Helper to create a mock DOM structure
function createMockDOM() {
  document.body.innerHTML = `
    <header class="header">
      <button class="mobile-menu-toggle" aria-label="Toggle menu">Menu</button>
      <nav class="header-nav" role="navigation">
        <a href="#features">Features</a>
        <a href="#status">Status</a>
        <a href="#usage">Usage</a>
        <a href="https://github.com/yetone/mirdb">GitHub</a>
        <a href="https://external-site.com/docs">External Docs</a>
      </nav>
    </header>
    <main>
      <section id="features">Features</section>
      <section id="status">Status</section>
      <section id="usage">Usage</section>
    </main>
    <footer>
      <a href="https://github.com/yetone/mirdb/issues">Issues</a>
      <a href="https://github.com/yetone/mirdb#readme">README</a>
    </footer>
  `;
}

// Load and execute navigation module, returning functions
function loadNavigationModule() {
  const navigationPath = path.join(__dirname, '../../src/js/navigation.js');
  const code = fs.readFileSync(navigationPath, 'utf8');

  // Create a context with browser globals
  const context = {
    document: document,
    window: window,
    Element: Element,
    console: console,
  };

  // Run the script in the context
  vm.createContext(context);
  vm.runInContext(code, context);

  // Return the functions from the context
  return {
    toggleMenu: context.toggleMenu,
    initMobileMenu: context.initMobileMenu,
    handleSmoothScroll: context.handleSmoothScroll,
    initExternalLinks: context.initExternalLinks,
  };
}

let navigationFunctions;

describe('Navigation Functionality', () => {
  beforeEach(() => {
    createMockDOM();
    navigationFunctions = loadNavigationModule();
  });

  afterEach(() => {
    document.body.innerHTML = '';
    jest.clearAllMocks();
  });

  describe('initExternalLinks', () => {
    test('TC11: initExternalLinks sets target and rel attributes on external links', () => {
      // Call initExternalLinks
      navigationFunctions.initExternalLinks();

      // Get all external links
      const externalLinks = document.querySelectorAll('a[href^="https://"]');

      externalLinks.forEach((link) => {
        expect(link.target).toBe('_blank');
        expect(link.rel).toMatch(/noopener/);
      });
    });

    test('initExternalLinks does not modify internal links', () => {
      navigationFunctions.initExternalLinks();

      // Get internal links (starting with #)
      const internalLinks = document.querySelectorAll('a[href^="#"]');

      internalLinks.forEach((link) => {
        // Internal links should not have target="_blank"
        expect(link.target).not.toBe('_blank');
      });
    });

    test('initExternalLinks adds noopener to rel attribute', () => {
      // Create a link without rel attribute
      const testLink = document.createElement('a');
      testLink.href = 'https://example.com';
      document.body.appendChild(testLink);

      navigationFunctions.initExternalLinks();

      expect(testLink.rel).toContain('noopener');
    });

    test('initExternalLinks preserves existing rel values', () => {
      // Create a link with existing rel attribute
      const testLink = document.createElement('a');
      testLink.href = 'https://example.com';
      testLink.rel = 'sponsored';
      document.body.appendChild(testLink);

      navigationFunctions.initExternalLinks();

      expect(testLink.rel).toContain('sponsored');
      expect(testLink.rel).toContain('noopener');
    });
  });

  describe('toggleMenu', () => {
    test('TC12: toggleMenu toggles menu visibility class', () => {
      const nav = document.querySelector('.header-nav');

      // Initially, nav should not have 'is-open' class
      expect(nav.classList.contains('is-open')).toBe(false);

      // Call toggleMenu
      navigationFunctions.toggleMenu();

      // After toggle, nav should have 'is-open' class
      expect(nav.classList.contains('is-open')).toBe(true);

      // Toggle again
      navigationFunctions.toggleMenu();

      // Should be closed again
      expect(nav.classList.contains('is-open')).toBe(false);
    });

    test('toggleMenu updates aria-expanded attribute', () => {
      const menuButton = document.querySelector('.mobile-menu-toggle');

      navigationFunctions.toggleMenu();

      expect(menuButton.getAttribute('aria-expanded')).toBe('true');

      navigationFunctions.toggleMenu();

      expect(menuButton.getAttribute('aria-expanded')).toBe('false');
    });
  });

  describe('initMobileMenu', () => {
    test('initMobileMenu attaches click handler to menu button', () => {
      const menuButton = document.querySelector('.mobile-menu-toggle');
      const nav = document.querySelector('.header-nav');

      navigationFunctions.initMobileMenu();

      // Simulate click
      menuButton.click();

      // Nav should now have 'is-open' class
      expect(nav.classList.contains('is-open')).toBe(true);
    });

    test('initMobileMenu handles missing menu button gracefully', () => {
      // Remove menu button
      const menuButton = document.querySelector('.mobile-menu-toggle');
      menuButton.remove();

      // Should not throw
      expect(() => {
        navigationFunctions.initMobileMenu();
      }).not.toThrow();
    });
  });

  describe('handleSmoothScroll', () => {
    test('handleSmoothScroll sets up click handlers for internal links', () => {
      // Mock scrollIntoView
      const scrollMock = jest.fn();
      Element.prototype.scrollIntoView = scrollMock;

      navigationFunctions.handleSmoothScroll();

      const featuresLink = document.querySelector('a[href="#features"]');

      // Simulate click
      featuresLink.click();

      // scrollIntoView should have been called
      expect(scrollMock).toHaveBeenCalledWith({
        behavior: 'smooth',
        block: 'start'
      });
    });

    test('handleSmoothScroll handles clicks on links to non-existent sections', () => {
      Element.prototype.scrollIntoView = jest.fn();

      // Add a link to a non-existent section
      const badLink = document.createElement('a');
      badLink.href = '#nonexistent';
      document.querySelector('nav').appendChild(badLink);

      navigationFunctions.handleSmoothScroll();

      // Should not throw when clicking
      expect(() => {
        badLink.click();
      }).not.toThrow();
    });
  });
});
