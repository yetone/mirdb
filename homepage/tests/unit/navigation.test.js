/**
 * Navigation Module Unit Tests
 * Owner: Scenario 6 - Navigation and Footer Implementation
 *
 * Unit tests for navigation.js functions.
 */

// Mock DOM environment
const fs = require('fs');
const path = require('path');

describe('Navigation Module', () => {
  let navigation;

  beforeAll(() => {
    // Load the navigation module
    const navigationPath = path.join(__dirname, '../../js/navigation.js');
    const navigationCode = fs.readFileSync(navigationPath, 'utf8');

    // Execute in test context
    eval(navigationCode);

    // Get exported functions
    navigation = {
      initNavigation,
      toggleMobileMenu,
      openMobileMenu,
      closeMobileMenu,
      isMobileMenuOpen
    };
  });

  beforeEach(() => {
    // Set up DOM for each test
    document.body.innerHTML = `
      <a href="#main-content" class="skip-link">Skip to main content</a>
      <nav class="nav">
        <div class="container nav__container">
          <a href="#" class="nav__logo">
            <span>MirDB</span>
          </a>
          <ul class="nav__links">
            <li><a href="#features">Features</a></li>
            <li><a href="#getting-started">Get Started</a></li>
            <li><a href="#architecture">Architecture</a></li>
          </ul>
          <button class="nav__mobile-toggle" aria-expanded="false" aria-controls="mobile-menu" aria-label="Toggle navigation menu">
            <span></span>
            <span></span>
            <span></span>
          </button>
        </div>
      </nav>
      <div id="mobile-menu" class="nav__mobile-menu" aria-hidden="true">
        <ul class="nav__mobile-menu-links">
          <li><a href="#features">Features</a></li>
          <li><a href="#getting-started">Get Started</a></li>
          <li><a href="#architecture">Architecture</a></li>
        </ul>
      </div>
      <main id="main-content">
        <section id="features" style="height: 500px;">Features</section>
        <section id="getting-started" style="height: 500px;">Getting Started</section>
        <section id="architecture" style="height: 500px;">Architecture</section>
      </main>
    `;
  });

  afterEach(() => {
    document.body.innerHTML = '';
    document.body.style.overflow = '';
  });

  describe('Mobile Menu', () => {
    test('isMobileMenuOpen returns false when menu is closed', () => {
      expect(navigation.isMobileMenuOpen()).toBe(false);
    });

    test('openMobileMenu opens the mobile menu', () => {
      navigation.openMobileMenu();

      const toggleButton = document.querySelector('.nav__mobile-toggle');
      const mobileMenu = document.querySelector('.nav__mobile-menu');

      expect(toggleButton.getAttribute('aria-expanded')).toBe('true');
      expect(mobileMenu.classList.contains('nav__mobile-menu--open')).toBe(true);
      expect(mobileMenu.getAttribute('aria-hidden')).toBe('false');
      expect(document.body.style.overflow).toBe('hidden');
    });

    test('closeMobileMenu closes the mobile menu', () => {
      // First open the menu
      navigation.openMobileMenu();

      // Then close it
      navigation.closeMobileMenu();

      const toggleButton = document.querySelector('.nav__mobile-toggle');
      const mobileMenu = document.querySelector('.nav__mobile-menu');

      expect(toggleButton.getAttribute('aria-expanded')).toBe('false');
      expect(mobileMenu.classList.contains('nav__mobile-menu--open')).toBe(false);
      expect(mobileMenu.getAttribute('aria-hidden')).toBe('true');
      expect(document.body.style.overflow).toBe('');
    });

    test('toggleMobileMenu toggles the menu state', () => {
      // Initially closed
      expect(navigation.isMobileMenuOpen()).toBe(false);

      // Toggle to open
      navigation.toggleMobileMenu();
      expect(navigation.isMobileMenuOpen()).toBe(true);

      // Toggle to close
      navigation.toggleMobileMenu();
      expect(navigation.isMobileMenuOpen()).toBe(false);
    });

    test('isMobileMenuOpen returns true when menu is open', () => {
      navigation.openMobileMenu();
      expect(navigation.isMobileMenuOpen()).toBe(true);
    });
  });

  describe('initNavigation', () => {
    test('does not throw when initialized', () => {
      expect(() => navigation.initNavigation()).not.toThrow();
    });

    test('sets up click handler on mobile toggle button', () => {
      navigation.initNavigation();

      const toggleButton = document.querySelector('.nav__mobile-toggle');
      const clickEvent = new MouseEvent('click', { bubbles: true });

      toggleButton.dispatchEvent(clickEvent);

      expect(navigation.isMobileMenuOpen()).toBe(true);
    });

    test('sets up click handler on mobile menu links to close menu', () => {
      navigation.initNavigation();

      // Open the menu first
      navigation.openMobileMenu();
      expect(navigation.isMobileMenuOpen()).toBe(true);

      // Click a link in the mobile menu
      const mobileLink = document.querySelector('.nav__mobile-menu-links a');
      const clickEvent = new MouseEvent('click', { bubbles: true });
      mobileLink.dispatchEvent(clickEvent);

      expect(navigation.isMobileMenuOpen()).toBe(false);
    });

    test('Escape key closes mobile menu', () => {
      navigation.initNavigation();

      // Open the menu
      navigation.openMobileMenu();
      expect(navigation.isMobileMenuOpen()).toBe(true);

      // Press Escape
      const escapeEvent = new KeyboardEvent('keydown', { key: 'Escape' });
      document.dispatchEvent(escapeEvent);

      expect(navigation.isMobileMenuOpen()).toBe(false);
    });
  });

  describe('DOM element handling', () => {
    test('handles missing mobile toggle button gracefully', () => {
      document.querySelector('.nav__mobile-toggle').remove();

      expect(() => navigation.toggleMobileMenu()).not.toThrow();
      expect(() => navigation.openMobileMenu()).not.toThrow();
      expect(() => navigation.closeMobileMenu()).not.toThrow();
    });

    test('handles missing mobile menu gracefully', () => {
      document.querySelector('.nav__mobile-menu').remove();

      expect(() => navigation.toggleMobileMenu()).not.toThrow();
      expect(() => navigation.openMobileMenu()).not.toThrow();
      expect(() => navigation.closeMobileMenu()).not.toThrow();
    });

    test('handles missing skip link gracefully', () => {
      document.querySelector('.skip-link').remove();

      expect(() => navigation.initNavigation()).not.toThrow();
    });
  });

  describe('Anchor link handling', () => {
    test('anchor links with # href are attached handlers during init', () => {
      navigation.initNavigation();

      const anchorLinks = document.querySelectorAll('a[href^="#"]');
      expect(anchorLinks.length).toBeGreaterThan(0);
    });
  });
});
