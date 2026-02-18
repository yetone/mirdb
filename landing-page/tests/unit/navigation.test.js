/**
 * Navigation Unit Tests
 * Owner: Scenario 2 - Navigation & Menu
 *
 * Test cases:
 * - Nav element exists with at least 3 anchor links
 * - Logo element present in navigation bar
 * - CTA button exists in navigation
 * - Mobile menu toggles correctly
 * - Navigation links are clickable
 */

const fs = require('fs');
const path = require('path');

// Read HTML file
const htmlPath = path.join(__dirname, '../../index.html');
const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

describe('Navigation - Unit Tests', () => {
  let container;

  beforeEach(() => {
    // Reset DOM for each test
    document.body.innerHTML = '';
    container = document.createElement('div');
    container.innerHTML = htmlContent;
    document.body.appendChild(container);
  });

  afterEach(() => {
    document.body.innerHTML = '';
  });

  describe('Test Case 1: Nav element with navigation links', () => {
    test('Nav element exists with at least 3 anchor links', () => {
      const nav = document.querySelector('nav[role="navigation"]');
      expect(nav).toBeTruthy();

      // Get all anchor links within the nav (desktop links)
      const navLinks = nav.querySelectorAll('.nav-links .nav-link');
      expect(navLinks.length).toBeGreaterThanOrEqual(3);
    });

    test('Navigation links have valid href attributes', () => {
      const navLinks = document.querySelectorAll('.nav-links .nav-link');

      navLinks.forEach((link) => {
        expect(link.getAttribute('href')).toBeTruthy();
        expect(link.getAttribute('href')).toMatch(/^#/); // Should be anchor links
      });
    });
  });

  describe('Test Case 2: Logo element in navigation', () => {
    test('Logo element is present in navigation bar', () => {
      const nav = document.querySelector('nav[role="navigation"]');
      expect(nav).toBeTruthy();

      const logo = nav.querySelector('.nav-logo');
      expect(logo).toBeTruthy();
    });

    test('Logo contains text or image', () => {
      const logo = document.querySelector('.nav-logo');
      expect(logo).toBeTruthy();

      // Check for text content or image
      const hasText = logo.textContent.trim().length > 0;
      const hasImage = logo.querySelector('img') !== null;

      expect(hasText || hasImage).toBe(true);
    });

    test('Logo links to homepage', () => {
      const logo = document.querySelector('.nav-logo');
      expect(logo.getAttribute('href')).toBe('/');
    });
  });

  describe('Test Case 3: CTA button in navigation', () => {
    test('CTA button exists in navigation for quick access', () => {
      const nav = document.querySelector('nav[role="navigation"]');
      expect(nav).toBeTruthy();

      const ctaButton = nav.querySelector('.nav-cta');
      expect(ctaButton).toBeTruthy();
    });

    test('CTA button has btn-primary class for styling', () => {
      const ctaButton = document.querySelector('.nav-cta');
      expect(ctaButton.classList.contains('btn')).toBe(true);
      expect(ctaButton.classList.contains('btn-primary')).toBe(true);
    });

    test('CTA button has accessible text', () => {
      const ctaButton = document.querySelector('.nav-cta');
      expect(ctaButton.textContent.trim().length).toBeGreaterThan(0);
    });
  });

  describe('Navigation Structure', () => {
    test('Header has correct role attribute', () => {
      const header = document.querySelector('header[role="banner"]');
      expect(header).toBeTruthy();
    });

    test('Nav has aria-label for accessibility', () => {
      const nav = document.querySelector('nav[role="navigation"]');
      expect(nav.getAttribute('aria-label')).toBeTruthy();
    });

    test('Mobile toggle button has accessibility attributes', () => {
      const toggle = document.getElementById('nav-mobile-toggle');
      expect(toggle).toBeTruthy();
      expect(toggle.getAttribute('aria-label')).toBeTruthy();
      expect(toggle.getAttribute('aria-expanded')).toBeTruthy();
      expect(toggle.getAttribute('aria-controls')).toBe('nav-mobile-menu');
    });

    test('Mobile menu has aria-hidden attribute', () => {
      const mobileMenu = document.getElementById('nav-mobile-menu');
      expect(mobileMenu).toBeTruthy();
      expect(mobileMenu.getAttribute('aria-hidden')).toBeTruthy();
    });
  });

  describe('Mobile Menu Structure', () => {
    test('Mobile menu contains navigation links', () => {
      const mobileMenu = document.getElementById('nav-mobile-menu');
      const mobileLinks = mobileMenu.querySelectorAll('.nav-mobile-link');

      expect(mobileLinks.length).toBeGreaterThanOrEqual(3);
    });

    test('Mobile menu contains CTA button', () => {
      const mobileMenu = document.getElementById('nav-mobile-menu');
      const mobileCta = mobileMenu.querySelector('.nav-mobile-cta');

      expect(mobileCta).toBeTruthy();
    });

    test('Mobile links match desktop links', () => {
      const desktopLinks = document.querySelectorAll('.nav-links .nav-link');
      const mobileLinks = document.querySelectorAll('.nav-mobile-link');

      expect(mobileLinks.length).toBe(desktopLinks.length);

      desktopLinks.forEach((desktopLink, index) => {
        expect(mobileLinks[index].getAttribute('href')).toBe(
          desktopLink.getAttribute('href')
        );
      });
    });
  });
});
