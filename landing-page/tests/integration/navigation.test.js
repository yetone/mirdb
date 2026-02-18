/**
 * Navigation Integration Tests
 * Owner: Scenario 2 - Navigation & Menu
 *
 * Test cases:
 * - Sticky navigation behavior
 * - Mobile menu toggle
 * - Responsive behavior
 */

const fs = require('fs');
const path = require('path');

// Read HTML and CSS files
const htmlPath = path.join(__dirname, '../../index.html');
const cssPath = path.join(__dirname, '../../css/styles.css');
const htmlContent = fs.readFileSync(htmlPath, 'utf-8');
const cssContent = fs.readFileSync(cssPath, 'utf-8');

describe('Navigation - Integration Tests', () => {
  let container;

  beforeEach(() => {
    // Reset DOM for each test
    document.body.innerHTML = '';
    container = document.createElement('div');
    container.innerHTML = htmlContent;
    document.body.appendChild(container);

    // Add styles
    const style = document.createElement('style');
    style.textContent = cssContent;
    document.head.appendChild(style);
  });

  afterEach(() => {
    document.body.innerHTML = '';
    // Remove added styles
    const styles = document.querySelectorAll('style');
    styles.forEach((style) => style.remove());
  });

  describe('Test Case 4: Sticky Navigation', () => {
    test('Navigation bar has position:sticky in CSS', () => {
      const header = document.querySelector('.header');
      expect(header).toBeTruthy();

      // Check computed styles
      const computedStyle = window.getComputedStyle(header);
      expect(['sticky', 'fixed']).toContain(computedStyle.position);
    });

    test('Navigation has z-index for layering', () => {
      // Check that the CSS contains z-index definition for header
      // jsdom doesn't fully support CSS custom properties, so check CSS text
      expect(cssContent).toMatch(/\.header\s*\{[^}]*z-index/);
    });

    test('Header has box-shadow for visual separation', () => {
      const header = document.querySelector('.header');
      const computedStyle = window.getComputedStyle(header);

      // Box shadow should be set
      expect(computedStyle.boxShadow).not.toBe('none');
    });
  });

  describe('Test Case 6: Mobile Menu Visibility', () => {
    test('Hamburger menu CSS is defined for mobile viewport', () => {
      // Check that the CSS contains media query for mobile
      expect(cssContent).toMatch(/@media.*max-width.*767/i);
    });

    test('Mobile toggle button has display:none by default (desktop)', () => {
      const toggle = document.getElementById('nav-mobile-toggle');
      const computedStyle = window.getComputedStyle(toggle);

      // On desktop (default), toggle should be hidden
      expect(computedStyle.display).toBe('none');
    });

    test('Desktop nav links are visible by default', () => {
      const navLinks = document.querySelector('.nav-links');
      const computedStyle = window.getComputedStyle(navLinks);

      expect(computedStyle.display).not.toBe('none');
    });

    test('Mobile menu is hidden by default', () => {
      const mobileMenu = document.getElementById('nav-mobile-menu');
      const computedStyle = window.getComputedStyle(mobileMenu);

      // Should be hidden (either display:none or visibility:hidden)
      const isHidden =
        computedStyle.display === 'none' ||
        computedStyle.visibility === 'hidden' ||
        computedStyle.opacity === '0';

      expect(isHidden).toBe(true);
    });
  });

  describe('Navigation CSS Styling', () => {
    test('Nav uses flexbox for layout', () => {
      const nav = document.querySelector('.nav');
      const computedStyle = window.getComputedStyle(nav);

      expect(computedStyle.display).toBe('flex');
    });

    test('Nav items are centered vertically', () => {
      const nav = document.querySelector('.nav');
      const computedStyle = window.getComputedStyle(nav);

      expect(computedStyle.alignItems).toBe('center');
    });

    test('Nav has space-between justification', () => {
      const nav = document.querySelector('.nav');
      const computedStyle = window.getComputedStyle(nav);

      expect(computedStyle.justifyContent).toBe('space-between');
    });
  });

  describe('CTA Button Styling', () => {
    test('CTA button has proper background color', () => {
      const ctaButton = document.querySelector('.nav-cta');
      const computedStyle = window.getComputedStyle(ctaButton);

      // Should have a background color set (not transparent)
      expect(computedStyle.backgroundColor).not.toBe('transparent');
      expect(computedStyle.backgroundColor).not.toBe('rgba(0, 0, 0, 0)');
    });

    test('CTA button has border-radius', () => {
      // Check that the CSS defines border-radius for .btn class
      // jsdom doesn't fully support CSS custom properties
      expect(cssContent).toMatch(/\.btn\s*\{[^}]*border-radius/);
    });
  });

  describe('Accessibility Integration', () => {
    test('Skip link is present', () => {
      const skipLink = document.querySelector('.skip-link');
      expect(skipLink).toBeTruthy();
      expect(skipLink.getAttribute('href')).toBe('#main-content');
    });

    test('Main content target exists', () => {
      const mainContent = document.getElementById('main-content');
      expect(mainContent).toBeTruthy();
    });

    test('Header landmark is properly structured', () => {
      const header = document.querySelector('header[role="banner"]');
      const nav = header.querySelector('nav[role="navigation"]');

      expect(header).toBeTruthy();
      expect(nav).toBeTruthy();
    });
  });
});
