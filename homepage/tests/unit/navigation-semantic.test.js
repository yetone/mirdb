/**
 * Navigation Semantic HTML Unit Tests
 * Owner: Scenario 8 - Navigation and Header
 *
 * Tests:
 * - Navigation uses <nav> element with proper ARIA attributes
 * - Header has role="banner"
 * - Navigation links use semantic list structure
 * - Accessibility attributes are present
 */

const fs = require('fs');
const path = require('path');

describe('Navigation Semantic HTML', () => {
  let document;

  beforeAll(() => {
    // Read the HTML file and set it as document body
    const htmlPath = path.join(__dirname, '../../index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    // Use the global document from jsdom environment
    document = window.document;
    document.documentElement.innerHTML = htmlContent;
  });

  describe('Test Case 4: Navigation Semantic HTML Structure', () => {
    test('navigation uses <nav> element', () => {
      const nav = document.querySelector('header nav');
      expect(nav).not.toBeNull();
      expect(nav.tagName.toLowerCase()).toBe('nav');
    });

    test('navigation has role="navigation"', () => {
      const nav = document.querySelector('header nav');
      expect(nav).not.toBeNull();
      expect(nav.getAttribute('role')).toBe('navigation');
    });

    test('navigation has aria-label attribute', () => {
      const nav = document.querySelector('header nav');
      expect(nav).not.toBeNull();
      const ariaLabel = nav.getAttribute('aria-label');
      expect(ariaLabel).toBeTruthy();
      expect(ariaLabel.toLowerCase()).toContain('navigation');
    });

    test('header has role="banner"', () => {
      const header = document.querySelector('header');
      expect(header).not.toBeNull();
      expect(header.getAttribute('role')).toBe('banner');
    });

    test('navigation links are in a list structure', () => {
      const navList = document.querySelector('nav ul');
      expect(navList).not.toBeNull();

      const listItems = document.querySelectorAll('nav ul li');
      expect(listItems.length).toBeGreaterThan(0);
    });

    test('navigation links have proper href attributes', () => {
      const navLinks = document.querySelectorAll('.nav-links a');
      expect(navLinks.length).toBeGreaterThan(0);

      navLinks.forEach((link) => {
        const href = link.getAttribute('href');
        expect(href).toBeTruthy();
        // Each link should start with # (internal) or http (external)
        expect(href.startsWith('#') || href.startsWith('http')).toBe(true);
      });
    });

    test('external links have proper security attributes', () => {
      const externalLinks = document.querySelectorAll('.nav-links a[target="_blank"]');

      externalLinks.forEach((link) => {
        const rel = link.getAttribute('rel');
        expect(rel).toBeTruthy();
        expect(rel).toContain('noopener');
      });
    });

    test('logo has accessible aria-label', () => {
      const logo = document.querySelector('.logo');
      expect(logo).not.toBeNull();

      const ariaLabel = logo.getAttribute('aria-label');
      expect(ariaLabel).toBeTruthy();
    });

    test('hamburger menu has proper aria attributes', () => {
      const hamburgerButton = document.querySelector('.hamburger-menu');
      expect(hamburgerButton).not.toBeNull();

      // Should have aria-label
      const ariaLabel = hamburgerButton.getAttribute('aria-label');
      expect(ariaLabel).toBeTruthy();

      // Should have aria-expanded attribute
      const ariaExpanded = hamburgerButton.getAttribute('aria-expanded');
      expect(ariaExpanded).toBeTruthy();
    });
  });

  describe('Navigation Link Structure', () => {
    test('contains link to Features section', () => {
      const featuresLink = document.querySelector('nav a[href="#features"]');
      expect(featuresLink).not.toBeNull();
    });

    test('contains link to Architecture section', () => {
      const archLink = document.querySelector('nav a[href="#architecture"]');
      expect(archLink).not.toBeNull();
    });

    test('contains link to Quick Start section', () => {
      const quickstartLink = document.querySelector('nav a[href="#quickstart"]');
      expect(quickstartLink).not.toBeNull();
    });

    test('contains link to Comparison section', () => {
      const comparisonLink = document.querySelector('nav a[href="#comparison"]');
      expect(comparisonLink).not.toBeNull();
    });

    test('contains link to GitHub', () => {
      const githubLink = document.querySelector('nav a[href*="github.com"]');
      expect(githubLink).not.toBeNull();
      expect(githubLink.getAttribute('target')).toBe('_blank');
    });
  });

  describe('Header Structure', () => {
    test('header contains logo', () => {
      const logo = document.querySelector('header .logo');
      expect(logo).not.toBeNull();
    });

    test('logo contains MirDB text', () => {
      const logo = document.querySelector('.logo');
      expect(logo).not.toBeNull();
      expect(logo.textContent).toContain('MirDB');
    });

    test('header contains navigation', () => {
      const nav = document.querySelector('header nav');
      expect(nav).not.toBeNull();
    });

    test('header has container for layout', () => {
      const container = document.querySelector('header .container');
      expect(container).not.toBeNull();
    });
  });
});
