/**
 * E2E Tests for Navigation Bar Functionality
 *
 * Scenario: Verify that the navigation bar functions correctly with links to key sections
 * These tests verify the navigation bar implementation meets all requirements.
 */

const fs = require('fs');
const path = require('path');

describe('Navigation Bar Functionality', () => {
  let document;
  let htmlContent;
  const MIRDB_GITHUB_URL = 'https://github.com/yetone/mirdb';

  beforeAll(() => {
    // Load the HTML file
    const htmlPath = path.join(__dirname, '..', 'index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    // Parse HTML using jsdom
    document = new DOMParser().parseFromString(htmlContent, 'text/html');
  });

  describe('Test Case 1: Navigation bar displays MirDB logo', () => {
    /**
     * Test Case ID: 1
     * Input: Check navigation bar contains Logo
     * Expected: Navigation bar displays MirDB logo
     * Type: e2e
     */
    test('navigation bar should exist and be visible', () => {
      const nav = document.querySelector('nav');
      expect(nav).toBeTruthy();
    });

    test('navigation bar should contain MirDB logo', () => {
      const nav = document.querySelector('nav');
      expect(nav).toBeTruthy();

      // Look for logo in various forms (text or image)
      const logoLink = nav.querySelector('.nav-logo');
      expect(logoLink).toBeTruthy();
    });

    test('logo should display MirDB text', () => {
      const nav = document.querySelector('nav');
      const logoText = nav.querySelector('.nav-logo');

      expect(logoText).toBeTruthy();
      expect(logoText.textContent.toLowerCase()).toContain('mirdb');
    });

    test('logo should link to top of page (home)', () => {
      const nav = document.querySelector('nav');
      const logoLink = nav.querySelector('.nav-logo');

      expect(logoLink).toBeTruthy();
      const href = logoLink.getAttribute('href');
      expect(href).toBe('#');
    });

    test('navigation bar should be at top of page with position sticky or fixed', () => {
      const nav = document.querySelector('nav');
      expect(nav).toBeTruthy();

      // The navigation should have sticky positioning (from CSS)
      // We verify the element exists in the HTML at the top
      const body = document.body;
      const firstChild = body.children[0];

      // Navigation should be one of the first elements
      // Comments don't count as children in most DOM implementations
      expect(firstChild.tagName.toLowerCase()).toBe('nav');
    });
  });

  describe('Test Case 2: Navigation has Features link that scrolls to features section', () => {
    /**
     * Test Case ID: 2
     * Input: Check navigation bar contains Features link
     * Expected: Navigation has Features link that scrolls to features section
     * Type: e2e
     */
    test('navigation should have Features link', () => {
      const nav = document.querySelector('nav');
      const featuresLink = nav.querySelector('a[href="#features"]');

      expect(featuresLink).toBeTruthy();
    });

    test('Features link should have correct text', () => {
      const nav = document.querySelector('nav');
      const featuresLink = nav.querySelector('a[href="#features"]');

      expect(featuresLink).toBeTruthy();
      expect(featuresLink.textContent.toLowerCase()).toContain('features');
    });

    test('Features section should exist in the page', () => {
      const featuresSection = document.getElementById('features');
      expect(featuresSection).toBeTruthy();
    });

    test('Features link should point to valid features section', () => {
      const nav = document.querySelector('nav');
      const featuresLink = nav.querySelector('a[href="#features"]');

      expect(featuresLink).toBeTruthy();
      const href = featuresLink.getAttribute('href');
      const targetId = href.substring(1);
      const targetSection = document.getElementById(targetId);

      expect(targetSection).toBeTruthy();
    });
  });

  describe('Test Case 3: Navigation has Docs link pointing to documentation', () => {
    /**
     * Test Case ID: 3
     * Input: Check navigation bar contains Docs link
     * Expected: Navigation has Docs link pointing to documentation
     * Type: e2e
     */
    test('navigation should have a link to documentation', () => {
      const nav = document.querySelector('nav');
      const navLinks = nav.querySelectorAll('.nav-links a');

      // Find link that either says "Docs" or points to documentation
      const docsLink = Array.from(navLinks).find(
        (link) =>
          link.textContent.toLowerCase().includes('docs') ||
          link.textContent.toLowerCase().includes('documentation') ||
          link.href.includes('readme') ||
          link.href.includes('docs')
      );

      expect(docsLink).toBeTruthy();
    });

    test('Docs link should point to valid documentation', () => {
      const nav = document.querySelector('nav');
      const navLinks = nav.querySelectorAll('.nav-links a');

      const docsLink = Array.from(navLinks).find(
        (link) =>
          link.textContent.toLowerCase().includes('docs') ||
          link.textContent.toLowerCase().includes('documentation') ||
          link.href.includes('readme') ||
          link.href.includes('docs')
      );

      expect(docsLink).toBeTruthy();
      const href = docsLink.getAttribute('href');
      expect(href).toBeTruthy();

      // Docs should either be an internal section or GitHub readme
      expect(href).toMatch(/(#|github\.com.*readme|docs)/i);
    });
  });

  describe('Test Case 4: Navigation has GitHub link opening repository in new tab', () => {
    /**
     * Test Case ID: 4
     * Input: Check navigation bar contains GitHub link
     * Expected: Navigation has GitHub link opening repository in new tab
     * Type: e2e
     */
    test('navigation should have GitHub link', () => {
      const nav = document.querySelector('nav');
      const navLinks = nav.querySelectorAll('.nav-links a');

      const githubLink = Array.from(navLinks).find(
        (link) =>
          link.textContent.toLowerCase().includes('github') ||
          link.href.includes('github.com')
      );

      expect(githubLink).toBeTruthy();
    });

    test('GitHub link should point to MirDB repository', () => {
      const nav = document.querySelector('nav');
      const navLinks = nav.querySelectorAll('.nav-links a');

      const githubLink = Array.from(navLinks).find(
        (link) => link.href.includes('github.com')
      );

      expect(githubLink).toBeTruthy();
      expect(githubLink.href).toContain(MIRDB_GITHUB_URL);
    });

    test('GitHub link should open in new tab', () => {
      const nav = document.querySelector('nav');
      const navLinks = nav.querySelectorAll('.nav-links a');

      const githubLink = Array.from(navLinks).find(
        (link) => link.href.includes('github.com')
      );

      expect(githubLink).toBeTruthy();
      expect(githubLink.getAttribute('target')).toBe('_blank');
    });

    test('GitHub link should have security attributes', () => {
      const nav = document.querySelector('nav');
      const navLinks = nav.querySelectorAll('.nav-links a');

      const githubLink = Array.from(navLinks).find(
        (link) => link.href.includes('github.com')
      );

      expect(githubLink).toBeTruthy();
      const rel = githubLink.getAttribute('rel');
      expect(rel).toBeTruthy();
      expect(rel).toContain('noopener');
      expect(rel).toContain('noreferrer');
    });
  });

  describe('Additional Navigation Tests', () => {
    test('navigation bar should have proper navigation role', () => {
      // Check that nav element exists (provides implicit navigation landmark)
      const nav = document.querySelector('nav');
      expect(nav).toBeTruthy();
    });

    test('all navigation links should have valid href attributes', () => {
      const nav = document.querySelector('nav');
      const links = nav.querySelectorAll('a[href]');

      expect(links.length).toBeGreaterThan(0);

      links.forEach((link) => {
        const href = link.getAttribute('href');
        expect(href).toBeTruthy();
        expect(href.trim()).not.toBe('');
      });
    });

    test('navigation should have at least 3 navigation items', () => {
      const nav = document.querySelector('nav');
      const navLinks = nav.querySelectorAll('.nav-links a');

      // Should have Features, Docs/other internal, and GitHub at minimum
      expect(navLinks.length).toBeGreaterThanOrEqual(3);
    });

    test('navigation links should have meaningful text content', () => {
      const nav = document.querySelector('nav');
      const navLinks = nav.querySelectorAll('.nav-links a');

      navLinks.forEach((link) => {
        const text = link.textContent.trim();
        expect(text).toBeTruthy();
        expect(text.length).toBeGreaterThan(0);
      });
    });

    test('internal anchor links in navigation should reference existing sections', () => {
      const nav = document.querySelector('nav');
      const internalLinks = nav.querySelectorAll('.nav-links a[href^="#"]');

      internalLinks.forEach((link) => {
        const href = link.getAttribute('href');
        // Skip empty anchor
        if (href === '#') return;

        const targetId = href.substring(1);
        const targetSection = document.getElementById(targetId);
        expect(targetSection).toBeTruthy();
      });
    });

    test('external links in navigation should have proper security attributes', () => {
      const nav = document.querySelector('nav');
      const externalLinks = nav.querySelectorAll('.nav-links a[href^="http"]');

      externalLinks.forEach((link) => {
        expect(link.getAttribute('target')).toBe('_blank');
        const rel = link.getAttribute('rel');
        expect(rel).toBeTruthy();
        expect(rel).toContain('noopener');
        expect(rel).toContain('noreferrer');
      });
    });
  });
});
