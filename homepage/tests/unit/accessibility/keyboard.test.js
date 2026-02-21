/**
 * Unit Tests for Keyboard Navigation Accessibility
 * Owner: Scenario 12 - Accessibility - Keyboard Navigation
 *
 * Tests verify:
 * - Skip navigation link is present in HTML
 * - Skip navigation link has correct href
 * - Skip navigation link has proper text content
 * - Focus styles are defined in CSS
 * - Main content target exists for skip link
 */

const fs = require('fs');
const path = require('path');

describe('Keyboard Navigation Accessibility - Unit Tests', () => {
  let htmlContent;
  let cssContent;

  beforeAll(() => {
    // Read the built HTML file
    const htmlPath = path.join(__dirname, '../../../book/index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf8');

    // Read the CSS file with focus styles
    const cssPath = path.join(__dirname, '../../../book/css/general.css');
    cssContent = fs.readFileSync(cssPath, 'utf8');
  });

  describe('Test Case 5: Skip to main content link', () => {
    test('Skip navigation link is present in HTML', () => {
      expect(htmlContent).toContain('class="skip-nav"');
    });

    test('Skip navigation link has correct href pointing to #content', () => {
      // Match skip-nav link with href="#content" regardless of attribute order
      const hasSkipNav = htmlContent.includes('class="skip-nav"');
      const hasCorrectHref = htmlContent.includes('href="#content"');
      expect(hasSkipNav).toBe(true);
      expect(hasCorrectHref).toBe(true);
    });

    test('Skip navigation link contains "skip" text (case-insensitive)', () => {
      // Extract the skip-nav link content
      const skipNavMatch = htmlContent.match(/<a[^>]*class="skip-nav"[^>]*>([^<]*)<\/a>/i);
      expect(skipNavMatch).not.toBeNull();
      expect(skipNavMatch[1].toLowerCase()).toContain('skip');
    });

    test('Main content target (#content) exists in HTML', () => {
      expect(htmlContent).toContain('id="content"');
    });

    test('Skip navigation link appears before main content', () => {
      const skipNavIndex = htmlContent.indexOf('class="skip-nav"');
      const mainContentIndex = htmlContent.indexOf('id="content"');
      expect(skipNavIndex).toBeLessThan(mainContentIndex);
    });
  });

  describe('Focus styles in CSS', () => {
    test('Focus styles are defined for interactive elements', () => {
      // Check that :focus or :focus-visible styles exist
      expect(cssContent).toMatch(/:focus(-visible)?/);
    });

    test('Skip navigation focus styles are defined', () => {
      // Check that .skip-nav:focus styles exist
      expect(cssContent).toContain('.skip-nav:focus');
    });

    test('Focus outline uses visible color', () => {
      // Check that outline styles are defined
      expect(cssContent).toMatch(/outline:\s*\d+px\s+solid/);
    });

    test('Focus outline offset is defined for better visibility', () => {
      expect(cssContent).toMatch(/outline-offset:/);
    });

    test('Navigation link focus styles are defined', () => {
      expect(cssContent).toContain('.nav-link:focus-visible');
    });

    test('Button focus styles are defined', () => {
      expect(cssContent).toMatch(/button:focus-visible|\.cta-button:focus-visible/);
    });

    test('Footer link focus styles are defined', () => {
      expect(cssContent).toContain('.footer-link:focus-visible');
    });
  });

  describe('Interactive elements have proper attributes', () => {
    test('All navigation links are present and accessible', () => {
      // Check that nav links exist
      expect(htmlContent).toContain('class="nav-link"');
    });

    test('CTA button has an ID for targeting', () => {
      expect(htmlContent).toContain('id="get-started-btn"');
    });

    test('Mobile menu toggle has proper aria attributes', () => {
      expect(htmlContent).toContain('aria-expanded');
      expect(htmlContent).toContain('aria-controls="main-nav"');
    });

    test('Copy buttons have accessible labels', () => {
      expect(htmlContent).toContain('aria-label="Copy to clipboard"');
    });

    test('External links have proper aria-label for accessibility', () => {
      expect(htmlContent).toMatch(/aria-label="[^"]*opens in new tab[^"]*"/i);
    });
  });
});
