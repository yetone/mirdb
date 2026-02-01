/**
 * Security Best Practices Tests
 * Owner: Scenario 20 - Security Best Practices
 *
 * Tests:
 * - CDN scripts have integrity attribute (SRI)
 * - CDN scripts have crossorigin attribute
 * - Minimal inline JavaScript
 */
const { loadHTML } = require('../helpers/dom-utils');

describe('Security Best Practices', () => {
  beforeEach(() => {
    loadHTML('index.html');
  });

  describe('CDN Script Security', () => {
    test('CDN scripts should have integrity attribute with SRI hash', () => {
      // Get all external scripts from CDN
      const scripts = document.querySelectorAll('script[src*="cdn"], script[src*="cdnjs"]');

      expect(scripts.length).toBeGreaterThan(0);

      scripts.forEach((script) => {
        const src = script.getAttribute('src');
        const integrity = script.getAttribute('integrity');

        // Check that integrity attribute exists and contains a valid SRI hash
        expect(integrity).not.toBeNull();
        expect(integrity).toBeTruthy();

        // SRI hashes should start with sha256-, sha384-, or sha512-
        const validSRI = /^sha(256|384|512)-[A-Za-z0-9+/=]+$/.test(integrity);
        expect(validSRI).toBe(true);
      });
    });

    test('CDN scripts should have crossorigin="anonymous" attribute', () => {
      // Get all external scripts from CDN
      const scripts = document.querySelectorAll('script[src*="cdn"], script[src*="cdnjs"]');

      expect(scripts.length).toBeGreaterThan(0);

      scripts.forEach((script) => {
        const crossorigin = script.getAttribute('crossorigin');

        // Check that crossorigin attribute is set to 'anonymous'
        expect(crossorigin).toBe('anonymous');
      });
    });

    test('CDN stylesheets should have integrity and crossorigin attributes', () => {
      // Get all external stylesheets from CDN
      const stylesheets = document.querySelectorAll('link[rel="stylesheet"][href*="cdn"], link[rel="stylesheet"][href*="cdnjs"]');

      stylesheets.forEach((stylesheet) => {
        const href = stylesheet.getAttribute('href');
        const integrity = stylesheet.getAttribute('integrity');
        const crossorigin = stylesheet.getAttribute('crossorigin');

        // Check that integrity attribute exists and contains a valid SRI hash
        expect(integrity).not.toBeNull();
        expect(integrity).toBeTruthy();

        // SRI hashes should start with sha256-, sha384-, or sha512-
        const validSRI = /^sha(256|384|512)-[A-Za-z0-9+/=]+$/.test(integrity);
        expect(validSRI).toBe(true);

        // Check that crossorigin attribute is set to 'anonymous'
        expect(crossorigin).toBe('anonymous');
      });
    });
  });

  describe('Inline JavaScript', () => {
    test('should have minimal inline JavaScript', () => {
      // Get all inline scripts (scripts without src attribute)
      const inlineScripts = document.querySelectorAll('script:not([src])');

      // Count inline scripts - should be minimal (0 or very few)
      // We allow at most 1 inline script for critical initialization if absolutely needed
      expect(inlineScripts.length).toBeLessThanOrEqual(1);

      // If there is an inline script, check it's small (less than 500 characters)
      inlineScripts.forEach((script) => {
        const content = script.textContent.trim();
        // Allow empty scripts or small initialization code
        expect(content.length).toBeLessThan(500);
      });
    });

    test('should prefer external JavaScript files', () => {
      // Get all external scripts
      const externalScripts = document.querySelectorAll('script[src]');

      // Get all inline scripts
      const inlineScripts = document.querySelectorAll('script:not([src])');

      // External scripts should outnumber inline scripts
      expect(externalScripts.length).toBeGreaterThanOrEqual(inlineScripts.length);
    });

    test('should not have inline event handlers in HTML', () => {
      // Check for common inline event handlers
      const elementsWithInlineHandlers = document.querySelectorAll(
        '[onclick], [onload], [onerror], [onmouseover], [onmouseout], [onsubmit], [onfocus], [onblur]'
      );

      // Should have no inline event handlers - use external JS instead
      expect(elementsWithInlineHandlers.length).toBe(0);
    });
  });

  describe('External Link Security', () => {
    test('external links should have rel="noopener" to prevent tabnabbing', () => {
      // Get all external links that open in new tab
      const externalLinks = document.querySelectorAll('a[target="_blank"]');

      externalLinks.forEach((link) => {
        const rel = link.getAttribute('rel');

        // Should have noopener in rel attribute
        expect(rel).toBeTruthy();
        expect(rel).toContain('noopener');
      });
    });
  });
});
