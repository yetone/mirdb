/**
 * Tests for Security Headers Configuration (Scenario 20)
 *
 * These tests verify that proper security headers are configured including:
 * - HTTPS configuration
 * - Content-Security-Policy header
 * - X-Content-Type-Options header
 * - External links have rel='noopener noreferrer'
 */

const fs = require('fs');
const path = require('path');

describe('Security Headers Configuration', () => {
  let document;
  let htmlContent;

  beforeAll(() => {
    // Load the HTML file
    const htmlPath = path.join(__dirname, '..', 'index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    // Parse HTML using jsdom
    document = new DOMParser().parseFromString(htmlContent, 'text/html');
  });

  describe('Test Case 1: HTTPS Configuration', () => {
    /**
     * Test Case ID: 1
     * Input: Check page is served over HTTPS
     * Expected: Page URL uses https:// protocol
     * Type: integration
     *
     * Note: This test validates that the HTML content is configured for HTTPS
     * by checking canonical URLs, external resource references, and meta tags.
     */
    test('canonical URL should use HTTPS protocol', () => {
      const canonical = document.querySelector('link[rel="canonical"]');
      expect(canonical).toBeTruthy();

      const href = canonical.getAttribute('href');
      expect(href).toBeTruthy();
      expect(href).toMatch(/^https:\/\//);
    });

    test('Open Graph URL should use HTTPS protocol', () => {
      const ogUrl = document.querySelector('meta[property="og:url"]');
      expect(ogUrl).toBeTruthy();

      const content = ogUrl.getAttribute('content');
      expect(content).toBeTruthy();
      expect(content).toMatch(/^https:\/\//);
    });

    test('Open Graph image URL should use HTTPS protocol', () => {
      const ogImage = document.querySelector('meta[property="og:image"]');
      expect(ogImage).toBeTruthy();

      const content = ogImage.getAttribute('content');
      expect(content).toBeTruthy();
      expect(content).toMatch(/^https:\/\//);
    });

    test('Twitter image URL should use HTTPS protocol', () => {
      const twitterImage = document.querySelector('meta[name="twitter:image"]');
      expect(twitterImage).toBeTruthy();

      const content = twitterImage.getAttribute('content');
      expect(content).toBeTruthy();
      expect(content).toMatch(/^https:\/\//);
    });

    test('all external links should use HTTPS protocol', () => {
      const externalLinks = document.querySelectorAll('a[href^="http"]');
      expect(externalLinks.length).toBeGreaterThan(0);

      externalLinks.forEach((link) => {
        const href = link.getAttribute('href');
        expect(href).toMatch(/^https:\/\//);
      });
    });
  });

  describe('Test Case 2: Content-Security-Policy Configuration', () => {
    /**
     * Test Case ID: 2
     * Input: Check Content-Security-Policy header
     * Expected: CSP header is present and appropriately configured
     * Type: integration
     *
     * Note: For static HTML files, CSP can be configured via meta tag.
     * Server-side headers require server configuration.
     */
    test('CSP meta tag should be present', () => {
      const cspMeta = document.querySelector('meta[http-equiv="Content-Security-Policy"]');
      expect(cspMeta).toBeTruthy();
    });

    test('CSP should include default-src directive', () => {
      const cspMeta = document.querySelector('meta[http-equiv="Content-Security-Policy"]');
      expect(cspMeta).toBeTruthy();

      const content = cspMeta.getAttribute('content');
      expect(content).toBeTruthy();
      expect(content).toContain('default-src');
    });

    test('CSP should include script-src directive', () => {
      const cspMeta = document.querySelector('meta[http-equiv="Content-Security-Policy"]');
      expect(cspMeta).toBeTruthy();

      const content = cspMeta.getAttribute('content');
      expect(content).toBeTruthy();
      expect(content).toContain('script-src');
    });

    test('CSP should include style-src directive', () => {
      const cspMeta = document.querySelector('meta[http-equiv="Content-Security-Policy"]');
      expect(cspMeta).toBeTruthy();

      const content = cspMeta.getAttribute('content');
      expect(content).toBeTruthy();
      expect(content).toContain('style-src');
    });

    test('CSP should include img-src directive', () => {
      const cspMeta = document.querySelector('meta[http-equiv="Content-Security-Policy"]');
      expect(cspMeta).toBeTruthy();

      const content = cspMeta.getAttribute('content');
      expect(content).toBeTruthy();
      expect(content).toContain('img-src');
    });
  });

  describe('Test Case 3: X-Content-Type-Options Configuration', () => {
    /**
     * Test Case ID: 3
     * Input: Check X-Content-Type-Options header
     * Expected: Header is set to 'nosniff'
     * Type: integration
     *
     * Note: X-Content-Type-Options is primarily a server header, but can
     * be simulated via meta tag for documentation. The test validates
     * the presence of this security configuration.
     */
    test('X-Content-Type-Options meta tag should be present', () => {
      const xctMeta = document.querySelector('meta[http-equiv="X-Content-Type-Options"]');
      expect(xctMeta).toBeTruthy();
    });

    test('X-Content-Type-Options should be set to nosniff', () => {
      const xctMeta = document.querySelector('meta[http-equiv="X-Content-Type-Options"]');
      expect(xctMeta).toBeTruthy();

      const content = xctMeta.getAttribute('content');
      expect(content).toBe('nosniff');
    });
  });

  describe('Test Case 4: External Links Security (rel=noopener)', () => {
    /**
     * Test Case ID: 4
     * Input: Check external links have rel='noopener'
     * Expected: All target='_blank' links have rel='noopener noreferrer'
     * Type: unit
     */
    test('all external links with target="_blank" should have rel attribute', () => {
      const blankLinks = document.querySelectorAll('a[target="_blank"]');
      expect(blankLinks.length).toBeGreaterThan(0);

      blankLinks.forEach((link) => {
        const rel = link.getAttribute('rel');
        expect(rel).toBeTruthy();
      });
    });

    test('all external links with target="_blank" should have rel="noopener"', () => {
      const blankLinks = document.querySelectorAll('a[target="_blank"]');
      expect(blankLinks.length).toBeGreaterThan(0);

      blankLinks.forEach((link) => {
        const rel = link.getAttribute('rel');
        expect(rel).toBeTruthy();
        expect(rel).toContain('noopener');
      });
    });

    test('all external links with target="_blank" should have rel="noreferrer"', () => {
      const blankLinks = document.querySelectorAll('a[target="_blank"]');
      expect(blankLinks.length).toBeGreaterThan(0);

      blankLinks.forEach((link) => {
        const rel = link.getAttribute('rel');
        expect(rel).toBeTruthy();
        expect(rel).toContain('noreferrer');
      });
    });

    test('navigation bar external links should have proper security attributes', () => {
      const nav = document.querySelector('nav');
      expect(nav).toBeTruthy();

      const externalLinks = nav.querySelectorAll('a[href^="http"]');

      externalLinks.forEach((link) => {
        expect(link.getAttribute('target')).toBe('_blank');
        const rel = link.getAttribute('rel');
        expect(rel).toBeTruthy();
        expect(rel).toContain('noopener');
        expect(rel).toContain('noreferrer');
      });
    });

    test('hero section external links should have proper security attributes', () => {
      const hero = document.querySelector('.hero');
      expect(hero).toBeTruthy();

      const externalLinks = hero.querySelectorAll('a[href^="http"]');

      externalLinks.forEach((link) => {
        expect(link.getAttribute('target')).toBe('_blank');
        const rel = link.getAttribute('rel');
        expect(rel).toBeTruthy();
        expect(rel).toContain('noopener');
        expect(rel).toContain('noreferrer');
      });
    });

    test('footer external links should have proper security attributes', () => {
      const footer = document.querySelector('footer');
      expect(footer).toBeTruthy();

      const externalLinks = footer.querySelectorAll('a[href^="http"]');
      expect(externalLinks.length).toBeGreaterThan(0);

      externalLinks.forEach((link) => {
        expect(link.getAttribute('target')).toBe('_blank');
        const rel = link.getAttribute('rel');
        expect(rel).toBeTruthy();
        expect(rel).toContain('noopener');
        expect(rel).toContain('noreferrer');
      });
    });
  });

  describe('Additional Security Meta Tags', () => {
    test('X-Frame-Options meta tag should be present (clickjacking protection)', () => {
      const xfoMeta = document.querySelector('meta[http-equiv="X-Frame-Options"]');
      expect(xfoMeta).toBeTruthy();
    });

    test('X-Frame-Options should be set to DENY or SAMEORIGIN', () => {
      const xfoMeta = document.querySelector('meta[http-equiv="X-Frame-Options"]');
      expect(xfoMeta).toBeTruthy();

      const content = xfoMeta.getAttribute('content');
      expect(['DENY', 'SAMEORIGIN']).toContain(content);
    });

    test('X-XSS-Protection meta tag should be present', () => {
      const xxssMeta = document.querySelector('meta[http-equiv="X-XSS-Protection"]');
      expect(xxssMeta).toBeTruthy();
    });

    test('X-XSS-Protection should enable XSS filtering', () => {
      const xxssMeta = document.querySelector('meta[http-equiv="X-XSS-Protection"]');
      expect(xxssMeta).toBeTruthy();

      const content = xxssMeta.getAttribute('content');
      expect(content).toContain('1');
    });

    test('Referrer-Policy meta tag should be present', () => {
      const rpMeta = document.querySelector('meta[name="referrer"]');
      expect(rpMeta).toBeTruthy();
    });

    test('Referrer-Policy should have secure value', () => {
      const rpMeta = document.querySelector('meta[name="referrer"]');
      expect(rpMeta).toBeTruthy();

      const content = rpMeta.getAttribute('content');
      // Allow various secure referrer policies
      const secureValues = [
        'no-referrer',
        'no-referrer-when-downgrade',
        'strict-origin',
        'strict-origin-when-cross-origin',
        'same-origin',
        'origin-when-cross-origin'
      ];
      expect(secureValues).toContain(content);
    });
  });
});
