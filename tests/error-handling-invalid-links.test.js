/**
 * Error Handling - Invalid Links Tests
 * Scenario: Verify that all internal and external links are valid
 *
 * Test Cases:
 * 1. All anchor href values have corresponding id elements on page
 * 2. All external links use https protocol
 * 3. GitHub link points to valid github.com URL
 */

const fs = require('fs');
const path = require('path');

describe('Error Handling - Invalid Links', () => {
  let document;

  beforeAll(() => {
    // Load the index.html file
    const htmlPath = path.resolve(__dirname, '../index.html');
    const html = fs.readFileSync(htmlPath, 'utf8');
    document = new DOMParser().parseFromString(html, 'text/html');
  });

  describe('Test Case 1: Internal anchor links have corresponding id elements', () => {
    test('should collect all anchor tags with href starting with #', () => {
      // Step 1: Collect all anchor tags
      const internalLinks = document.querySelectorAll('a[href^="#"]');

      // Should have internal anchor links
      expect(internalLinks.length).toBeGreaterThan(0);
    });

    test('all internal anchor href values should have corresponding id elements', () => {
      // Step 2: Verify internal anchors - Check that all #anchor links have corresponding ids
      const internalLinks = document.querySelectorAll('a[href^="#"]');
      const missingTargets = [];
      const validatedLinks = [];

      internalLinks.forEach((link) => {
        const href = link.getAttribute('href');
        if (href && href.startsWith('#') && href.length > 1) {
          const targetId = href.substring(1);
          const targetElement = document.getElementById(targetId);

          if (!targetElement) {
            missingTargets.push({
              href: href,
              linkText: link.textContent.trim(),
              targetId: targetId
            });
          } else {
            validatedLinks.push({
              href: href,
              targetId: targetId,
              targetTag: targetElement.tagName.toLowerCase()
            });
          }
        }
      });

      // All anchor href values should have corresponding id elements on page
      expect(missingTargets).toEqual([]);
      expect(validatedLinks.length).toBeGreaterThan(0);
    });

    test('internal anchor links in navigation should have corresponding sections', () => {
      const nav = document.querySelector('nav');
      expect(nav).not.toBeNull();

      const navInternalLinks = nav.querySelectorAll('a[href^="#"]');

      navInternalLinks.forEach((link) => {
        const href = link.getAttribute('href');
        if (href && href.length > 1) {
          const targetId = href.substring(1);
          const targetElement = document.getElementById(targetId);

          // Each navigation anchor should have a corresponding element
          expect(targetElement).not.toBeNull();
        }
      });
    });

    test('internal anchor links in main content should have valid targets', () => {
      const main = document.querySelector('main');
      expect(main).not.toBeNull();

      const mainInternalLinks = main.querySelectorAll('a[href^="#"]');

      mainInternalLinks.forEach((link) => {
        const href = link.getAttribute('href');
        if (href && href.length > 1) {
          const targetId = href.substring(1);
          const targetElement = document.getElementById(targetId);

          // Each internal anchor should have a corresponding element
          expect(targetElement).not.toBeNull();
        }
      });
    });
  });

  describe('Test Case 2: External links use https protocol', () => {
    test('should collect all external links', () => {
      // Step 1: Collect all anchor tags - find external URLs
      const allLinks = document.querySelectorAll('a[href]');
      const externalLinks = [];

      allLinks.forEach((link) => {
        const href = link.getAttribute('href');
        if (href && (href.startsWith('http://') || href.startsWith('https://'))) {
          externalLinks.push({
            href: href,
            text: link.textContent.trim()
          });
        }
      });

      // Should have some external links
      expect(externalLinks.length).toBeGreaterThan(0);
    });

    test('all external links should use https protocol', () => {
      // Step 3: Verify external links - Check that external URLs are properly formatted
      const allLinks = document.querySelectorAll('a[href]');
      const httpLinks = [];
      const httpsLinks = [];

      allLinks.forEach((link) => {
        const href = link.getAttribute('href');
        if (href) {
          if (href.startsWith('http://')) {
            httpLinks.push({
              href: href,
              text: link.textContent.trim()
            });
          } else if (href.startsWith('https://')) {
            httpsLinks.push({
              href: href,
              text: link.textContent.trim()
            });
          }
        }
      });

      // All external links should use https protocol (no http:// links)
      expect(httpLinks).toEqual([]);
      expect(httpsLinks.length).toBeGreaterThan(0);
    });

    test('external links should be valid URLs', () => {
      const allLinks = document.querySelectorAll('a[href^="https://"]');

      allLinks.forEach((link) => {
        const href = link.getAttribute('href');

        // Should be a valid URL format
        expect(() => new URL(href)).not.toThrow();

        // URL should have a proper hostname
        const url = new URL(href);
        expect(url.hostname).toBeTruthy();
        expect(url.hostname.includes('.')).toBe(true);
      });
    });
  });

  describe('Test Case 3: GitHub repository link validation', () => {
    test('should have a GitHub link on the page', () => {
      const allLinks = document.querySelectorAll('a[href*="github.com"]');

      // Should have at least one GitHub link
      expect(allLinks.length).toBeGreaterThan(0);
    });

    test('GitHub link should point to valid github.com URL', () => {
      const githubLinks = document.querySelectorAll('a[href*="github.com"]');

      expect(githubLinks.length).toBeGreaterThan(0);

      githubLinks.forEach((link) => {
        const href = link.getAttribute('href');

        // Should use https
        expect(href.startsWith('https://')).toBe(true);

        // Should be a valid URL
        const url = new URL(href);
        expect(url.hostname).toBe('github.com');

        // Should point to a repository (format: github.com/owner/repo)
        const pathParts = url.pathname.split('/').filter(part => part.length > 0);
        expect(pathParts.length).toBeGreaterThanOrEqual(2);
      });
    });

    test('GitHub link in navigation should be valid', () => {
      const nav = document.querySelector('nav');
      expect(nav).not.toBeNull();

      const navGithubLink = nav.querySelector('a[href*="github.com"]');

      if (navGithubLink) {
        const href = navGithubLink.getAttribute('href');

        // Should use https
        expect(href.startsWith('https://')).toBe(true);

        // Should be a valid github.com URL
        const url = new URL(href);
        expect(url.hostname).toBe('github.com');
      }
    });

    test('GitHub link in footer should be valid', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();

      const footerGithubLink = footer.querySelector('a[href*="github.com"]');
      expect(footerGithubLink).not.toBeNull();

      const href = footerGithubLink.getAttribute('href');

      // Should use https
      expect(href.startsWith('https://')).toBe(true);

      // Should be a valid github.com URL
      const url = new URL(href);
      expect(url.hostname).toBe('github.com');

      // Should include mirdb in the URL
      expect(href.toLowerCase()).toContain('mirdb');
    });

    test('GitHub links should have proper attributes for external links', () => {
      const githubLinks = document.querySelectorAll('a[href*="github.com"]');

      githubLinks.forEach((link) => {
        // External links should open in new tab
        expect(link.getAttribute('target')).toBe('_blank');

        // Should have security attributes
        const rel = link.getAttribute('rel') || '';
        expect(rel).toContain('noopener');
        expect(rel).toContain('noreferrer');
      });
    });
  });

  describe('Comprehensive link validation', () => {
    test('should have no broken internal anchor links anywhere on the page', () => {
      const allInternalLinks = document.querySelectorAll('a[href^="#"]');
      const brokenLinks = [];

      allInternalLinks.forEach((link) => {
        const href = link.getAttribute('href');
        if (href && href.length > 1) {
          const targetId = href.substring(1);
          const targetElement = document.getElementById(targetId);

          if (!targetElement) {
            brokenLinks.push({
              href: href,
              text: link.textContent.trim(),
              location: link.closest('section, header, footer, main')?.tagName || 'unknown'
            });
          }
        }
      });

      expect(brokenLinks).toEqual([]);
    });

    test('all links should have href attribute', () => {
      const allAnchors = document.querySelectorAll('a');

      allAnchors.forEach((anchor) => {
        const href = anchor.getAttribute('href');
        expect(href).not.toBeNull();
        expect(href.length).toBeGreaterThan(0);
      });
    });
  });
});
