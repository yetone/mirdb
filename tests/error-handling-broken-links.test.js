/**
 * Tests for Error Handling - Broken Links
 * Scenario ID: 21
 * UUID: 175f2e42-09c8-427b-a2ae-93526c9ab029
 *
 * Test Cases:
 * 1. Check all anchor href attributes for empty or malformed values
 * 2. Verify internal anchor links (#section) point to existing elements
 * 3. Test external links return 200 status (or appropriate redirect)
 */

const fs = require('fs');
const path = require('path');

describe('Error Handling - Broken Links', () => {
  let document;
  let htmlContent;

  beforeEach(() => {
    htmlContent = fs.readFileSync(path.resolve(__dirname, '../index.html'), 'utf8');
    document = new DOMParser().parseFromString(htmlContent, 'text/html');
  });

  /**
   * Test Case 1: Check all anchor href attributes
   * Expected: No empty or malformed href values
   */
  describe('Test Case 1: Anchor href Validation', () => {
    test('should have no anchor tags with empty href attribute', () => {
      const allLinks = document.querySelectorAll('a');
      const emptyHrefLinks = Array.from(allLinks).filter(link => {
        const href = link.getAttribute('href');
        return href === '' || href === null;
      });

      expect(emptyHrefLinks).toHaveLength(0);
    });

    test('should have no anchor tags with href="#" only (placeholder links)', () => {
      const allLinks = document.querySelectorAll('a');
      const placeholderLinks = Array.from(allLinks).filter(link => {
        const href = link.getAttribute('href');
        return href === '#';
      });

      // Placeholder links should not exist (they indicate incomplete implementation)
      expect(placeholderLinks).toHaveLength(0);
    });

    test('should have no anchor tags with javascript: protocol', () => {
      const allLinks = document.querySelectorAll('a');
      const jsLinks = Array.from(allLinks).filter(link => {
        const href = link.getAttribute('href') || '';
        return href.toLowerCase().startsWith('javascript:');
      });

      expect(jsLinks).toHaveLength(0);
    });

    test('should have no anchor tags with void(0) href', () => {
      const allLinks = document.querySelectorAll('a');
      const voidLinks = Array.from(allLinks).filter(link => {
        const href = link.getAttribute('href') || '';
        return href.toLowerCase().includes('void(0)');
      });

      expect(voidLinks).toHaveLength(0);
    });

    test('all anchor tags should have valid href format', () => {
      const allLinks = document.querySelectorAll('a');
      const invalidLinks = [];

      Array.from(allLinks).forEach(link => {
        const href = link.getAttribute('href');
        if (!href) {
          invalidLinks.push({ href, reason: 'missing href' });
          return;
        }

        // Valid formats: internal anchors (#id), relative paths, absolute URLs
        const isValidInternalAnchor = href.startsWith('#') && href.length > 1;
        const isValidRelativePath = !href.startsWith('#') && !href.startsWith('http') && href.length > 0;
        const isValidAbsoluteUrl = href.startsWith('http://') || href.startsWith('https://');
        const isValidMailto = href.startsWith('mailto:');
        const isValidTel = href.startsWith('tel:');

        if (!isValidInternalAnchor && !isValidRelativePath && !isValidAbsoluteUrl && !isValidMailto && !isValidTel) {
          invalidLinks.push({ href, reason: 'invalid format' });
        }
      });

      expect(invalidLinks).toHaveLength(0);
    });

    test('external URLs should be properly formatted with protocol', () => {
      const allLinks = document.querySelectorAll('a');
      const externalLinks = Array.from(allLinks).filter(link => {
        const href = link.getAttribute('href') || '';
        return href.includes('github.com') || href.includes('://');
      });

      externalLinks.forEach(link => {
        const href = link.getAttribute('href');
        // External links must have a protocol
        expect(href).toMatch(/^https?:\/\//);
      });
    });
  });

  /**
   * Test Case 2: Verify internal anchor links
   * Expected: Internal anchor links (#section) point to existing elements
   */
  describe('Test Case 2: Internal Anchor Links Validation', () => {
    test('should identify all internal anchor links', () => {
      const allLinks = document.querySelectorAll('a');
      const internalAnchorLinks = Array.from(allLinks).filter(link => {
        const href = link.getAttribute('href') || '';
        return href.startsWith('#') && href.length > 1;
      });

      // The page should have at least one internal link (Get Started button)
      expect(internalAnchorLinks.length).toBeGreaterThanOrEqual(1);
    });

    test('all internal anchor links should point to existing elements', () => {
      const allLinks = document.querySelectorAll('a');
      const internalAnchorLinks = Array.from(allLinks).filter(link => {
        const href = link.getAttribute('href') || '';
        return href.startsWith('#') && href.length > 1;
      });

      const brokenInternalLinks = [];

      internalAnchorLinks.forEach(link => {
        const href = link.getAttribute('href');
        const targetId = href.substring(1); // Remove the # prefix
        const targetElement = document.getElementById(targetId);

        if (!targetElement) {
          brokenInternalLinks.push({
            href,
            targetId,
            linkText: link.textContent.trim()
          });
        }
      });

      // All internal anchor links should point to existing elements
      expect(brokenInternalLinks).toHaveLength(0);
    });

    test('Get Started button should link to existing section', () => {
      const getStartedLink = document.querySelector('a[href="#getting-started"]');
      expect(getStartedLink).not.toBeNull();

      const gettingStartedSection = document.getElementById('getting-started');
      expect(gettingStartedSection).not.toBeNull();
    });

    test('all section IDs referenced in navigation should exist', () => {
      const navLinks = document.querySelectorAll('nav a, .hero a, header a');
      const internalNavLinks = Array.from(navLinks).filter(link => {
        const href = link.getAttribute('href') || '';
        return href.startsWith('#') && href.length > 1;
      });

      internalNavLinks.forEach(link => {
        const href = link.getAttribute('href');
        const targetId = href.substring(1);
        const targetElement = document.getElementById(targetId);

        expect(targetElement).not.toBeNull();
      });
    });

    test('internal anchor targets should be visible sections', () => {
      const allLinks = document.querySelectorAll('a');
      const internalAnchorLinks = Array.from(allLinks).filter(link => {
        const href = link.getAttribute('href') || '';
        return href.startsWith('#') && href.length > 1;
      });

      internalAnchorLinks.forEach(link => {
        const href = link.getAttribute('href');
        const targetId = href.substring(1);
        const targetElement = document.getElementById(targetId);

        expect(targetElement).not.toBeNull();
        // Target should be a section, div, or other block element
        expect(['SECTION', 'DIV', 'ARTICLE', 'MAIN', 'HEADER', 'FOOTER', 'NAV', 'H1', 'H2', 'H3', 'H4', 'H5', 'H6']).toContain(targetElement.tagName);
      });
    });
  });

  /**
   * Test Case 3: Test external links
   * Expected: External links return 200 status (or appropriate redirect)
   *
   * Note: This is an integration test. In a unit test environment,
   * we validate the URL format and structure. Actual HTTP status
   * checks would require a network request.
   */
  describe('Test Case 3: External Links Validation', () => {
    test('should identify all external links', () => {
      const allLinks = document.querySelectorAll('a');
      const externalLinks = Array.from(allLinks).filter(link => {
        const href = link.getAttribute('href') || '';
        return href.startsWith('http://') || href.startsWith('https://');
      });

      // The page should have external links (GitHub, documentation)
      expect(externalLinks.length).toBeGreaterThanOrEqual(1);
    });

    test('external links should use HTTPS protocol', () => {
      const allLinks = document.querySelectorAll('a');
      const httpLinks = Array.from(allLinks).filter(link => {
        const href = link.getAttribute('href') || '';
        return href.startsWith('http://');
      });

      // All external links should use HTTPS for security
      expect(httpLinks).toHaveLength(0);
    });

    test('GitHub links should have valid repository URL structure', () => {
      const allLinks = document.querySelectorAll('a');
      const githubLinks = Array.from(allLinks).filter(link => {
        const href = link.getAttribute('href') || '';
        return href.includes('github.com');
      });

      expect(githubLinks.length).toBeGreaterThanOrEqual(1);

      githubLinks.forEach(link => {
        const href = link.getAttribute('href');
        // Valid GitHub URL patterns
        const isValidGithubUrl = href.match(/^https:\/\/github\.com\/[\w-]+\/[\w-]+(#[\w-]*)?$/);
        expect(isValidGithubUrl).not.toBeNull();
      });
    });

    test('external links should have security attributes', () => {
      const allLinks = document.querySelectorAll('a');
      const externalLinks = Array.from(allLinks).filter(link => {
        const href = link.getAttribute('href') || '';
        return href.startsWith('http://') || href.startsWith('https://');
      });

      externalLinks.forEach(link => {
        const target = link.getAttribute('target');
        const rel = link.getAttribute('rel');

        expect(target).toBe('_blank');
        expect(rel).not.toBeNull();
        expect(rel).toContain('noopener');
      });
    });

    test('external links should not have typos in domain names', () => {
      const allLinks = document.querySelectorAll('a');
      const externalLinks = Array.from(allLinks).filter(link => {
        const href = link.getAttribute('href') || '';
        return href.startsWith('https://');
      });

      const knownTypos = [
        'githbu.com',
        'guthub.com',
        'githib.com',
        'githu.com',
        'githb.com'
      ];

      externalLinks.forEach(link => {
        const href = link.getAttribute('href').toLowerCase();
        knownTypos.forEach(typo => {
          expect(href).not.toContain(typo);
        });
      });
    });

    test('all external links should be accessible (well-formed URLs)', () => {
      const allLinks = document.querySelectorAll('a');
      const externalLinks = Array.from(allLinks).filter(link => {
        const href = link.getAttribute('href') || '';
        return href.startsWith('https://');
      });

      externalLinks.forEach(link => {
        const href = link.getAttribute('href');
        // URL should be parseable
        expect(() => new URL(href)).not.toThrow();

        // URL should have a hostname
        const url = new URL(href);
        expect(url.hostname).toBeTruthy();
        expect(url.hostname.length).toBeGreaterThan(0);
      });
    });
  });

  /**
   * Additional comprehensive tests for link integrity
   */
  describe('Additional Link Integrity Tests', () => {
    test('should report total number of links on the page', () => {
      const allLinks = document.querySelectorAll('a');
      // Just verify we have links and log count
      expect(allLinks.length).toBeGreaterThan(0);
    });

    test('no duplicate anchor IDs should exist', () => {
      const allElementsWithId = document.querySelectorAll('[id]');
      const ids = Array.from(allElementsWithId).map(el => el.id);
      const uniqueIds = [...new Set(ids)];

      expect(ids.length).toBe(uniqueIds.length);
    });

    test('all links should have accessible text content', () => {
      const allLinks = document.querySelectorAll('a');

      Array.from(allLinks).forEach(link => {
        const textContent = link.textContent.trim();
        const ariaLabel = link.getAttribute('aria-label');
        const title = link.getAttribute('title');

        // Link should have some accessible text
        const hasAccessibleText = textContent.length > 0 || ariaLabel || title;
        expect(hasAccessibleText).toBeTruthy();
      });
    });

    test('CSS link in head should be valid', () => {
      const cssLink = document.querySelector('link[rel="stylesheet"]');
      expect(cssLink).not.toBeNull();

      const href = cssLink.getAttribute('href');
      expect(href).not.toBeNull();
      expect(href).not.toBe('');
      expect(href).toMatch(/\.css$/);
    });
  });
});
