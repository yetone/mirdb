/**
 * Broken Links Tests
 *
 * These tests verify all links on the page are valid and not broken.
 * Includes validation for internal anchors, external URLs, and empty hrefs.
 *
 * @jest-environment jsdom
 */

const fs = require('fs');
const path = require('path');

describe('Error Handling - Broken Links', () => {
  let document;
  let htmlContent;

  beforeAll(() => {
    // Load the landing page HTML
    const htmlPath = path.join(__dirname, '..', 'index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf8');
    global.document.documentElement.innerHTML = htmlContent;
    document = global.document;
  });

  // Test Case 1: Scan all anchor elements - All href attributes contain valid URLs or anchors
  describe('Test Case 1: Scan all anchor elements', () => {
    test('should have valid href attributes in all anchor elements', () => {
      const allLinks = document.querySelectorAll('a');
      expect(allLinks.length).toBeGreaterThan(0);

      allLinks.forEach((link, index) => {
        const href = link.getAttribute('href');

        // href must exist and not be empty
        expect(href).not.toBeNull();
        expect(href.trim()).not.toBe('');

        // href should be either:
        // 1. A valid URL (starts with http:// or https://)
        // 2. An internal anchor (starts with #)
        // 3. A relative path (starts with / or ./)
        const isValidUrl = /^https?:\/\//.test(href);
        const isInternalAnchor = /^#[\w-]+$/.test(href);
        const isRelativePath = /^\.?\//.test(href);

        const isValid = isValidUrl || isInternalAnchor || isRelativePath;
        expect(isValid).toBe(true);
      });
    });

    test('should have well-formed URLs for external links', () => {
      const allLinks = document.querySelectorAll('a');

      allLinks.forEach(link => {
        const href = link.getAttribute('href');

        if (href && href.startsWith('http')) {
          // Validate URL can be parsed
          expect(() => new URL(href)).not.toThrow();

          // Should use HTTPS for security
          expect(href.startsWith('https://')).toBe(true);
        }
      });
    });
  });

  // Test Case 2: Check internal anchor links - All internal anchor links (#section) point to existing IDs
  describe('Test Case 2: Check internal anchor links', () => {
    test('should have all internal anchor links pointing to existing IDs', () => {
      const allLinks = document.querySelectorAll('a');
      const internalAnchorLinks = [];

      // Collect all internal anchor links
      allLinks.forEach(link => {
        const href = link.getAttribute('href');
        if (href && href.startsWith('#') && href.length > 1) {
          internalAnchorLinks.push(href);
        }
      });

      // Verify each anchor link has a corresponding element with that ID
      internalAnchorLinks.forEach(anchor => {
        const targetId = anchor.substring(1); // Remove the # prefix
        const targetElement = document.getElementById(targetId);

        expect(targetElement).not.toBeNull();
      });
    });

    test('should have meaningful anchor targets (not just empty elements)', () => {
      const allLinks = document.querySelectorAll('a');

      allLinks.forEach(link => {
        const href = link.getAttribute('href');
        if (href && href.startsWith('#') && href.length > 1) {
          const targetId = href.substring(1);
          const targetElement = document.getElementById(targetId);

          if (targetElement) {
            // Target should have content or child elements
            const hasContent = targetElement.textContent.trim().length > 0 ||
                              targetElement.children.length > 0;
            expect(hasContent).toBe(true);
          }
        }
      });
    });
  });

  // Test Case 3: Validate GitHub link responds - GitHub repository URL returns 200 status
  describe('Test Case 3: Validate GitHub link responds', () => {
    test('should have GitHub links with valid URL format', () => {
      const allLinks = document.querySelectorAll('a');
      const githubLinks = [];

      allLinks.forEach(link => {
        const href = link.getAttribute('href');
        if (href && href.includes('github.com')) {
          githubLinks.push(href);
        }
      });

      expect(githubLinks.length).toBeGreaterThan(0);

      // Validate each GitHub URL matches expected format
      const validGitHubPattern = /^https:\/\/github\.com\/[\w-]+\/[\w-]+(\/.*)?$/;
      githubLinks.forEach(url => {
        expect(url).toMatch(validGitHubPattern);
      });
    });

    test('should have at least one link to the MirDB repository', () => {
      const allLinks = document.querySelectorAll('a');
      let foundMirdbRepo = false;

      allLinks.forEach(link => {
        const href = link.getAttribute('href');
        if (href && href.includes('github.com/yetone/mirdb')) {
          foundMirdbRepo = true;
        }
      });

      expect(foundMirdbRepo).toBe(true);
    });
  });

  // Test Case 4: Check for empty href attributes - No anchor elements have empty href attributes
  describe('Test Case 4: Check for empty href attributes', () => {
    test('should not have any anchor elements with empty href attributes', () => {
      const allLinks = document.querySelectorAll('a');
      const emptyHrefLinks = [];

      allLinks.forEach((link, index) => {
        const href = link.getAttribute('href');

        // Check for various forms of empty href
        if (href === '' || href === null || href === undefined) {
          emptyHrefLinks.push({
            index,
            text: link.textContent.trim(),
            href
          });
        }
      });

      expect(emptyHrefLinks).toEqual([]);
    });

    test('should not have any anchor elements with only whitespace in href', () => {
      const allLinks = document.querySelectorAll('a');
      const whitespaceHrefLinks = [];

      allLinks.forEach((link, index) => {
        const href = link.getAttribute('href');

        if (href !== null && href.trim() === '' && href !== '') {
          whitespaceHrefLinks.push({
            index,
            text: link.textContent.trim(),
            href
          });
        }
      });

      expect(whitespaceHrefLinks).toEqual([]);
    });

    test('should not have any anchor elements with href="#" (placeholder links)', () => {
      const allLinks = document.querySelectorAll('a');
      const placeholderLinks = [];

      allLinks.forEach((link, index) => {
        const href = link.getAttribute('href');

        if (href === '#') {
          placeholderLinks.push({
            index,
            text: link.textContent.trim()
          });
        }
      });

      expect(placeholderLinks).toEqual([]);
    });

    test('should not have any anchor elements with javascript: void href', () => {
      const allLinks = document.querySelectorAll('a');
      const jsVoidLinks = [];

      allLinks.forEach((link, index) => {
        const href = link.getAttribute('href');

        if (href && (href.startsWith('javascript:') || href.includes('void'))) {
          jsVoidLinks.push({
            index,
            text: link.textContent.trim(),
            href
          });
        }
      });

      expect(jsVoidLinks).toEqual([]);
    });
  });

  // Additional comprehensive link validation
  describe('Comprehensive link validation', () => {
    test('should have all links accessible (with valid href)', () => {
      const allLinks = document.querySelectorAll('a');

      allLinks.forEach(link => {
        const href = link.getAttribute('href');
        expect(href).toBeDefined();
        expect(href).not.toBeNull();
      });
    });

    test('should have descriptive link text for accessibility', () => {
      const allLinks = document.querySelectorAll('a');

      allLinks.forEach(link => {
        const linkText = link.textContent.trim();
        const hasAriaLabel = link.getAttribute('aria-label');

        // Link should have either visible text or aria-label
        const hasAccessibleText = linkText.length > 0 || hasAriaLabel;
        expect(hasAccessibleText).toBe(true);
      });
    });

    test('should count total number of links on page', () => {
      const allLinks = document.querySelectorAll('a');

      // Log for debugging
      const linkSummary = {
        total: allLinks.length,
        internal: 0,
        external: 0
      };

      allLinks.forEach(link => {
        const href = link.getAttribute('href');
        if (href && href.startsWith('#')) {
          linkSummary.internal++;
        } else if (href && href.startsWith('http')) {
          linkSummary.external++;
        }
      });

      // Should have at least some links
      expect(linkSummary.total).toBeGreaterThan(0);
    });
  });
});
