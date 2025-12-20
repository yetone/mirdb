/**
 * Unit Tests for Error Handling - Broken Links
 * Scenario: Verify that all internal and external links on the page are valid and functional
 *
 * Test Case 1: Extract all href attributes and filter internal links
 * Test Case 3: Check for empty or malformed href attributes
 */

const fs = require('fs');
const path = require('path');
const { JSDOM } = require('jsdom');

describe('Error Handling - Broken Links (Unit Tests)', () => {
  let document;
  let dom;
  let allLinks;

  beforeAll(() => {
    const htmlPath = path.resolve(__dirname, '../../index.html');
    const html = fs.readFileSync(htmlPath, 'utf-8');
    dom = new JSDOM(html, { url: 'http://localhost' });
    document = dom.window.document;
    allLinks = document.querySelectorAll('a[href]');
  });

  /**
   * Test Case 1: Extract all href attributes and filter internal links
   * Expected: All internal links (#anchors) navigate to existing sections
   */
  describe('Test Case 1: Internal Link Validation', () => {
    test('should extract all anchor tags from the page', () => {
      // Verify we can extract links from the page
      expect(allLinks.length).toBeGreaterThan(0);
    });

    test('should identify internal anchor links (#anchors)', () => {
      const internalLinks = document.querySelectorAll('a[href^="#"]');
      expect(internalLinks.length).toBeGreaterThan(0);
    });

    test('should have all internal anchor links point to existing sections', () => {
      const internalLinks = document.querySelectorAll('a[href^="#"]');
      const brokenLinks = [];

      internalLinks.forEach(link => {
        const href = link.getAttribute('href');
        // Skip empty hash links (they scroll to top, which is valid behavior for some designs)
        if (href === '#') return;

        const targetId = href.replace('#', '');
        const target = document.getElementById(targetId);

        if (!target) {
          brokenLinks.push({
            href: href,
            linkText: link.textContent.trim(),
            targetId: targetId
          });
        }
      });

      if (brokenLinks.length > 0) {
        console.error('Broken internal links found:', brokenLinks);
      }

      expect(brokenLinks).toHaveLength(0);
    });

    test('should verify navigation menu links point to existing sections', () => {
      const navLinks = document.querySelectorAll('nav a[href^="#"]');

      navLinks.forEach(link => {
        const href = link.getAttribute('href');
        if (href === '#') return;

        const targetId = href.replace('#', '');
        const target = document.getElementById(targetId);
        expect(target).not.toBeNull();
      });
    });

    test('should have all expected section IDs present in the DOM', () => {
      const expectedSections = ['features', 'quick-start', 'configuration'];

      expectedSections.forEach(sectionId => {
        const section = document.getElementById(sectionId);
        expect(section).not.toBeNull();
      });
    });

    test('should have consistent internal link destinations', () => {
      const internalLinks = document.querySelectorAll('a[href^="#"]');
      const linkDestinations = new Map();

      internalLinks.forEach(link => {
        const href = link.getAttribute('href');
        if (href === '#') return;

        const targetId = href.replace('#', '');
        const target = document.getElementById(targetId);

        // Record all destinations
        if (!linkDestinations.has(targetId)) {
          linkDestinations.set(targetId, {
            exists: target !== null,
            linksCount: 1
          });
        } else {
          linkDestinations.get(targetId).linksCount++;
        }
      });

      // All destinations should exist
      linkDestinations.forEach((value, key) => {
        expect(value.exists).toBe(true);
      });
    });
  });

  /**
   * Test Case 3: Check for empty or malformed href attributes
   * Expected: No empty href='', href='#', or javascript: links
   */
  describe('Test Case 3: Malformed href Detection', () => {
    test('should not have empty href attributes', () => {
      const emptyHrefLinks = document.querySelectorAll('a[href=""]');
      const linksWithEmptyHref = Array.from(emptyHrefLinks).map(link => ({
        text: link.textContent.trim(),
        outerHTML: link.outerHTML.substring(0, 100)
      }));

      if (linksWithEmptyHref.length > 0) {
        console.error('Links with empty href found:', linksWithEmptyHref);
      }

      expect(emptyHrefLinks.length).toBe(0);
    });

    test('should not have javascript: protocol links', () => {
      const jsLinks = document.querySelectorAll('a[href^="javascript:"]');
      const javascriptLinks = Array.from(jsLinks).map(link => ({
        text: link.textContent.trim(),
        href: link.getAttribute('href')
      }));

      if (javascriptLinks.length > 0) {
        console.error('JavaScript protocol links found:', javascriptLinks);
      }

      expect(jsLinks.length).toBe(0);
    });

    test('should not have void(0) links', () => {
      const voidLinks = document.querySelectorAll('a[href*="void(0)"]');
      expect(voidLinks.length).toBe(0);
    });

    test('should not have malformed URL protocols', () => {
      const malformedProtocols = ['file:', 'data:', 'javascript:', 'vbscript:'];
      let malformedLinks = [];

      allLinks.forEach(link => {
        const href = link.getAttribute('href');
        malformedProtocols.forEach(protocol => {
          if (href && href.toLowerCase().startsWith(protocol)) {
            malformedLinks.push({
              href: href,
              text: link.textContent.trim(),
              protocol: protocol
            });
          }
        });
      });

      if (malformedLinks.length > 0) {
        console.error('Malformed protocol links found:', malformedLinks);
      }

      expect(malformedLinks).toHaveLength(0);
    });

    test('should have all links with non-whitespace href values', () => {
      const linksWithWhitespaceHref = [];

      allLinks.forEach(link => {
        const href = link.getAttribute('href');
        if (href && href.trim() === '') {
          linksWithWhitespaceHref.push({
            text: link.textContent.trim(),
            href: href
          });
        }
      });

      expect(linksWithWhitespaceHref).toHaveLength(0);
    });

    test('should verify placeholder # links are intentional (logo link to top)', () => {
      const hashOnlyLinks = document.querySelectorAll('a[href="#"]');

      // Only the logo link should have href="#" to scroll to top
      // Other navigation should use specific anchors
      hashOnlyLinks.forEach(link => {
        // Check if this is a logo/home link (acceptable use of #)
        const isLogoLink = link.classList.contains('logo-link') ||
                          link.getAttribute('aria-label')?.toLowerCase().includes('home') ||
                          link.closest('header') && link.querySelector('svg');

        if (!isLogoLink) {
          // Non-logo links with just # are suspicious
          console.warn('Non-logo link with href="#" found:', link.textContent.trim());
        }
      });

      // There should be at most 1 hash-only link (the logo)
      expect(hashOnlyLinks.length).toBeLessThanOrEqual(1);
    });

    test('should have all external links use HTTPS protocol', () => {
      const httpLinks = document.querySelectorAll('a[href^="http://"]');
      const insecureLinks = Array.from(httpLinks).map(link => ({
        href: link.getAttribute('href'),
        text: link.textContent.trim()
      }));

      if (insecureLinks.length > 0) {
        console.error('Insecure HTTP links found:', insecureLinks);
      }

      expect(httpLinks.length).toBe(0);
    });
  });

  /**
   * Additional Link Quality Tests
   */
  describe('Link Quality and Accessibility', () => {
    test('should have all external links with proper security attributes', () => {
      const externalLinks = document.querySelectorAll('a[href^="http"]');

      externalLinks.forEach(link => {
        const target = link.getAttribute('target');
        const rel = link.getAttribute('rel');

        if (target === '_blank') {
          expect(rel).toContain('noopener');
        }
      });
    });

    test('should have all links with accessible text content', () => {
      const linksWithoutText = [];

      allLinks.forEach(link => {
        const text = link.textContent.trim();
        const ariaLabel = link.getAttribute('aria-label');
        const hasImage = link.querySelector('img[alt]');
        const hasSvg = link.querySelector('svg');

        // Link should have text, aria-label, or image with alt text
        const isAccessible = text !== '' || ariaLabel || hasImage || hasSvg;

        if (!isAccessible) {
          linksWithoutText.push({
            href: link.getAttribute('href'),
            outerHTML: link.outerHTML.substring(0, 100)
          });
        }
      });

      expect(linksWithoutText).toHaveLength(0);
    });

    test('should have valid URL format for external links', () => {
      const externalLinks = document.querySelectorAll('a[href^="http"]');
      const invalidUrls = [];

      externalLinks.forEach(link => {
        const href = link.getAttribute('href');
        try {
          new URL(href);
        } catch (e) {
          invalidUrls.push({
            href: href,
            text: link.textContent.trim(),
            error: e.message
          });
        }
      });

      if (invalidUrls.length > 0) {
        console.error('Invalid URL format found:', invalidUrls);
      }

      expect(invalidUrls).toHaveLength(0);
    });

    test('should identify all link types correctly', () => {
      const linkTypes = {
        internal: document.querySelectorAll('a[href^="#"]').length,
        external: document.querySelectorAll('a[href^="http"]').length,
        relative: document.querySelectorAll('a[href]:not([href^="#"]):not([href^="http"]):not([href^="mailto:"]):not([href^="tel:"])').length
      };

      // Should have internal navigation links
      expect(linkTypes.internal).toBeGreaterThan(0);

      // Should have external links (GitHub, rustup, etc.)
      expect(linkTypes.external).toBeGreaterThan(0);

      // Log link distribution for visibility
      console.log('Link type distribution:', linkTypes);
    });
  });

  /**
   * Comprehensive Link Inventory
   */
  describe('Link Inventory and Validation', () => {
    test('should catalog all links in the document', () => {
      const linkInventory = {
        total: allLinks.length,
        byLocation: {
          header: document.querySelectorAll('header a[href]').length,
          main: document.querySelectorAll('main a[href]').length,
          footer: document.querySelectorAll('footer a[href]').length,
          nav: document.querySelectorAll('nav a[href]').length
        },
        byType: {
          internal: document.querySelectorAll('a[href^="#"]').length,
          external: document.querySelectorAll('a[href^="https://"]').length,
          insecure: document.querySelectorAll('a[href^="http://"]').length
        }
      };

      // Log inventory for visibility
      console.log('Link inventory:', JSON.stringify(linkInventory, null, 2));

      // Basic validations
      expect(linkInventory.total).toBeGreaterThan(0);
      expect(linkInventory.byType.insecure).toBe(0);
    });

    test('should verify all GitHub links point to valid repositories', () => {
      const githubLinks = document.querySelectorAll('a[href*="github.com"]');
      const githubUrls = [];

      githubLinks.forEach(link => {
        const href = link.getAttribute('href');
        githubUrls.push({
          href: href,
          text: link.textContent.trim(),
          matchesPattern: /^https:\/\/github\.com\/[\w-]+\/[\w-]+/.test(href)
        });
      });

      // All GitHub links should match valid repository pattern
      githubUrls.forEach(urlInfo => {
        expect(urlInfo.matchesPattern).toBe(true);
      });
    });

    test('should verify footer contains required links', () => {
      const footer = document.querySelector('footer');
      const footerLinks = footer.querySelectorAll('a[href]');

      const requiredLinkTexts = ['github', 'documentation', 'license'];
      const foundLinks = Array.from(footerLinks).map(l => l.textContent.toLowerCase());

      requiredLinkTexts.forEach(requiredText => {
        const found = foundLinks.some(text => text.includes(requiredText));
        expect(found).toBe(true);
      });
    });
  });
});
