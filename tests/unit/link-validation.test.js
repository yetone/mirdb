/**
 * Unit tests for Link Validation
 * Scenario: Verify all links on the homepage are valid and functional
 */

const fs = require('fs');
const path = require('path');
const { JSDOM } = require('jsdom');

describe('Link Validation', () => {
  let document;
  let allLinks;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../index.html');
    const html = fs.readFileSync(htmlPath, 'utf-8');
    const dom = new JSDOM(html);
    document = dom.window.document;
    allLinks = document.querySelectorAll('a[href]');
  });

  // Test Case 1: Check all internal anchor links
  describe('TC1: Internal anchor links validation', () => {
    test('all internal anchor links point to existing IDs on the page', () => {
      const internalAnchorLinks = document.querySelectorAll('a[href^="#"]');
      const errors = [];

      internalAnchorLinks.forEach((link) => {
        const href = link.getAttribute('href');
        // Skip empty hash or just '#'
        if (href === '#' || href === '') {
          return;
        }

        const targetId = href.substring(1); // Remove the '#' prefix
        const targetElement = document.getElementById(targetId);

        if (!targetElement) {
          errors.push({
            link: link.outerHTML,
            href: href,
            expectedId: targetId,
          });
        }
      });

      expect(errors).toEqual([]);
    });

    test('Get Started button points to valid quickstart section', () => {
      const getStartedLink = document.querySelector('a[href="#quickstart"]');
      expect(getStartedLink).not.toBeNull();

      const quickstartSection = document.getElementById('quickstart');
      expect(quickstartSection).not.toBeNull();
    });

    test('internal anchor links have smooth scroll targets', () => {
      const internalAnchorLinks = document.querySelectorAll('a[href^="#"]');
      const validAnchors = [];

      internalAnchorLinks.forEach((link) => {
        const href = link.getAttribute('href');
        if (href !== '#' && href !== '') {
          const targetId = href.substring(1);
          const targetElement = document.getElementById(targetId);
          if (targetElement) {
            validAnchors.push({
              href: href,
              targetExists: true,
              targetTagName: targetElement.tagName,
            });
          }
        }
      });

      // All internal anchors should have corresponding elements
      expect(validAnchors.length).toBeGreaterThan(0);
      validAnchors.forEach((anchor) => {
        expect(anchor.targetExists).toBe(true);
      });
    });
  });

  // Test Case 3: Check for empty href attributes
  describe('TC3: Empty href validation', () => {
    test('no links have empty href attributes', () => {
      const linksWithEmptyHref = [];

      allLinks.forEach((link) => {
        const href = link.getAttribute('href');
        if (href === '' || href === null) {
          linksWithEmptyHref.push(link.outerHTML);
        }
      });

      expect(linksWithEmptyHref).toEqual([]);
    });

    test('no links have hash-only href values', () => {
      const hashOnlyLinks = [];

      allLinks.forEach((link) => {
        const href = link.getAttribute('href');
        if (href === '#') {
          hashOnlyLinks.push(link.outerHTML);
        }
      });

      expect(hashOnlyLinks).toEqual([]);
    });

    test('all links have meaningful href values', () => {
      allLinks.forEach((link) => {
        const href = link.getAttribute('href');
        expect(href).toBeTruthy();
        expect(href.trim()).not.toBe('');
        expect(href).not.toBe('#');
      });
    });
  });

  // Additional structural validation
  describe('Link structure validation', () => {
    test('page contains both internal and external links', () => {
      const internalLinks = document.querySelectorAll('a[href^="#"]');
      const externalLinks = document.querySelectorAll('a[href^="http"]');

      expect(internalLinks.length).toBeGreaterThan(0);
      expect(externalLinks.length).toBeGreaterThan(0);
    });

    test('all expected section IDs exist for navigation', () => {
      const expectedSections = ['features', 'quickstart', 'commands', 'roadmap', 'config'];

      expectedSections.forEach((sectionId) => {
        const section = document.getElementById(sectionId);
        expect(section).not.toBeNull();
      });
    });

    test('all external links use HTTPS protocol', () => {
      const externalLinks = document.querySelectorAll('a[href^="http"]');
      const httpLinks = [];

      externalLinks.forEach((link) => {
        const href = link.getAttribute('href');
        if (href.startsWith('http://')) {
          httpLinks.push(href);
        }
      });

      expect(httpLinks).toEqual([]);
    });
  });
});
