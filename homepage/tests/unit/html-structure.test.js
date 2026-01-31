/**
 * Semantic HTML Structure Unit Tests
 * Owner: Scenario 13 - SEO Requirements
 *
 * Tests:
 * - Semantic elements (header, nav, main, section, footer)
 * - Heading hierarchy (single h1)
 * - Landmark roles
 *
 * Requirements: NFR-4
 */

const fs = require('fs');
const path = require('path');

describe('Semantic HTML Structure', () => {
  let htmlContent;
  let document;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf8');
    document = new DOMParser().parseFromString(htmlContent, 'text/html');
  });

  describe('Semantic Elements', () => {
    test('header element exists', () => {
      const header = document.querySelector('header');
      expect(header).not.toBeNull();
    });

    test('nav element exists', () => {
      const nav = document.querySelector('nav');
      expect(nav).not.toBeNull();
    });

    test('main element exists', () => {
      const main = document.querySelector('main');
      expect(main).not.toBeNull();
    });

    test('section elements exist', () => {
      const sections = document.querySelectorAll('section');
      expect(sections.length).toBeGreaterThan(0);
    });

    test('footer element exists', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();
    });

    test('page uses all required semantic elements', () => {
      const header = document.querySelector('header');
      const nav = document.querySelector('nav');
      const main = document.querySelector('main');
      const sections = document.querySelectorAll('section');
      const footer = document.querySelector('footer');

      expect(header).not.toBeNull();
      expect(nav).not.toBeNull();
      expect(main).not.toBeNull();
      expect(sections.length).toBeGreaterThan(0);
      expect(footer).not.toBeNull();
    });
  });

  describe('Heading Hierarchy', () => {
    test('page has exactly one h1 element', () => {
      const h1Elements = document.querySelectorAll('h1');
      expect(h1Elements.length).toBe(1);
    });

    test('h1 contains MirDB', () => {
      const h1 = document.querySelector('h1');
      expect(h1).not.toBeNull();
      expect(h1.textContent).toContain('MirDB');
    });

    test('h2 elements follow h1', () => {
      const h2Elements = document.querySelectorAll('h2');
      expect(h2Elements.length).toBeGreaterThan(0);
    });

    test('heading hierarchy is proper (no skipping levels)', () => {
      const headings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      let previousLevel = 0;

      headings.forEach((heading) => {
        const currentLevel = parseInt(heading.tagName.charAt(1), 10);
        // Can go deeper by only 1 level at a time, or go to same level, or go back up
        if (previousLevel > 0 && currentLevel > previousLevel + 1) {
          // This would be skipping a level (e.g., h1 -> h3)
          expect(currentLevel).toBeLessThanOrEqual(previousLevel + 1);
        }
        previousLevel = currentLevel;
      });
    });

    test('h1 appears before h2s', () => {
      const h1 = document.querySelector('h1');
      const firstH2 = document.querySelector('h2');

      if (h1 && firstH2) {
        const h1Position = h1.compareDocumentPosition(firstH2);
        // Node.DOCUMENT_POSITION_FOLLOWING = 4
        expect(h1Position & 4).toBe(4);
      }
    });
  });

  describe('ARIA Landmarks and Roles', () => {
    test('header has banner role', () => {
      const header = document.querySelector('header');
      // header element has implicit banner role when not nested
      expect(header).not.toBeNull();
      const role = header.getAttribute('role');
      // Either no role (implicit) or explicit banner
      expect(!role || role === 'banner').toBe(true);
    });

    test('nav has navigation role', () => {
      const nav = document.querySelector('nav');
      expect(nav).not.toBeNull();
      const role = nav.getAttribute('role');
      // Either no role (implicit) or explicit navigation
      expect(!role || role === 'navigation').toBe(true);
    });

    test('main has main role', () => {
      const main = document.querySelector('main');
      expect(main).not.toBeNull();
      const role = main.getAttribute('role');
      // Either no role (implicit) or explicit main
      expect(!role || role === 'main').toBe(true);
    });

    test('footer has contentinfo role', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();
      const role = footer.getAttribute('role');
      // Either no role (implicit) or explicit contentinfo
      expect(!role || role === 'contentinfo').toBe(true);
    });

    test('sections have accessible names', () => {
      const sections = document.querySelectorAll('main > section');
      sections.forEach((section) => {
        const hasAriaLabel = section.hasAttribute('aria-label');
        const hasAriaLabelledBy = section.hasAttribute('aria-labelledby');
        const hasHeading = section.querySelector('h2, h3');
        // Section should have some form of accessible name
        expect(hasAriaLabel || hasAriaLabelledBy || hasHeading).toBe(true);
      });
    });
  });

  describe('Document Structure', () => {
    test('document has proper DOCTYPE', () => {
      expect(htmlContent.toLowerCase().trim().startsWith('<!doctype html>')).toBe(true);
    });

    test('html element has lang attribute', () => {
      const html = document.querySelector('html');
      expect(html).not.toBeNull();
      const lang = html.getAttribute('lang');
      expect(lang).toBeTruthy();
      expect(lang.length).toBeGreaterThanOrEqual(2);
    });

    test('head element exists', () => {
      const head = document.querySelector('head');
      expect(head).not.toBeNull();
    });

    test('body element exists', () => {
      const body = document.querySelector('body');
      expect(body).not.toBeNull();
    });
  });
});
