/**
 * Test Suite: Accessibility - Screen Reader Compatibility
 * Scenario: Verify the homepage is accessible to screen reader users with proper semantic markup
 *
 * Tests verify:
 * - Proper heading hierarchy (h1-h6)
 * - Image alt text for non-sighted users
 * - ARIA landmarks for quick navigation
 * - Descriptive link text
 *
 * These tests use JSDOM to parse and validate the HTML structure.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { JSDOM } from 'jsdom';
import { readFileSync } from 'fs';
import { resolve } from 'path';

describe('Accessibility - Screen Reader Compatibility', () => {
  let dom;
  let document;

  beforeEach(() => {
    const htmlPath = resolve(__dirname, '../index.html');
    const html = readFileSync(htmlPath, 'utf-8');
    dom = new JSDOM(html, { runScripts: 'dangerously', resources: 'usable' });
    document = dom.window.document;
  });

  afterEach(() => {
    if (dom) {
      dom.window.close();
    }
  });

  /**
   * Test Case 1: Check for single h1 element
   * Expected: Page has exactly one h1 element (main heading)
   */
  describe('Test Case 1: Single h1 element', () => {
    it('should have exactly one h1 element as the main heading', () => {
      const h1Elements = document.querySelectorAll('h1');
      expect(h1Elements.length).toBe(1);
    });

    it('should have meaningful content in the h1 element', () => {
      const h1 = document.querySelector('h1');
      expect(h1).not.toBeNull();
      expect(h1.textContent.trim().length).toBeGreaterThan(0);
    });
  });

  /**
   * Test Case 2: Verify heading hierarchy
   * Expected: Headings follow sequential order without skipping levels
   */
  describe('Test Case 2: Heading hierarchy', () => {
    it('should have headings in sequential order without skipping levels', () => {
      const headings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      expect(headings.length).toBeGreaterThan(0);

      // Track the maximum heading level seen so far
      let maxLevelSeen = 0;

      headings.forEach((heading) => {
        const level = parseInt(heading.tagName.charAt(1), 10);

        // Heading should not skip more than one level (e.g., h1 -> h3 is invalid)
        // A heading can go to same level or one higher, or back to any lower level
        if (level > maxLevelSeen + 1) {
          throw new Error(
            `Invalid heading hierarchy: found ${heading.tagName} after maximum level ${maxLevelSeen}. ` +
            `Headings should not skip levels (e.g., h${maxLevelSeen} should not be followed by h${level} without h${maxLevelSeen + 1}).`
          );
        }

        // Update max level seen (can increase by 1 at most)
        if (level > maxLevelSeen) {
          maxLevelSeen = level;
        }
      });

      // If we get here, hierarchy is valid
      expect(true).toBe(true);
    });

    it('should start with h1 as the first heading', () => {
      const firstHeading = document.querySelector('h1, h2, h3, h4, h5, h6');
      expect(firstHeading).not.toBeNull();
      expect(firstHeading.tagName).toBe('H1');
    });
  });

  /**
   * Test Case 3: Check all images have alt attributes
   * Expected: Every img element has non-empty alt attribute
   */
  describe('Test Case 3: Image alt attributes', () => {
    it('should have alt attribute on all img elements', () => {
      const images = document.querySelectorAll('img');

      images.forEach((img, index) => {
        const alt = img.getAttribute('alt');
        expect(alt).not.toBeNull();
        expect(alt.trim().length).toBeGreaterThan(0);
      });
    });

    it('should have alt text or title for SVG images with role="img"', () => {
      const svgImages = document.querySelectorAll('svg[role="img"]');

      svgImages.forEach((svg) => {
        // SVG images with role="img" should have either:
        // - aria-label attribute
        // - aria-labelledby pointing to a title element
        // - a title child element
        const ariaLabel = svg.getAttribute('aria-label');
        const ariaLabelledBy = svg.getAttribute('aria-labelledby');
        const titleElement = svg.querySelector('title');

        const hasAccessibleName =
          (ariaLabel && ariaLabel.trim().length > 0) ||
          (ariaLabelledBy && ariaLabelledBy.trim().length > 0) ||
          (titleElement && titleElement.textContent.trim().length > 0);

        expect(hasAccessibleName).toBe(true);
      });
    });
  });

  /**
   * Test Case 4: Check for main landmark
   * Expected: Page has main element or role='main' landmark
   */
  describe('Test Case 4: Main landmark', () => {
    it('should have a main element or element with role="main"', () => {
      const mainElement = document.querySelector('main');
      const mainRole = document.querySelector('[role="main"]');

      const hasMainLandmark = mainElement !== null || mainRole !== null;
      expect(hasMainLandmark).toBe(true);
    });

    it('should have only one main landmark', () => {
      const mainElements = document.querySelectorAll('main');
      const mainRoles = document.querySelectorAll('[role="main"]');

      // Count total main landmarks, avoiding duplicates
      let mainCount = mainElements.length;
      mainRoles.forEach((el) => {
        if (el.tagName.toLowerCase() !== 'main') {
          mainCount++;
        }
      });

      expect(mainCount).toBe(1);
    });
  });

  /**
   * Test Case 5: Check for nav landmark
   * Expected: Navigation has nav element or role='navigation'
   */
  describe('Test Case 5: Navigation landmark', () => {
    it('should have a nav element or element with role="navigation"', () => {
      const navElement = document.querySelector('nav');
      const navRole = document.querySelector('[role="navigation"]');

      const hasNavLandmark = navElement !== null || navRole !== null;
      expect(hasNavLandmark).toBe(true);
    });

    it('should have navigation containing links', () => {
      const navElement = document.querySelector('nav') || document.querySelector('[role="navigation"]');
      expect(navElement).not.toBeNull();

      const links = navElement.querySelectorAll('a');
      expect(links.length).toBeGreaterThan(0);
    });
  });

  /**
   * Test Case 6: Verify link text is descriptive
   * Expected: Links have descriptive text, not generic 'click here'
   */
  describe('Test Case 6: Descriptive link text', () => {
    const genericLinkTexts = [
      'click here',
      'click',
      'here',
      'read more',
      'more',
      'link',
      'this link',
    ];

    it('should not have links with generic non-descriptive text', () => {
      const links = document.querySelectorAll('a');
      expect(links.length).toBeGreaterThan(0);

      links.forEach((link) => {
        const linkText = link.textContent.trim().toLowerCase();

        // Check if link text is one of the generic phrases
        const isGeneric = genericLinkTexts.some(
          (generic) => linkText === generic
        );

        expect(isGeneric).toBe(false);
      });
    });

    it('should have links with meaningful accessible names', () => {
      const links = document.querySelectorAll('a');

      links.forEach((link) => {
        // Get the accessible name - could be text content, aria-label, or aria-labelledby
        const textContent = link.textContent.trim();
        const ariaLabel = link.getAttribute('aria-label');
        const title = link.getAttribute('title');

        const accessibleName =
          ariaLabel ||
          textContent ||
          title;

        // Link should have some accessible name
        expect(accessibleName).toBeTruthy();
        expect(accessibleName.length).toBeGreaterThan(0);
      });
    });

    it('should not have empty links', () => {
      const links = document.querySelectorAll('a');

      links.forEach((link) => {
        // Check for accessible name via text, aria-label, aria-labelledby, or title
        const textContent = link.textContent.trim();
        const ariaLabel = link.getAttribute('aria-label');
        const ariaLabelledBy = link.getAttribute('aria-labelledby');
        const title = link.getAttribute('title');

        // At least one method of providing accessible name should exist
        const hasAccessibleName =
          textContent.length > 0 ||
          (ariaLabel && ariaLabel.length > 0) ||
          (ariaLabelledBy && ariaLabelledBy.length > 0) ||
          (title && title.length > 0);

        expect(hasAccessibleName).toBe(true);
      });
    });
  });

  /**
   * Additional Accessibility Tests
   */
  describe('Additional Accessibility Checks', () => {
    it('should have a lang attribute on the html element', () => {
      const html = document.querySelector('html');
      expect(html).not.toBeNull();
      const lang = html.getAttribute('lang');
      expect(lang).not.toBeNull();
      expect(lang.length).toBeGreaterThan(0);
    });

    it('should have a descriptive title element', () => {
      const title = document.querySelector('title');
      expect(title).not.toBeNull();
      expect(title.textContent.trim().length).toBeGreaterThan(0);
    });

    it('should have buttons with accessible names', () => {
      const buttons = document.querySelectorAll('button');

      buttons.forEach((button) => {
        const textContent = button.textContent.trim();
        const ariaLabel = button.getAttribute('aria-label');
        const title = button.getAttribute('title');

        const hasAccessibleName =
          textContent.length > 0 ||
          (ariaLabel && ariaLabel.length > 0) ||
          (title && title.length > 0);

        expect(hasAccessibleName).toBe(true);
      });
    });

    it('should have header landmark', () => {
      const headerElement = document.querySelector('header');
      const headerRole = document.querySelector('[role="banner"]');

      const hasHeaderLandmark = headerElement !== null || headerRole !== null;
      expect(hasHeaderLandmark).toBe(true);
    });

    it('should have footer landmark', () => {
      const footerElement = document.querySelector('footer');
      const footerRole = document.querySelector('[role="contentinfo"]');

      const hasFooterLandmark = footerElement !== null || footerRole !== null;
      expect(hasFooterLandmark).toBe(true);
    });
  });
});
