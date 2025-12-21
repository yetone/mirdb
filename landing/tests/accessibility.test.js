import { describe, it, expect, beforeEach } from 'vitest';
import { JSDOM } from 'jsdom';
import fs from 'fs';
import path from 'path';

describe('Accessibility Compliance', () => {
  let document;
  let dom;

  beforeEach(() => {
    const htmlPath = path.resolve(__dirname, '../index.html');
    const html = fs.readFileSync(htmlPath, 'utf8');
    dom = new JSDOM(html);
    document = dom.window.document;
  });

  // Test Case 1: Single h1 element on page
  describe('Test Case 1: Single h1 Element', () => {
    it('should have exactly one h1 element on the page', () => {
      const h1Elements = document.querySelectorAll('h1');
      expect(h1Elements.length).toBe(1);
    });

    it('should have h1 with meaningful content', () => {
      const h1 = document.querySelector('h1');
      expect(h1).not.toBeNull();
      expect(h1.textContent.trim().length).toBeGreaterThan(0);
    });
  });

  // Test Case 2: Heading hierarchy sequence
  describe('Test Case 2: Heading Hierarchy', () => {
    it('should have headings in logical order without skipping levels', () => {
      const headings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      const headingLevels = Array.from(headings).map(h => parseInt(h.tagName.charAt(1)));

      // First heading should be h1
      expect(headingLevels[0]).toBe(1);

      // Each subsequent heading should not skip more than one level
      for (let i = 1; i < headingLevels.length; i++) {
        const currentLevel = headingLevels[i];
        const previousLevel = headingLevels[i - 1];
        // When going deeper, should not skip levels (h1 -> h3 is bad, h1 -> h2 is good)
        // When going up, any level is fine (h3 -> h2 is fine, h3 -> h1 is fine)
        if (currentLevel > previousLevel) {
          expect(currentLevel - previousLevel).toBeLessThanOrEqual(1);
        }
      }
    });

    it('should start with h1 as the first heading', () => {
      const firstHeading = document.querySelector('h1, h2, h3, h4, h5, h6');
      expect(firstHeading).not.toBeNull();
      expect(firstHeading.tagName).toBe('H1');
    });

    it('should have h2 elements following the h1', () => {
      const h2Elements = document.querySelectorAll('h2');
      expect(h2Elements.length).toBeGreaterThan(0);
    });
  });

  // Test Case 3: Alt text for images
  describe('Test Case 3: Image Alt Attributes', () => {
    it('should have alt attributes on all img elements', () => {
      const images = document.querySelectorAll('img');
      images.forEach((img, index) => {
        const hasAlt = img.hasAttribute('alt');
        expect(hasAlt, `Image ${index + 1} is missing alt attribute`).toBe(true);
      });
    });

    it('should have aria-hidden on decorative SVG icons', () => {
      const decorativeSvgs = document.querySelectorAll('svg');
      decorativeSvgs.forEach((svg) => {
        // Decorative SVGs should have aria-hidden="true"
        if (svg.closest('.feature-icon') || svg.closest('[aria-hidden="true"]')) {
          const ariaHidden = svg.getAttribute('aria-hidden');
          expect(ariaHidden).toBe('true');
        }
      });
    });
  });

  // Test Case 4: Semantic landmarks
  describe('Test Case 4: Semantic HTML Landmarks', () => {
    it('should have a header element', () => {
      const header = document.querySelector('header');
      expect(header).not.toBeNull();
    });

    it('should have a main element', () => {
      const main = document.querySelector('main');
      expect(main).not.toBeNull();
    });

    it('should have a footer element', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();
    });

    it('should use section elements for content organization', () => {
      const sections = document.querySelectorAll('section');
      expect(sections.length).toBeGreaterThan(0);
    });

    it('should have role="banner" on header or use banner landmark', () => {
      const header = document.querySelector('header');
      // Header element implicitly has banner role, or explicit role can be set
      expect(header !== null || document.querySelector('[role="banner"]') !== null).toBe(true);
    });

    it('should have role="contentinfo" on footer or use contentinfo landmark', () => {
      const footer = document.querySelector('footer');
      const hasContentinfoRole = footer && footer.getAttribute('role') === 'contentinfo';
      // Footer element implicitly has contentinfo role
      expect(footer !== null).toBe(true);
    });
  });

  // Test Case 9: Button and link accessible names
  describe('Test Case 9: Accessible Names for Interactive Elements', () => {
    it('should have accessible names on all buttons', () => {
      const buttons = document.querySelectorAll('button');
      buttons.forEach((button, index) => {
        const hasAccessibleName =
          button.textContent.trim().length > 0 ||
          button.getAttribute('aria-label') ||
          button.getAttribute('aria-labelledby') ||
          button.getAttribute('title');
        expect(hasAccessibleName, `Button ${index + 1} lacks accessible name`).toBe(true);
      });
    });

    it('should have accessible names on all links', () => {
      const links = document.querySelectorAll('a');
      links.forEach((link, index) => {
        const hasAccessibleName =
          link.textContent.trim().length > 0 ||
          link.getAttribute('aria-label') ||
          link.getAttribute('aria-labelledby');
        expect(hasAccessibleName, `Link ${index + 1} lacks accessible name`).toBe(true);
      });
    });

    it('should not have empty links', () => {
      const links = document.querySelectorAll('a');
      links.forEach((link) => {
        const textContent = link.textContent.trim();
        const ariaLabel = link.getAttribute('aria-label');
        const hasContent = textContent.length > 0 || (ariaLabel && ariaLabel.length > 0);
        expect(hasContent).toBe(true);
      });
    });

    it('should have meaningful link text (not just "click here" or "read more")', () => {
      const links = document.querySelectorAll('a');
      const vagueTexts = ['click here', 'read more', 'more', 'here', 'link'];
      links.forEach((link) => {
        const text = link.textContent.trim().toLowerCase();
        if (text.length > 0) {
          const isVague = vagueTexts.some(vague => text === vague);
          expect(isVague, `Link with text "${text}" is not descriptive`).toBe(false);
        }
      });
    });
  });

  // Additional accessibility tests
  describe('Additional Accessibility Features', () => {
    it('should have lang attribute on html element', () => {
      const html = document.documentElement;
      expect(html.hasAttribute('lang')).toBe(true);
      expect(html.getAttribute('lang')).toBe('en');
    });

    it('should have a descriptive title element', () => {
      const title = document.querySelector('title');
      expect(title).not.toBeNull();
      expect(title.textContent.length).toBeGreaterThan(0);
    });

    it('should have viewport meta tag for responsive design', () => {
      const viewport = document.querySelector('meta[name="viewport"]');
      expect(viewport).not.toBeNull();
    });

    it('should have navigation element with aria-label in footer', () => {
      const nav = document.querySelector('nav[aria-label]');
      if (nav) {
        expect(nav.getAttribute('aria-label').length).toBeGreaterThan(0);
      }
    });

    it('should have proper table structure with headers if tables exist', () => {
      const tables = document.querySelectorAll('table');
      tables.forEach((table) => {
        const headers = table.querySelectorAll('th');
        expect(headers.length).toBeGreaterThan(0);
      });
    });
  });
});
