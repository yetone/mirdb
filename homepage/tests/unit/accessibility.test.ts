/**
 * Accessibility Unit Tests
 * Owner: Scenario 10 - Accessibility Compliance
 *
 * Test cases:
 * - Semantic HTML structure
 * - Image alt text
 * - Heading hierarchy
 * - ARIA label presence
 */

import { describe, it, expect, beforeAll } from 'vitest';
import { JSDOM } from 'jsdom';
import * as fs from 'fs';
import * as path from 'path';

let document: Document;

beforeAll(() => {
  const htmlPath = path.resolve(__dirname, '../..', 'index.html');
  const htmlContent = fs.readFileSync(htmlPath, 'utf-8');
  const dom = new JSDOM(htmlContent);
  document = dom.window.document;
});

describe('Accessibility Unit Tests', () => {
  describe('Test Case 2: Semantic HTML Structure', () => {
    it('should have a header element', () => {
      const header = document.querySelector('header');
      expect(header).not.toBeNull();
      expect(header?.getAttribute('role')).toBe('banner');
    });

    it('should have a nav element', () => {
      const nav = document.querySelector('nav');
      expect(nav).not.toBeNull();
      expect(nav?.getAttribute('role')).toBe('navigation');
    });

    it('should have a main element', () => {
      const main = document.querySelector('main');
      expect(main).not.toBeNull();
      expect(main?.getAttribute('role')).toBe('main');
    });

    it('should have section elements with proper labeling', () => {
      const sections = document.querySelectorAll('section');
      expect(sections.length).toBeGreaterThan(0);

      sections.forEach((section) => {
        const ariaLabelledBy = section.getAttribute('aria-labelledby');
        const ariaLabel = section.getAttribute('aria-label');
        // Each section should have either aria-labelledby or aria-label
        const hasLabel = ariaLabelledBy || ariaLabel;
        expect(hasLabel).toBeTruthy();
      });
    });

    it('should have a footer element', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();
      expect(footer?.getAttribute('role')).toBe('contentinfo');
    });
  });

  describe('Test Case 5: Image Alt Text', () => {
    it('should have alt text on all img elements', () => {
      const images = document.querySelectorAll('img');
      images.forEach((img) => {
        const alt = img.getAttribute('alt');
        // Either has descriptive alt text or empty alt for decorative images
        expect(alt !== null).toBe(true);
      });
    });

    it('should mark decorative SVGs with aria-hidden', () => {
      const decorativeSvgs = document.querySelectorAll('svg.feature-icon, .hero-cta svg');
      decorativeSvgs.forEach((svg) => {
        const ariaHidden = svg.getAttribute('aria-hidden');
        expect(ariaHidden).toBe('true');
      });
    });

    it('should ensure SVG icons are either decorative or have accessible labels', () => {
      const allSvgs = document.querySelectorAll('svg');
      allSvgs.forEach((svg) => {
        const ariaHidden = svg.getAttribute('aria-hidden');
        const ariaLabel = svg.getAttribute('aria-label');
        const role = svg.getAttribute('role');
        const title = svg.querySelector('title');

        // SVG should either be hidden from AT or have accessible name
        const isAccessible = ariaHidden === 'true' || ariaLabel || role === 'img' && title;
        expect(isAccessible).toBe(true);
      });
    });
  });

  describe('Test Case 6: ARIA Labels', () => {
    it('should have aria-label on navigation toggle button', () => {
      const toggleBtn = document.querySelector('.nav-toggle');
      expect(toggleBtn).not.toBeNull();
      expect(toggleBtn?.getAttribute('aria-label')).toBeTruthy();
      expect(toggleBtn?.getAttribute('aria-expanded')).toBe('false');
      expect(toggleBtn?.getAttribute('aria-controls')).toBeTruthy();
    });

    it('should have aria-label on logo link', () => {
      const logoLink = document.querySelector('.nav-logo');
      expect(logoLink).not.toBeNull();
      expect(logoLink?.getAttribute('aria-label')).toBeTruthy();
    });

    it('should have proper aria attributes on navigation menu', () => {
      const navMenu = document.querySelector('.nav-links');
      expect(navMenu).not.toBeNull();
      expect(navMenu?.getAttribute('role')).toBe('menubar');
    });

    it('should have proper role attributes on navigation links', () => {
      const navLinks = document.querySelectorAll('.nav-link');
      expect(navLinks.length).toBeGreaterThan(0);

      navLinks.forEach((link) => {
        expect(link.getAttribute('role')).toBe('menuitem');
      });
    });

    it('should have proper role attributes on CTA buttons', () => {
      const ctaButtons = document.querySelectorAll('.hero-cta .btn');
      expect(ctaButtons.length).toBeGreaterThan(0);

      ctaButtons.forEach((btn) => {
        expect(btn.getAttribute('role')).toBe('button');
      });
    });
  });

  describe('Test Case 7: Heading Hierarchy', () => {
    it('should have exactly one h1 element', () => {
      const h1Elements = document.querySelectorAll('h1');
      expect(h1Elements.length).toBe(1);
    });

    it('should have headings in logical order without skipping levels', () => {
      const headings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      const headingLevels: number[] = [];

      headings.forEach((heading) => {
        const level = parseInt(heading.tagName.substring(1), 10);
        headingLevels.push(level);
      });

      // Check that no levels are skipped (e.g., h1 -> h3 without h2)
      for (let i = 1; i < headingLevels.length; i++) {
        const currentLevel = headingLevels[i];
        const previousLevel = headingLevels[i - 1];

        // If current level is deeper, it should only be 1 level deeper at most
        if (currentLevel > previousLevel) {
          expect(currentLevel - previousLevel).toBeLessThanOrEqual(1);
        }
      }
    });

    it('should have h1 before any h2 elements', () => {
      const allHeadings = document.querySelectorAll('h1, h2');
      const headingList = Array.from(allHeadings);

      if (headingList.length > 0) {
        expect(headingList[0].tagName).toBe('H1');
      }
    });

    it('should have descriptive heading content', () => {
      const headings = document.querySelectorAll('h1, h2, h3');

      headings.forEach((heading) => {
        const textContent = heading.textContent?.trim();
        expect(textContent).toBeTruthy();
        expect(textContent?.length).toBeGreaterThan(0);
      });
    });

    it('should have h2 elements after h1 for section headings', () => {
      const h2Elements = document.querySelectorAll('h2');
      expect(h2Elements.length).toBeGreaterThan(0);

      // Each section should have an h2 heading
      const sections = ['features', 'usage', 'quickstart'];
      sections.forEach((sectionId) => {
        const section = document.querySelector(`#${sectionId}`);
        const sectionHeading = section?.querySelector('h2');
        expect(sectionHeading).not.toBeNull();
      });
    });
  });
});
