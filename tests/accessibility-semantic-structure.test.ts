import { describe, it, expect, beforeAll } from 'vitest';
import { JSDOM } from 'jsdom';
import * as fs from 'fs';
import * as path from 'path';

describe('Accessibility - Semantic Structure', () => {
  let document: Document;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '..', 'index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf-8');
    const dom = new JSDOM(htmlContent);
    document = dom.window.document;
  });

  describe('Heading Structure', () => {
    it('should have exactly one h1 element with MirDB', () => {
      const h1Elements = document.querySelectorAll('h1');

      expect(h1Elements.length).toBe(1);
      expect(h1Elements[0].textContent).toBe('MirDB');
    });

    it('should have headings that follow logical hierarchy without skipping levels', () => {
      const allHeadings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      const headingLevels: number[] = [];

      allHeadings.forEach((heading) => {
        const level = parseInt(heading.tagName.charAt(1), 10);
        headingLevels.push(level);
      });

      // Check that we don't skip levels (e.g., h1 -> h3 without h2)
      for (let i = 1; i < headingLevels.length; i++) {
        const currentLevel = headingLevels[i];
        const previousLevel = headingLevels[i - 1];

        // If we're going down (higher number), we shouldn't skip more than 1 level
        if (currentLevel > previousLevel) {
          const skippedLevels = currentLevel - previousLevel;
          expect(skippedLevels).toBeLessThanOrEqual(1);
        }
      }
    });

    it('should start with h1 as the first heading', () => {
      const firstHeading = document.querySelector('h1, h2, h3, h4, h5, h6');

      expect(firstHeading).not.toBeNull();
      expect(firstHeading?.tagName).toBe('H1');
    });

    it('should have h2 elements for main sections', () => {
      const h2Elements = document.querySelectorAll('h2');

      expect(h2Elements.length).toBeGreaterThan(0);

      // Verify expected section headings exist
      const h2Texts = Array.from(h2Elements).map(h => h.textContent?.trim());
      expect(h2Texts).toContain('Key Features');
      expect(h2Texts).toContain('Getting Started');
      expect(h2Texts).toContain('Architecture');
    });

    it('should have h3 elements for subsections', () => {
      const h3Elements = document.querySelectorAll('h3');

      expect(h3Elements.length).toBeGreaterThan(0);
    });
  });

  describe('Landmark Regions', () => {
    it('should have navigation wrapped in nav element', () => {
      const navElement = document.querySelector('nav');

      expect(navElement).not.toBeNull();
      expect(navElement?.classList.contains('navbar')).toBe(true);

      // Nav should contain links
      const navLinks = navElement?.querySelectorAll('a');
      expect(navLinks?.length).toBeGreaterThan(0);
    });

    it('should have main content wrapped in main element', () => {
      const mainElement = document.querySelector('main');

      expect(mainElement).not.toBeNull();

      // Main should contain section elements
      const sections = mainElement?.querySelectorAll('section');
      expect(sections?.length).toBeGreaterThan(0);
    });

    it('should have footer content wrapped in footer element', () => {
      const footerElement = document.querySelector('footer');

      expect(footerElement).not.toBeNull();
      expect(footerElement?.classList.contains('footer')).toBe(true);

      // Footer should contain content
      expect(footerElement?.textContent?.trim().length).toBeGreaterThan(0);
    });

    it('should have header element for hero section', () => {
      const headerElement = document.querySelector('header');

      expect(headerElement).not.toBeNull();
      expect(headerElement?.classList.contains('hero')).toBe(true);
    });

    it('should have section elements for content areas', () => {
      const sectionElements = document.querySelectorAll('section');

      expect(sectionElements.length).toBeGreaterThan(0);

      // Check that sections have IDs for navigation
      const sectionsWithIds = Array.from(sectionElements).filter(s => s.id);
      expect(sectionsWithIds.length).toBeGreaterThan(0);
    });
  });

  describe('ARIA and Accessibility Attributes', () => {
    it('should have role="navigation" on nav element', () => {
      const navElement = document.querySelector('nav');

      expect(navElement?.getAttribute('role')).toBe('navigation');
    });

    it('should have aria-label on navigation', () => {
      const navElement = document.querySelector('nav');

      expect(navElement?.getAttribute('aria-label')).toBeTruthy();
    });

    it('should have role="banner" on header', () => {
      const headerElement = document.querySelector('header.hero');

      expect(headerElement?.getAttribute('role')).toBe('banner');
    });

    it('should have lang attribute on html element', () => {
      const htmlElement = document.querySelector('html');

      expect(htmlElement?.getAttribute('lang')).toBe('en');
    });
  });

  describe('Document Structure', () => {
    it('should have a proper document structure with html, head, and body', () => {
      const htmlElement = document.querySelector('html');
      const headElement = document.querySelector('head');
      const bodyElement = document.querySelector('body');

      expect(htmlElement).not.toBeNull();
      expect(headElement).not.toBeNull();
      expect(bodyElement).not.toBeNull();
    });

    it('should have a title element', () => {
      const titleElement = document.querySelector('title');

      expect(titleElement).not.toBeNull();
      expect(titleElement?.textContent).toContain('MirDB');
    });

    it('should have viewport meta tag for responsive design', () => {
      const viewportMeta = document.querySelector('meta[name="viewport"]');

      expect(viewportMeta).not.toBeNull();
      expect(viewportMeta?.getAttribute('content')).toContain('width=device-width');
    });
  });
});
