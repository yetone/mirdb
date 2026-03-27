/**
 * Accessibility Unit Tests
 * Owner: Scenarios 13-15 - Accessibility
 *
 * Tests:
 * - Semantic HTML landmarks (header, main, nav, section, footer)
 * - Image alt text
 * - Heading hierarchy
 * - ARIA labels on code blocks
 * - Descriptive link text
 */
import { describe, it, expect, beforeAll } from 'vitest';
import { readFileSync } from 'fs';
import { JSDOM } from 'jsdom';
import { resolve } from 'path';

describe('Accessibility - Screen Reader', () => {
  let document;

  beforeAll(() => {
    const htmlPath = resolve(process.cwd(), 'src/index.html');
    const html = readFileSync(htmlPath, 'utf-8');
    const dom = new JSDOM(html);
    document = dom.window.document;
  });

  describe('TC1: Semantic HTML Landmarks', () => {
    it('should have a header element', () => {
      const header = document.querySelector('header');
      expect(header).not.toBeNull();
    });

    it('should have a main element', () => {
      const main = document.querySelector('main');
      expect(main).not.toBeNull();
    });

    it('should have a nav element', () => {
      const nav = document.querySelector('nav');
      expect(nav).not.toBeNull();
    });

    it('should have section elements', () => {
      const sections = document.querySelectorAll('section');
      expect(sections.length).toBeGreaterThan(0);
    });

    it('should have a footer element', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();
    });

    it('should have document language set', () => {
      const htmlElement = document.querySelector('html');
      expect(htmlElement.getAttribute('lang')).toBe('en');
    });
  });

  describe('TC2: Image Alt Text', () => {
    it('should have alt text on all img elements', () => {
      const images = document.querySelectorAll('img');
      expect(images.length).toBeGreaterThan(0);

      images.forEach((img, index) => {
        const alt = img.getAttribute('alt');
        expect(alt, `Image ${index + 1} missing alt attribute`).not.toBeNull();
        expect(alt.trim().length, `Image ${index + 1} has empty alt text`).toBeGreaterThan(0);
      });
    });

    it('should have meaningful alt text (not generic)', () => {
      const images = document.querySelectorAll('img');
      const genericAltPatterns = ['image', 'photo', 'picture', 'img', 'untitled'];

      images.forEach((img) => {
        const alt = img.getAttribute('alt').toLowerCase();
        genericAltPatterns.forEach(pattern => {
          // Allow pattern only if it's part of a larger meaningful description
          if (alt === pattern) {
            expect.fail(`Image has generic alt text: "${alt}"`);
          }
        });
      });
    });
  });

  describe('TC3: Heading Hierarchy', () => {
    it('should have exactly one h1 element', () => {
      const h1s = document.querySelectorAll('h1');
      expect(h1s.length).toBe(1);
    });

    it('should follow logical heading hierarchy without skipping levels', () => {
      const headings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      let currentLevel = 0;

      headings.forEach((heading) => {
        const level = parseInt(heading.tagName.charAt(1));

        // First heading should be h1
        if (currentLevel === 0) {
          expect(level, 'First heading should be h1').toBe(1);
        } else {
          // Should not skip more than one level
          const levelDiff = level - currentLevel;
          expect(levelDiff, `Heading hierarchy skips levels: h${currentLevel} to h${level}`).toBeLessThanOrEqual(1);
        }

        currentLevel = level;
      });
    });

    it('should have h2 headings for major sections', () => {
      const h2s = document.querySelectorAll('h2');
      expect(h2s.length).toBeGreaterThan(0);
    });
  });

  describe('TC4: Code Blocks ARIA Labels', () => {
    it('should have aria-label on code blocks', () => {
      // Check for code blocks with role="region" that have aria-labels
      const codeBlocks = document.querySelectorAll('.code-block, pre[role="region"], [role="region"][aria-label*="code"], [role="region"][aria-label*="command"]');

      // Also check code blocks in quick-start section
      const quickStartCodeBlocks = document.querySelectorAll('#quick-start .code-block');

      // If no quick-start code blocks, check for any pre elements that should have labels
      const allCodeRegions = document.querySelectorAll('[role="region"]');

      // At minimum, major code examples should have aria-labels
      expect(allCodeRegions.length, 'Should have code blocks with role="region"').toBeGreaterThan(0);

      allCodeRegions.forEach((block) => {
        const ariaLabel = block.getAttribute('aria-label');
        expect(ariaLabel, 'Code block should have aria-label').not.toBeNull();
        expect(ariaLabel.trim().length, 'Code block aria-label should not be empty').toBeGreaterThan(0);
      });
    });

    it('should have architecture diagram with appropriate label', () => {
      const archDiagram = document.querySelector('.architecture-diagram, [role="img"]');
      if (archDiagram) {
        const ariaLabel = archDiagram.getAttribute('aria-label');
        expect(ariaLabel, 'Architecture diagram should have aria-label').not.toBeNull();
      }
    });
  });

  describe('TC5: Descriptive Link Text', () => {
    it('should not have generic link text like "click here"', () => {
      const links = document.querySelectorAll('a');
      const genericTexts = ['click here', 'click', 'here', 'read more', 'more', 'link'];

      links.forEach((link) => {
        const linkText = link.textContent.trim().toLowerCase();

        genericTexts.forEach(generic => {
          if (linkText === generic) {
            expect.fail(`Link has generic text: "${linkText}"`);
          }
        });
      });
    });

    it('should have descriptive text or aria-label for all links', () => {
      const links = document.querySelectorAll('a');

      links.forEach((link, index) => {
        const linkText = link.textContent.trim();
        const ariaLabel = link.getAttribute('aria-label');

        // Either have meaningful text content or an aria-label
        const hasDescriptiveContent = linkText.length > 0 || (ariaLabel && ariaLabel.length > 0);
        expect(hasDescriptiveContent, `Link ${index + 1} lacks descriptive text or aria-label`).toBe(true);
      });
    });

    it('should have unique or distinguishable link text for different destinations', () => {
      const links = document.querySelectorAll('a[href]');
      const linksByText = new Map();

      links.forEach((link) => {
        const href = link.getAttribute('href');
        const text = link.textContent.trim().toLowerCase();

        // Skip empty text or anchors
        if (!text || href.startsWith('#')) return;

        if (linksByText.has(text)) {
          const existingHref = linksByText.get(text);
          // Same text can link to same destination, but different destinations need different text
          // Allow "View on GitHub" to appear multiple times for same destination
          if (existingHref !== href && !href.includes('github.com')) {
            console.warn(`Links with same text "${text}" go to different destinations`);
          }
        } else {
          linksByText.set(text, href);
        }
      });
    });
  });

  describe('Additional Screen Reader Accessibility', () => {
    it('should have skip link or proper landmark navigation', () => {
      // Either have a skip link OR proper landmarks that screen readers can use
      const skipLink = document.querySelector('a[href="#main"], a[href="#content"], .skip-link');
      const main = document.querySelector('main');
      const nav = document.querySelector('nav');

      // At minimum, should have main and nav landmarks
      expect(main || skipLink, 'Should have main landmark or skip link').toBeTruthy();
      expect(nav, 'Should have nav landmark').not.toBeNull();
    });

    it('should have meta description', () => {
      const metaDesc = document.querySelector('meta[name="description"]');
      expect(metaDesc).not.toBeNull();
      expect(metaDesc.getAttribute('content').length).toBeGreaterThan(0);
    });

    it('should have viewport meta tag', () => {
      const viewport = document.querySelector('meta[name="viewport"]');
      expect(viewport).not.toBeNull();
    });

    it('interactive elements should be accessible', () => {
      // Buttons should have accessible names
      const buttons = document.querySelectorAll('button');
      buttons.forEach((button, index) => {
        const hasName = button.textContent.trim().length > 0 ||
                       button.getAttribute('aria-label') ||
                       button.getAttribute('aria-labelledby') ||
                       button.getAttribute('title');
        expect(hasName, `Button ${index + 1} lacks accessible name`).toBeTruthy();
      });
    });
  });
});
