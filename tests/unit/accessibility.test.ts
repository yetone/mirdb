/**
 * Accessibility Unit Tests
 * Owner: Scenario 9 - Accessibility Compliance
 *
 * Unit tests for:
 * - ARIA labels on icon-only buttons
 * - Heading hierarchy validation
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { JSDOM } from 'jsdom';
import fs from 'fs';
import path from 'path';

describe('Accessibility Compliance', () => {
  let document: Document;

  beforeEach(() => {
    // Load the actual HTML file
    const htmlPath = path.resolve(__dirname, '../../src/index.html');
    const html = fs.readFileSync(htmlPath, 'utf-8');
    const dom = new JSDOM(html);
    document = dom.window.document;
  });

  // Test Case 5: Inspect icon buttons for ARIA labels
  describe('Icon Button ARIA Labels', () => {
    it('should have aria-label on all icon-only buttons', () => {
      // Find all buttons that contain only icons (no text content)
      const buttons = document.querySelectorAll('button');

      buttons.forEach((button) => {
        const textContent = button.textContent?.replace(/\s+/g, '').trim();
        const hasAriaLabel = button.hasAttribute('aria-label');

        // If button has minimal text (likely icon-only), it should have aria-label
        // The copy button has "Copy" text so it's acceptable
        if (!textContent || textContent.length < 10) {
          // Check if button has accessible name via aria-label or visible text
          const hasAccessibleName = hasAriaLabel || (textContent && textContent.length > 0);
          expect(hasAccessibleName).toBe(true);
        }
      });
    });

    it('should have aria-label on theme toggle button', () => {
      const themeToggle = document.querySelector('#theme-toggle');
      expect(themeToggle).not.toBeNull();
      expect(themeToggle?.hasAttribute('aria-label')).toBe(true);
      expect(themeToggle?.getAttribute('aria-label')).toBe('Toggle dark mode');
    });

    it('should have aria-label on copy code button', () => {
      const copyButton = document.querySelector('.code-block__copy');
      expect(copyButton).not.toBeNull();
      expect(copyButton?.hasAttribute('aria-label')).toBe(true);
      expect(copyButton?.getAttribute('aria-label')).toBe('Copy code to clipboard');
    });

    it('should have aria-hidden on decorative icons', () => {
      // Feature card icons should be aria-hidden
      const featureIcons = document.querySelectorAll('.feature-card__icon');
      featureIcons.forEach((icon) => {
        expect(icon.getAttribute('aria-hidden')).toBe('true');
      });

      // Theme toggle icons should be aria-hidden
      const themeIcons = document.querySelectorAll('.theme-toggle__icon');
      themeIcons.forEach((icon) => {
        expect(icon.getAttribute('aria-hidden')).toBe('true');
      });
    });
  });

  // Test Case 6: Check heading hierarchy
  describe('Heading Hierarchy', () => {
    it('should have exactly one h1 element', () => {
      const h1Elements = document.querySelectorAll('h1');
      expect(h1Elements.length).toBe(1);
    });

    it('should have h1 as the first heading', () => {
      const allHeadings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      expect(allHeadings.length).toBeGreaterThan(0);
      expect(allHeadings[0].tagName).toBe('H1');
    });

    it('should have h2s following h1', () => {
      const h1 = document.querySelector('h1');
      const h2Elements = document.querySelectorAll('h2');

      expect(h1).not.toBeNull();
      expect(h2Elements.length).toBeGreaterThan(0);

      // Get all headings in document order
      const allHeadings = Array.from(
        document.querySelectorAll('h1, h2, h3, h4, h5, h6')
      );

      // Find h1 index
      const h1Index = allHeadings.indexOf(h1 as HTMLElement);
      expect(h1Index).toBeGreaterThanOrEqual(0);

      // All h2s should appear after h1 in document order
      h2Elements.forEach((h2) => {
        const h2Index = allHeadings.indexOf(h2 as HTMLElement);
        expect(h2Index).toBeGreaterThan(h1Index);
      });
    });

    it('should have h3s following h2s (no skipping)', () => {
      const h3Elements = document.querySelectorAll('h3');

      if (h3Elements.length > 0) {
        // Check that there's at least one h2 before any h3
        const h2Elements = document.querySelectorAll('h2');
        expect(h2Elements.length).toBeGreaterThan(0);

        // Verify heading levels don't skip
        const allHeadings = Array.from(
          document.querySelectorAll('h1, h2, h3, h4, h5, h6')
        );

        for (let i = 1; i < allHeadings.length; i++) {
          const prevLevel = parseInt(allHeadings[i - 1].tagName.charAt(1));
          const currentLevel = parseInt(allHeadings[i].tagName.charAt(1));

          // Current heading should not skip more than one level from previous
          expect(currentLevel).toBeLessThanOrEqual(prevLevel + 1);
        }
      }
    });

    it('should have proper heading content', () => {
      const h1 = document.querySelector('h1');
      expect(h1?.textContent?.trim()).toBe('MirDB');

      const h2Elements = document.querySelectorAll('h2');
      const h2Texts = Array.from(h2Elements).map((h2) => h2.textContent?.trim());

      expect(h2Texts).toContain('Key Features');
      expect(h2Texts).toContain('Quick Start');
      expect(h2Texts).toContain('Default Configuration');
    });
  });

  // Additional accessibility unit tests
  describe('Skip Navigation', () => {
    it('should have skip navigation link', () => {
      const skipNav = document.querySelector('.skip-nav');
      expect(skipNav).not.toBeNull();
    });

    it('should link to main content', () => {
      const skipNav = document.querySelector('.skip-nav');
      expect(skipNav?.getAttribute('href')).toBe('#main-content');
    });

    it('should have main content element with correct id', () => {
      const mainContent = document.querySelector('#main-content');
      expect(mainContent).not.toBeNull();
      expect(mainContent?.tagName).toBe('MAIN');
    });
  });

  describe('ARIA Landmarks', () => {
    it('should have navigation with aria-label', () => {
      const nav = document.querySelector('nav');
      expect(nav).not.toBeNull();
      expect(nav?.hasAttribute('aria-label')).toBe(true);
    });

    it('should have table headers with scope attribute', () => {
      const tableHeaders = document.querySelectorAll('th');
      tableHeaders.forEach((th) => {
        expect(th.hasAttribute('scope')).toBe(true);
      });
    });
  });

  describe('Language', () => {
    it('should have lang attribute on html element', () => {
      const html = document.querySelector('html');
      expect(html?.hasAttribute('lang')).toBe(true);
      expect(html?.getAttribute('lang')).toBe('en');
    });
  });
});

