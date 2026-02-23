/**
 * Accessibility Unit Tests
 * Owner: Scenario 11 - Color Contrast, Scenario 12 - Images and Alt Text
 *
 * Tests for accessibility requirements:
 * - Alt text presence on all images
 * - Proper alt text content (not empty where meaningful)
 * - Logo and badge accessibility
 */

const fs = require('fs');
const path = require('path');

describe('Accessibility - Images and Alt Text', () => {
  let document;
  let htmlContent;

  beforeAll(() => {
    // Read the index.html file
    const htmlPath = path.join(__dirname, '../../index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf8');

    // Parse HTML using jsdom
    const { JSDOM } = require('jsdom');
    const dom = new JSDOM(htmlContent);
    document = dom.window.document;
  });

  describe('Test Case 1: MirDB Logo Alt Text', () => {
    test('Logo image has descriptive alt text containing "MirDB" and "Logo"', () => {
      const logo = document.querySelector('.hero-logo');
      expect(logo).not.toBeNull();
      expect(logo.tagName.toLowerCase()).toBe('img');

      const altText = logo.getAttribute('alt');
      expect(altText).not.toBeNull();
      expect(altText.length).toBeGreaterThan(0);
      expect(altText.toLowerCase()).toContain('mirdb');
      expect(altText.toLowerCase()).toContain('logo');
    });

    test('Logo image alt text is descriptive (not just "image" or "logo")', () => {
      const logo = document.querySelector('.hero-logo');
      const altText = logo.getAttribute('alt');

      // Alt text should be meaningful, not generic
      expect(altText.toLowerCase()).not.toBe('image');
      expect(altText.toLowerCase()).not.toBe('logo');
      expect(altText.toLowerCase()).not.toBe('img');
    });

    test('Logo image src attribute is valid', () => {
      const logo = document.querySelector('.hero-logo');
      const src = logo.getAttribute('src');

      expect(src).not.toBeNull();
      expect(src.length).toBeGreaterThan(0);
      expect(src).toContain('logo');
    });
  });

  describe('Test Case 2: CircleCI Badge Alt Text', () => {
    test('CircleCI badge has alt text describing build status purpose', () => {
      const badge = document.querySelector('[data-testid="circleci-img"]');
      expect(badge).not.toBeNull();
      expect(badge.tagName.toLowerCase()).toBe('img');

      const altText = badge.getAttribute('alt');
      expect(altText).not.toBeNull();
      expect(altText.length).toBeGreaterThan(0);
    });

    test('Badge alt text describes its purpose (build status)', () => {
      const badge = document.querySelector('[data-testid="circleci-img"]');
      const altText = badge.getAttribute('alt').toLowerCase();

      // Alt text should indicate it's related to build/CI status
      const hasRelevantKeyword =
        altText.includes('build') ||
        altText.includes('status') ||
        altText.includes('ci') ||
        altText.includes('circle');

      expect(hasRelevantKeyword).toBe(true);
    });

    test('Badge link has accessible aria-label', () => {
      const badgeLink = document.querySelector('[data-testid="circleci-link"]');
      expect(badgeLink).not.toBeNull();

      const ariaLabel = badgeLink.getAttribute('aria-label');
      expect(ariaLabel).not.toBeNull();
      expect(ariaLabel.length).toBeGreaterThan(0);
    });
  });

  describe('Test Case 3: All Images Have Alt Attributes', () => {
    test('Every <img> element has an alt attribute', () => {
      const allImages = document.querySelectorAll('img');

      expect(allImages.length).toBeGreaterThan(0);

      allImages.forEach((img, index) => {
        const hasAlt = img.hasAttribute('alt');
        const src = img.getAttribute('src') || 'unknown';
        expect(hasAlt).toBe(true);
      });
    });

    test('No images have missing or undefined alt attributes', () => {
      const allImages = document.querySelectorAll('img');

      allImages.forEach((img) => {
        const altValue = img.getAttribute('alt');
        // Alt can be empty string (for decorative) but not null/undefined
        expect(altValue).not.toBeNull();
        expect(typeof altValue).toBe('string');
      });
    });

    test('Meaningful images have non-empty alt text', () => {
      // Images that convey information should have descriptive alt text
      const meaningfulImages = [
        '.hero-logo',                    // Logo conveys brand identity
        '[data-testid="circleci-img"]'   // Badge conveys build status
      ];

      meaningfulImages.forEach((selector) => {
        const img = document.querySelector(selector);
        if (img) {
          const altText = img.getAttribute('alt');
          expect(altText).not.toBeNull();
          expect(altText.trim().length).toBeGreaterThan(0);
        }
      });
    });
  });

  describe('Additional Image Accessibility Checks', () => {
    test('Images with links have appropriate context', () => {
      // Find all images that are inside links
      const linkedImages = document.querySelectorAll('a img');

      linkedImages.forEach((img) => {
        const parentLink = img.closest('a');
        const imgAlt = img.getAttribute('alt');
        const linkAriaLabel = parentLink.getAttribute('aria-label');
        const linkText = parentLink.textContent.trim();

        // Either the image should have alt text, or the link should have aria-label or text
        const hasAccessibleContext =
          (imgAlt && imgAlt.length > 0) ||
          (linkAriaLabel && linkAriaLabel.length > 0) ||
          (linkText && linkText.length > 0);

        expect(hasAccessibleContext).toBe(true);
      });
    });

    test('Alt text does not include redundant phrases', () => {
      const allImages = document.querySelectorAll('img');

      allImages.forEach((img) => {
        const altText = img.getAttribute('alt');
        if (altText) {
          const lowerAlt = altText.toLowerCase();
          // Should not start with "image of", "picture of", "graphic of"
          expect(lowerAlt.startsWith('image of ')).toBe(false);
          expect(lowerAlt.startsWith('picture of ')).toBe(false);
          expect(lowerAlt.startsWith('graphic of ')).toBe(false);
        }
      });
    });
  });
});
