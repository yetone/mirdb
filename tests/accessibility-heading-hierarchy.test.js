/**
 * Tests for Accessibility - Heading Hierarchy (NFR-3)
 * Verify proper heading hierarchy for screen readers
 * - Single h1 element on the page
 * - No skipped heading levels (h1 -> h2 -> h3)
 * - h1 contains the product name 'MirDB'
 */

const fs = require('fs');
const path = require('path');

describe('Accessibility - Heading Hierarchy', () => {
  let document;

  beforeEach(() => {
    const html = fs.readFileSync(path.resolve(__dirname, '../index.html'), 'utf8');
    document = new DOMParser().parseFromString(html, 'text/html');
  });

  // Test Case 1: Exactly one h1 element exists on the page
  describe('Test Case 1: Single h1 Element', () => {
    test('should have exactly one h1 element on the page', () => {
      const h1Elements = document.querySelectorAll('h1');
      expect(h1Elements.length).toBe(1);
    });

    test('h1 should be present and visible', () => {
      const h1 = document.querySelector('h1');
      expect(h1).not.toBeNull();
      expect(h1.textContent.trim().length).toBeGreaterThan(0);
    });
  });

  // Test Case 2: No heading levels are skipped
  describe('Test Case 2: Proper Heading Hierarchy', () => {
    test('should not have h2 without preceding h1', () => {
      const h1 = document.querySelector('h1');
      const h2s = document.querySelectorAll('h2');

      // h1 must exist if there are h2s
      if (h2s.length > 0) {
        expect(h1).not.toBeNull();
      }
    });

    test('should not skip heading levels (no h3 without h2)', () => {
      const headings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      const headingLevels = Array.from(headings).map(h => parseInt(h.tagName.charAt(1)));

      // Check that no level is skipped
      for (let i = 1; i < headingLevels.length; i++) {
        const currentLevel = headingLevels[i];
        const previousLevel = headingLevels[i - 1];

        // A heading can be same level, one level deeper, or any number of levels higher
        // But cannot skip levels going deeper (e.g., h1 -> h3 without h2)
        if (currentLevel > previousLevel) {
          expect(currentLevel - previousLevel).toBeLessThanOrEqual(1);
        }
      }
    });

    test('heading structure should start with h1', () => {
      const firstHeading = document.querySelector('h1, h2, h3, h4, h5, h6');
      expect(firstHeading).not.toBeNull();
      expect(firstHeading.tagName.toLowerCase()).toBe('h1');
    });

    test('h3 elements should only appear after h2 elements in the document flow', () => {
      const allHeadings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      let hasSeenH2 = false;

      for (const heading of allHeadings) {
        const level = parseInt(heading.tagName.charAt(1));
        if (level === 2) {
          hasSeenH2 = true;
        }
        if (level === 3) {
          expect(hasSeenH2).toBe(true);
        }
      }
    });

    test('should have proper section hierarchy with h2 followed by h3', () => {
      const h2Elements = document.querySelectorAll('h2');
      const h3Elements = document.querySelectorAll('h3');

      // If there are h3 elements, there should be h2 elements
      if (h3Elements.length > 0) {
        expect(h2Elements.length).toBeGreaterThan(0);
      }
    });
  });

  // Test Case 3: h1 contains the product name 'MirDB'
  describe('Test Case 3: h1 Contains Product Name', () => {
    test('h1 should contain the product name "MirDB"', () => {
      const h1 = document.querySelector('h1');
      expect(h1).not.toBeNull();
      expect(h1.textContent).toContain('MirDB');
    });

    test('h1 should primarily display the product name', () => {
      const h1 = document.querySelector('h1');
      expect(h1).not.toBeNull();
      // The h1 text should be primarily the product name (not too long)
      const h1Text = h1.textContent.trim();
      expect(h1Text.length).toBeLessThan(50);
      expect(h1Text.toLowerCase()).toContain('mirdb');
    });
  });

  // Additional accessibility tests for heading structure
  describe('Additional Heading Accessibility Checks', () => {
    test('all headings should have text content', () => {
      const headings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      headings.forEach(heading => {
        expect(heading.textContent.trim().length).toBeGreaterThan(0);
      });
    });

    test('page should have multiple h2 elements for section organization', () => {
      const h2Elements = document.querySelectorAll('h2');
      // A well-structured page should have at least a few sections
      expect(h2Elements.length).toBeGreaterThanOrEqual(2);
    });

    test('headings should not be empty or contain only whitespace', () => {
      const headings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      headings.forEach(heading => {
        const text = heading.textContent.replace(/\s+/g, ' ').trim();
        expect(text.length).toBeGreaterThan(0);
      });
    });
  });
});
