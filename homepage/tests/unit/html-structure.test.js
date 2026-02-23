/**
 * HTML Structure Unit Tests
 * Owner: Scenario 10 - Accessibility - Semantic HTML
 *
 * Tests for semantic HTML validation:
 * - Presence of header, main, footer elements
 * - Section elements with proper headings
 * - Single h1, logical heading hierarchy
 * - Code elements using pre and code tags
 */

const fs = require('fs');
const path = require('path');

describe('Semantic HTML Structure', () => {
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

  describe('Test Case 1: Semantic Header Element', () => {
    test('Page uses <header> element for header content', () => {
      const header = document.querySelector('header');
      expect(header).not.toBeNull();
      expect(header.tagName.toLowerCase()).toBe('header');

      // Header should contain navigation
      const nav = header.querySelector('nav');
      expect(nav).not.toBeNull();
    });
  });

  describe('Test Case 2: Semantic Main Element', () => {
    test('Page uses <main> element for primary content', () => {
      const main = document.querySelector('main');
      expect(main).not.toBeNull();
      expect(main.tagName.toLowerCase()).toBe('main');

      // Main should contain the primary sections
      const sections = main.querySelectorAll('section');
      expect(sections.length).toBeGreaterThan(0);
    });
  });

  describe('Test Case 3: Semantic Footer Element', () => {
    test('Page uses <footer> element for footer content', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();
      expect(footer.tagName.toLowerCase()).toBe('footer');
    });
  });

  describe('Test Case 4: Section Elements', () => {
    test('Page sections are wrapped in <section> elements with appropriate headings', () => {
      const main = document.querySelector('main');
      const sections = main.querySelectorAll('section');

      // Should have multiple sections
      expect(sections.length).toBeGreaterThanOrEqual(4);

      // Each section should have a heading (h2 or h3) or be labelled
      sections.forEach((section) => {
        const hasHeading = section.querySelector('h2, h3') !== null;
        const hasAriaLabel = section.hasAttribute('aria-labelledby') || section.hasAttribute('aria-label');
        expect(hasHeading || hasAriaLabel).toBe(true);
      });
    });

    test('Sections have proper id attributes for navigation', () => {
      const expectedSections = ['hero', 'quickstart', 'features', 'roadmap', 'configuration'];

      expectedSections.forEach((sectionId) => {
        const section = document.getElementById(sectionId);
        expect(section).not.toBeNull();
        expect(section.tagName.toLowerCase()).toBe('section');
      });
    });
  });

  describe('Test Case 5: Single H1 Element', () => {
    test('Page has exactly one <h1> element with MirDB name', () => {
      const h1Elements = document.querySelectorAll('h1');

      // Should have exactly one h1
      expect(h1Elements.length).toBe(1);

      // H1 should contain MirDB
      const h1Text = h1Elements[0].textContent;
      expect(h1Text.toLowerCase()).toContain('mirdb');
    });
  });

  describe('Test Case 6: Heading Hierarchy', () => {
    test('Headings follow logical order (h1 -> h2 -> h3, no skips)', () => {
      const headings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      const headingLevels = Array.from(headings).map((h) => parseInt(h.tagName.charAt(1)));

      // Should start with h1
      expect(headingLevels[0]).toBe(1);

      // Check for no skips (e.g., h1 directly to h3)
      for (let i = 1; i < headingLevels.length; i++) {
        const currentLevel = headingLevels[i];
        const previousLevel = headingLevels[i - 1];

        // Can go down (h1 -> h2), stay same (h2 -> h2), or go up (h3 -> h2)
        // But cannot skip levels going down (h1 -> h3 is invalid)
        if (currentLevel > previousLevel) {
          expect(currentLevel - previousLevel).toBeLessThanOrEqual(1);
        }
      }
    });

    test('Each main section has an h2 heading', () => {
      const mainSections = ['quickstart', 'features', 'roadmap', 'configuration'];

      mainSections.forEach((sectionId) => {
        const section = document.getElementById(sectionId);
        const h2 = section.querySelector('h2');
        expect(h2).not.toBeNull();
      });
    });
  });

  describe('Test Case 7: Code Elements', () => {
    test('Code blocks use <pre> and <code> elements', () => {
      const preElements = document.querySelectorAll('pre');

      // Should have code blocks
      expect(preElements.length).toBeGreaterThan(0);

      // Each pre should contain a code element
      preElements.forEach((pre) => {
        const code = pre.querySelector('code');
        expect(code).not.toBeNull();
      });
    });

    test('Code elements have language classes for syntax highlighting', () => {
      const codeElements = document.querySelectorAll('pre code');

      codeElements.forEach((code) => {
        // Code elements should have a language class (e.g., language-bash)
        const hasLanguageClass = Array.from(code.classList).some((cls) => cls.startsWith('language-'));
        expect(hasLanguageClass).toBe(true);
      });
    });
  });

  describe('Additional Semantic HTML Checks', () => {
    test('Navigation uses <nav> element with aria-label', () => {
      const nav = document.querySelector('nav');
      expect(nav).not.toBeNull();
      expect(nav.hasAttribute('aria-label')).toBe(true);
    });

    test('Lists use proper <ul>/<ol> and <li> elements', () => {
      const lists = document.querySelectorAll('ul, ol');

      lists.forEach((list) => {
        const listItems = list.querySelectorAll(':scope > li');
        expect(listItems.length).toBeGreaterThan(0);
      });
    });

    test('Document has proper lang attribute', () => {
      const html = document.querySelector('html');
      expect(html.hasAttribute('lang')).toBe(true);
      expect(html.getAttribute('lang')).toBe('en');
    });
  });
});
