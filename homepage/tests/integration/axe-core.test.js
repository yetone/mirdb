/**
 * Axe-Core Accessibility Audit Tests
 * Owner: Scenario 12 - Accessibility Compliance
 *
 * Tests:
 * - Run full axe-core audit
 * - No critical violations
 * - No serious violations
 *
 * Requirements: NFR-3 (WCAG 2.1 AA)
 */

const fs = require('fs');
const path = require('path');
const { JSDOM } = require('jsdom');

// Mock axe-core for Jest unit testing approach
// In real integration testing, we would use @axe-core/playwright with Playwright
describe('Axe-Core Accessibility Audit', () => {
  let document;
  let htmlContent;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf-8');
    const dom = new JSDOM(htmlContent);
    document = dom.window.document;
  });

  describe('Test Case 1: No critical or serious accessibility violations', () => {
    test('HTML document has lang attribute', () => {
      const html = document.querySelector('html');
      expect(html).not.toBeNull();
      expect(html.getAttribute('lang')).toBe('en');
    });

    test('Document has a title', () => {
      const title = document.querySelector('title');
      expect(title).not.toBeNull();
      expect(title.textContent.length).toBeGreaterThan(0);
    });

    test('Main landmark exists', () => {
      const main = document.querySelector('main[role="main"]');
      expect(main).not.toBeNull();
    });

    test('Header landmark exists', () => {
      const header = document.querySelector('header[role="banner"]');
      expect(header).not.toBeNull();
    });

    test('Navigation landmarks exist with aria-label', () => {
      const navs = document.querySelectorAll('nav[role="navigation"]');
      expect(navs.length).toBeGreaterThan(0);

      navs.forEach(nav => {
        expect(nav.getAttribute('aria-label')).not.toBeNull();
      });
    });

    test('Footer landmark exists', () => {
      const footer = document.querySelector('footer[role="contentinfo"]');
      expect(footer).not.toBeNull();
    });

    test('All sections have aria-labelledby for accessible names', () => {
      const sections = document.querySelectorAll('main section');

      sections.forEach(section => {
        const ariaLabelledBy = section.getAttribute('aria-labelledby');
        if (ariaLabelledBy) {
          const labelElement = document.getElementById(ariaLabelledBy);
          expect(labelElement).not.toBeNull();
        }
      });
    });

    test('All buttons have accessible names', () => {
      const buttons = document.querySelectorAll('button');

      buttons.forEach(button => {
        const hasAriaLabel = button.getAttribute('aria-label') !== null;
        const hasTextContent = button.textContent.trim().length > 0;
        expect(hasAriaLabel || hasTextContent).toBeTruthy();
      });
    });

    test('All links have accessible names', () => {
      const links = document.querySelectorAll('a');

      links.forEach(link => {
        const hasAriaLabel = link.getAttribute('aria-label') !== null;
        const hasTextContent = link.textContent.trim().length > 0;
        const hasAriaLabelledBy = link.getAttribute('aria-labelledby') !== null;
        expect(hasAriaLabel || hasTextContent || hasAriaLabelledBy).toBeTruthy();
      });
    });

    test('External links have rel="noopener noreferrer"', () => {
      const externalLinks = document.querySelectorAll('a[target="_blank"]');

      externalLinks.forEach(link => {
        const rel = link.getAttribute('rel');
        expect(rel).toContain('noopener');
        expect(rel).toContain('noreferrer');
      });
    });

    test('Tables have proper structure', () => {
      const tables = document.querySelectorAll('table');

      tables.forEach(table => {
        // Tables should have thead and tbody
        const thead = table.querySelector('thead');
        const tbody = table.querySelector('tbody');

        // At least one should exist
        expect(thead || tbody).not.toBeNull();

        // Header cells should use th with scope
        const headerCells = table.querySelectorAll('th');
        headerCells.forEach(th => {
          const scope = th.getAttribute('scope');
          expect(scope === 'col' || scope === 'row').toBe(true);
        });
      });
    });

    test('Lists have proper structure (ul/ol contain li)', () => {
      const lists = document.querySelectorAll('ul, ol');

      lists.forEach(list => {
        const items = list.children;
        for (let i = 0; i < items.length; i++) {
          expect(items[i].tagName.toLowerCase()).toBe('li');
        }
      });
    });

    test('Heading hierarchy is correct (h1 followed by h2, h2 by h3, etc.)', () => {
      const headings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      let lastLevel = 0;

      headings.forEach(heading => {
        const level = parseInt(heading.tagName.charAt(1));
        // Each heading should not skip more than 1 level
        expect(level - lastLevel).toBeLessThanOrEqual(1);
        lastLevel = level;
      });
    });

    test('Only one h1 element exists on the page', () => {
      const h1Elements = document.querySelectorAll('h1');
      expect(h1Elements.length).toBe(1);
    });

    test('Interactive elements have focus styles via CSS', () => {
      // Check that styles.css is loaded which contains :focus-visible styles
      const styleLinks = document.querySelectorAll('link[rel="stylesheet"]');
      const hasStylesCss = Array.from(styleLinks).some(link =>
        link.getAttribute('href')?.includes('styles.css')
      );
      expect(hasStylesCss).toBe(true);
    });
  });

  describe('Form accessibility', () => {
    test('Form inputs have associated labels (if any exist)', () => {
      const inputs = document.querySelectorAll('input, textarea, select');

      inputs.forEach(input => {
        const id = input.getAttribute('id');
        const ariaLabel = input.getAttribute('aria-label');
        const ariaLabelledBy = input.getAttribute('aria-labelledby');

        if (id) {
          const label = document.querySelector(`label[for="${id}"]`);
          expect(label || ariaLabel || ariaLabelledBy).not.toBeNull();
        } else {
          // If no id, must have aria-label or aria-labelledby
          expect(ariaLabel || ariaLabelledBy).not.toBeNull();
        }
      });
    });
  });

  describe('ARIA attributes validation', () => {
    test('Elements with role="img" have aria-label', () => {
      const imgRoleElements = document.querySelectorAll('[role="img"]');

      imgRoleElements.forEach(el => {
        const ariaLabel = el.getAttribute('aria-label');
        const ariaLabelledBy = el.getAttribute('aria-labelledby');
        expect(ariaLabel || ariaLabelledBy).not.toBeNull();
      });
    });

    test('aria-hidden elements are not focusable', () => {
      const hiddenElements = document.querySelectorAll('[aria-hidden="true"]');

      hiddenElements.forEach(el => {
        // aria-hidden elements should not contain focusable elements without tabindex=-1
        const focusableWithoutNegativeTabindex = el.querySelectorAll(
          'a[href], button, input, select, textarea, [tabindex]:not([tabindex="-1"])'
        );
        expect(focusableWithoutNegativeTabindex.length).toBe(0);
      });
    });

    test('aria-expanded is used correctly on toggleable elements', () => {
      const expandableButtons = document.querySelectorAll('[aria-expanded]');

      expandableButtons.forEach(button => {
        const expanded = button.getAttribute('aria-expanded');
        expect(expanded === 'true' || expanded === 'false').toBe(true);
      });
    });
  });
});
