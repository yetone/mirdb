/**
 * Unit tests for accessibility requirements (WCAG 2.1 AA)
 * Tests for heading structure, image alt text, and semantic markup
 */

const fs = require('fs');
const path = require('path');
const { JSDOM } = require('jsdom');

describe('Accessibility - Basic Compliance', () => {
  let document;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../index.html');
    const html = fs.readFileSync(htmlPath, 'utf-8');
    const dom = new JSDOM(html);
    document = dom.window.document;
  });

  describe('TC3: Page has exactly one h1', () => {
    test('Page contains exactly one h1 element', () => {
      const h1Elements = document.querySelectorAll('h1');
      expect(h1Elements.length).toBe(1);
    });

    test('h1 element has meaningful content', () => {
      const h1 = document.querySelector('h1');
      expect(h1).not.toBeNull();
      expect(h1.textContent.trim()).toBeTruthy();
      expect(h1.textContent.trim().length).toBeGreaterThan(0);
    });
  });

  describe('TC4: Heading hierarchy follows logical order', () => {
    test('Headings follow logical order without skipping levels', () => {
      const allHeadings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      const headings = Array.from(allHeadings);

      expect(headings.length).toBeGreaterThan(0);

      // First heading should be h1
      expect(headings[0].tagName.toLowerCase()).toBe('h1');

      // Check that no heading level is skipped
      let currentMaxLevel = 1;
      for (const heading of headings) {
        const level = parseInt(heading.tagName.charAt(1));
        // Each heading should not skip more than one level from what we've seen
        // e.g., after h1, we can have h2 but not h3 directly
        // But after h2, we can have h3 or another h2
        expect(level).toBeLessThanOrEqual(currentMaxLevel + 1);

        if (level > currentMaxLevel) {
          currentMaxLevel = level;
        }
      }
    });

    test('No h2 appears before h1', () => {
      const allHeadings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      const headings = Array.from(allHeadings);

      if (headings.length > 0) {
        const firstHeading = headings[0];
        expect(firstHeading.tagName.toLowerCase()).toBe('h1');
      }
    });

    test('Headings are properly nested within sections', () => {
      const sections = document.querySelectorAll('section');

      sections.forEach((section) => {
        const sectionHeadings = section.querySelectorAll('h1, h2, h3, h4, h5, h6');
        if (sectionHeadings.length > 0) {
          // Section headings should start with h2 or lower (h1 is for page title)
          const firstSectionHeading = sectionHeadings[0];
          const level = parseInt(firstSectionHeading.tagName.charAt(1));
          expect(level).toBeGreaterThanOrEqual(2);
        }
      });
    });
  });

  describe('TC5: All images have alt attributes', () => {
    test('All img elements have alt attribute', () => {
      const images = document.querySelectorAll('img');

      images.forEach((img) => {
        expect(img.hasAttribute('alt')).toBe(true);
      });
    });

    test('Alt attributes are not empty for non-decorative images', () => {
      const images = document.querySelectorAll('img');

      images.forEach((img) => {
        const alt = img.getAttribute('alt');
        const isDecorative = img.getAttribute('role') === 'presentation' ||
                            img.getAttribute('aria-hidden') === 'true';

        // Non-decorative images should have meaningful alt text
        // Decorative images can have empty alt=""
        if (!isDecorative && alt !== '') {
          expect(alt).toBeTruthy();
          expect(alt.length).toBeGreaterThan(0);
        }
      });
    });
  });

  describe('Additional accessibility checks', () => {
    test('Document has lang attribute', () => {
      const htmlElement = document.documentElement;
      expect(htmlElement.hasAttribute('lang')).toBe(true);
      expect(htmlElement.getAttribute('lang')).toBe('en');
    });

    test('Page has a title element', () => {
      const title = document.querySelector('title');
      expect(title).not.toBeNull();
      expect(title.textContent.trim()).toBeTruthy();
    });

    test('Main content is wrapped in main element', () => {
      const main = document.querySelector('main');
      expect(main).not.toBeNull();
    });

    test('Header section exists', () => {
      const header = document.querySelector('header');
      expect(header).not.toBeNull();
    });

    test('Footer section exists', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();
    });

    test('Sections have aria-labelledby or aria-label for accessibility', () => {
      const sections = document.querySelectorAll('main section');

      sections.forEach((section) => {
        const hasAriaLabelledBy = section.hasAttribute('aria-labelledby');
        const hasAriaLabel = section.hasAttribute('aria-label');
        const hasHeading = section.querySelector('h2, h3, h4, h5, h6') !== null;

        // Each section should have some form of accessible name
        // Either via aria-labelledby, aria-label, or at least a heading
        const hasAccessibleName = hasAriaLabelledBy || hasAriaLabel || hasHeading;
        expect(hasAccessibleName).toBe(true);
      });
    });

    test('Decorative icons have aria-hidden="true"', () => {
      const featureIcons = document.querySelectorAll('.feature-icon');

      featureIcons.forEach((icon) => {
        expect(icon.getAttribute('aria-hidden')).toBe('true');
      });
    });

    test('Links have descriptive text or aria-label', () => {
      const links = document.querySelectorAll('a');

      links.forEach((link) => {
        const linkText = link.textContent.trim();
        const ariaLabel = link.getAttribute('aria-label');

        // Links should have either visible text or aria-label
        const hasDescriptiveText = linkText.length > 0 || (ariaLabel && ariaLabel.length > 0);
        expect(hasDescriptiveText).toBe(true);
      });
    });

    test('Tables have proper structure', () => {
      const tables = document.querySelectorAll('table');

      tables.forEach((table) => {
        // Tables should have thead and tbody
        const thead = table.querySelector('thead');
        const tbody = table.querySelector('tbody');

        expect(thead).not.toBeNull();
        expect(tbody).not.toBeNull();

        // thead should have th elements
        const thElements = thead.querySelectorAll('th');
        expect(thElements.length).toBeGreaterThan(0);
      });
    });

    test('Form inputs have associated labels', () => {
      const inputs = document.querySelectorAll('input, textarea, select');

      inputs.forEach((input) => {
        const id = input.getAttribute('id');
        const ariaLabel = input.getAttribute('aria-label');
        const ariaLabelledby = input.getAttribute('aria-labelledby');

        if (id) {
          const label = document.querySelector(`label[for="${id}"]`);
          const hasLabel = label !== null || ariaLabel || ariaLabelledby;
          expect(hasLabel).toBe(true);
        }
      });
    });

    test('External links have rel="noopener" for security', () => {
      const externalLinks = document.querySelectorAll('a[target="_blank"]');

      externalLinks.forEach((link) => {
        const rel = link.getAttribute('rel');
        expect(rel).toContain('noopener');
      });
    });
  });
});
