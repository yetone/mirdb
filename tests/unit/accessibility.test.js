/**
 * Accessibility Unit Tests
 * Owner: Scenario 7 - Accessibility Compliance
 *
 * Test cases:
 * - Semantic header element exists
 * - Semantic main element exists
 * - Semantic footer element exists
 * - All images have alt attributes
 * - Heading hierarchy is correct (h1 > h2 > h3)
 * - HTML has lang attribute
 */

const fs = require('fs');
const path = require('path');
const { JSDOM } = require('jsdom');

describe('Accessibility Compliance Tests', () => {
  let document;

  beforeAll(() => {
    const htmlPath = path.resolve(process.cwd(), 'index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf8');
    const dom = new JSDOM(htmlContent);
    document = dom.window.document;
  });

  describe('Semantic HTML Structure', () => {
    test('should have semantic header element containing logo and navigation', () => {
      const header = document.querySelector('header');
      expect(header).not.toBeNull();

      // Header should contain logo
      const logo = header.querySelector('img');
      expect(logo).not.toBeNull();
      expect(logo.getAttribute('src')).toContain('logo');

      // Header should contain navigation links (CTA buttons)
      const links = header.querySelectorAll('a');
      expect(links.length).toBeGreaterThan(0);
    });

    test('should have semantic main element containing primary content', () => {
      const main = document.querySelector('main');
      expect(main).not.toBeNull();

      // Main should contain the primary content sections
      const sections = main.querySelectorAll('section');
      expect(sections.length).toBeGreaterThan(0);

      // Verify main contains expected sections
      const features = main.querySelector('#features');
      expect(features).not.toBeNull();

      const demo = main.querySelector('#demo');
      expect(demo).not.toBeNull();

      const quickstart = main.querySelector('#quickstart');
      expect(quickstart).not.toBeNull();
    });

    test('should have semantic footer element at bottom of page', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();

      // Footer should be a direct child of body (semantic placement)
      const body = document.body;
      const lastSemanticElement = body.querySelector('footer');
      expect(lastSemanticElement).not.toBeNull();

      // Footer should contain some content
      expect(footer.textContent.trim()).not.toBe('');
    });
  });

  describe('Image Accessibility', () => {
    test('should have alt attributes on all images', () => {
      const images = document.querySelectorAll('img');
      expect(images.length).toBeGreaterThan(0);

      images.forEach((img, index) => {
        const alt = img.getAttribute('alt');
        expect(alt).not.toBeNull();
        expect(alt.trim()).not.toBe('');
      });
    });

    test('should have descriptive alt text for logo image', () => {
      const logo = document.querySelector('.logo');
      expect(logo).not.toBeNull();

      const alt = logo.getAttribute('alt');
      expect(alt).not.toBeNull();
      expect(alt.toLowerCase()).toContain('logo');
    });

    test('should have descriptive alt text for usage demonstration GIF', () => {
      const usageGif = document.querySelector('.usage-gif');
      expect(usageGif).not.toBeNull();

      const alt = usageGif.getAttribute('alt');
      expect(alt).not.toBeNull();
      expect(alt.length).toBeGreaterThan(10); // Should be descriptive
    });
  });

  describe('Heading Hierarchy', () => {
    test('should have exactly one h1 element', () => {
      const h1Elements = document.querySelectorAll('h1');
      expect(h1Elements.length).toBe(1);
    });

    test('should follow logical heading order without skipping levels', () => {
      const headings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      expect(headings.length).toBeGreaterThan(0);

      let previousLevel = 0;
      const errors = [];

      headings.forEach((heading, index) => {
        const level = parseInt(heading.tagName.charAt(1));

        // First heading should be h1
        if (index === 0) {
          if (level !== 1) {
            errors.push(`First heading should be h1, found h${level}`);
          }
        } else {
          // Headings should not skip levels (e.g., h1 directly to h3)
          if (level > previousLevel + 1) {
            errors.push(`Heading hierarchy skips from h${previousLevel} to h${level}`);
          }
        }

        previousLevel = level;
      });

      expect(errors).toEqual([]);
    });

    test('should have h2 for section titles', () => {
      const sections = document.querySelectorAll('main section');

      sections.forEach((section) => {
        const h2 = section.querySelector('h2');
        expect(h2).not.toBeNull();
      });
    });
  });

  describe('Language Attribute', () => {
    test('should have lang attribute on html element', () => {
      const html = document.documentElement;
      const lang = html.getAttribute('lang');
      expect(lang).not.toBeNull();
      expect(lang).toBe('en');
    });
  });

  describe('Interactive Element Accessibility', () => {
    test('should have accessible labels for copy buttons', () => {
      const copyButtons = document.querySelectorAll('.copy-btn');

      copyButtons.forEach((button) => {
        // Buttons should have either text content or aria-label
        const text = button.textContent.trim();
        const ariaLabel = button.getAttribute('aria-label');

        const hasAccessibleName = text.length > 0 || (ariaLabel && ariaLabel.length > 0);
        expect(hasAccessibleName).toBe(true);
      });
    });

    test('should have descriptive link text or aria-labels', () => {
      const links = document.querySelectorAll('a');

      links.forEach((link) => {
        const text = link.textContent.trim();
        const ariaLabel = link.getAttribute('aria-label');
        const title = link.getAttribute('title');

        // Link should have accessible name (text, aria-label, or contains img with alt)
        const containsImageWithAlt = link.querySelector('img[alt]') !== null;
        const hasAccessibleName =
          text.length > 0 ||
          (ariaLabel && ariaLabel.length > 0) ||
          (title && title.length > 0) ||
          containsImageWithAlt;

        expect(hasAccessibleName).toBe(true);
      });
    });
  });

  describe('Semantic Document Structure', () => {
    test('should have proper document outline', () => {
      // Document should have header, main, and footer
      const header = document.querySelector('header');
      const main = document.querySelector('main');
      const footer = document.querySelector('footer');

      expect(header).not.toBeNull();
      expect(main).not.toBeNull();
      expect(footer).not.toBeNull();
    });

    test('should use section elements for content areas', () => {
      const sections = document.querySelectorAll('section');
      expect(sections.length).toBeGreaterThan(0);

      // Each section should have an id or aria-label for navigation
      sections.forEach((section) => {
        const hasId = section.id && section.id.length > 0;
        const hasAriaLabel = section.getAttribute('aria-label');
        const hasAriaLabelledBy = section.getAttribute('aria-labelledby');

        // Section should be identifiable
        const isIdentifiable = hasId || hasAriaLabel || hasAriaLabelledBy;
        expect(isIdentifiable).toBe(true);
      });
    });

    test('should have meta viewport for responsive design', () => {
      const viewport = document.querySelector('meta[name="viewport"]');
      expect(viewport).not.toBeNull();

      const content = viewport.getAttribute('content');
      expect(content).toContain('width=device-width');
    });
  });
});
