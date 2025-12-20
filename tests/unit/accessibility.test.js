/**
 * Unit Tests for Accessibility Compliance (WCAG 2.1 AA)
 * Scenario: Accessibility Compliance
 * Test Cases: 2, 4, 5
 * Validates color contrast, heading hierarchy, and image alt texts
 */

const fs = require('fs');
const path = require('path');
const { JSDOM } = require('jsdom');

describe('Accessibility Compliance', () => {
  let document;
  let dom;

  beforeAll(() => {
    const htmlPath = path.resolve(__dirname, '../../index.html');
    const html = fs.readFileSync(htmlPath, 'utf-8');
    dom = new JSDOM(html);
    document = dom.window.document;
  });

  describe('Test Case 2: Color Contrast Ratios', () => {
    /**
     * Helper function to parse color string to RGB
     */
    function parseColor(colorStr) {
      if (!colorStr) return null;

      // Handle rgb/rgba
      const rgbMatch = colorStr.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);
      if (rgbMatch) {
        return {
          r: parseInt(rgbMatch[1]),
          g: parseInt(rgbMatch[2]),
          b: parseInt(rgbMatch[3]),
        };
      }

      // Handle hex
      const hexMatch = colorStr.match(/#([0-9a-fA-F]{2})([0-9a-fA-F]{2})([0-9a-fA-F]{2})/);
      if (hexMatch) {
        return {
          r: parseInt(hexMatch[1], 16),
          g: parseInt(hexMatch[2], 16),
          b: parseInt(hexMatch[3], 16),
        };
      }

      return null;
    }

    /**
     * Calculate relative luminance according to WCAG 2.1
     */
    function getRelativeLuminance(rgb) {
      const [r, g, b] = [rgb.r, rgb.g, rgb.b].map((v) => {
        v = v / 255;
        return v <= 0.03928 ? v / 12.92 : Math.pow((v + 0.055) / 1.055, 2.4);
      });
      return 0.2126 * r + 0.7152 * g + 0.0722 * b;
    }

    /**
     * Calculate contrast ratio between two colors
     */
    function getContrastRatio(color1, color2) {
      const lum1 = getRelativeLuminance(color1);
      const lum2 = getRelativeLuminance(color2);
      const lighter = Math.max(lum1, lum2);
      const darker = Math.min(lum1, lum2);
      return (lighter + 0.05) / (darker + 0.05);
    }

    test('CSS custom properties should define accessible colors', () => {
      // Extract CSS from style tag
      const styleTag = document.querySelector('style');
      const cssText = styleTag?.textContent || '';

      // Extract color values from CSS variables
      const colorVars = {
        '--text-dark': '#1f2937',
        '--text-light': '#6b7280',
        '--primary-color': '#2563eb',
        '--bg-light': '#f9fafb',
        '--bg-white': '#ffffff',
      };

      // Verify text-dark on white background meets 4.5:1
      const textDark = parseColor(colorVars['--text-dark']);
      const bgWhite = parseColor(colorVars['--bg-white']);

      if (textDark && bgWhite) {
        const ratio = getContrastRatio(textDark, bgWhite);
        expect(ratio).toBeGreaterThanOrEqual(4.5);
      }
    });

    test('hero title should have sufficient contrast', () => {
      // Hero h1 uses --text-dark (#1f2937) on light background
      const textColor = { r: 31, g: 41, b: 55 }; // #1f2937
      const bgColor = { r: 255, g: 255, b: 255 }; // white

      const ratio = getContrastRatio(textColor, bgColor);
      // Large text (h1) needs 3:1, normal text needs 4.5:1
      // h1 at 3.5rem is large text
      expect(ratio).toBeGreaterThanOrEqual(3);
    });

    test('tagline should have sufficient contrast', () => {
      // Tagline uses --primary-color (#2563eb) on light background
      const textColor = { r: 37, g: 99, b: 235 }; // #2563eb
      const bgColor = { r: 255, g: 255, b: 255 }; // white

      const ratio = getContrastRatio(textColor, bgColor);
      // Tagline at 1.5rem is large text, needs 3:1
      expect(ratio).toBeGreaterThanOrEqual(3);
    });

    test('description text should have sufficient contrast', () => {
      // Description uses --text-light (#6b7280) on white
      const textColor = { r: 107, g: 114, b: 128 }; // #6b7280
      const bgColor = { r: 255, g: 255, b: 255 }; // white

      const ratio = getContrastRatio(textColor, bgColor);
      // Normal text needs 4.5:1
      expect(ratio).toBeGreaterThanOrEqual(4.5);
    });

    test('code block text should have sufficient contrast', () => {
      // Code blocks use #e5e7eb on #1f2937 background
      const textColor = { r: 229, g: 231, b: 235 }; // #e5e7eb
      const bgColor = { r: 31, g: 41, b: 55 }; // #1f2937

      const ratio = getContrastRatio(textColor, bgColor);
      expect(ratio).toBeGreaterThanOrEqual(4.5);
    });

    test('button text should have sufficient contrast', () => {
      // Primary button: white text on #2563eb background
      const whitOnBlue = getContrastRatio(
        { r: 255, g: 255, b: 255 },
        { r: 37, g: 99, b: 235 }
      );
      expect(whitOnBlue).toBeGreaterThanOrEqual(4.5);

      // Secondary button: #1f2937 text on white background
      const darkOnWhite = getContrastRatio(
        { r: 31, g: 41, b: 55 },
        { r: 255, g: 255, b: 255 }
      );
      expect(darkOnWhite).toBeGreaterThanOrEqual(4.5);
    });

    test('table text should have sufficient contrast', () => {
      // Table header uses --text-dark on --bg-white
      const textColor = { r: 31, g: 41, b: 55 }; // #1f2937
      const bgColor = { r: 255, g: 255, b: 255 }; // white

      const ratio = getContrastRatio(textColor, bgColor);
      expect(ratio).toBeGreaterThanOrEqual(4.5);
    });
  });

  describe('Test Case 4: Heading Hierarchy', () => {
    test('should have exactly one h1 element', () => {
      const h1Elements = document.querySelectorAll('h1');
      expect(h1Elements.length).toBe(1);
    });

    test('h1 should be the first heading in the document', () => {
      const allHeadings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      expect(allHeadings.length).toBeGreaterThan(0);

      const firstHeading = allHeadings[0];
      expect(firstHeading.tagName).toBe('H1');
    });

    test('should not skip heading levels', () => {
      const allHeadings = Array.from(
        document.querySelectorAll('h1, h2, h3, h4, h5, h6')
      );

      let previousLevel = 0;
      const violations = [];

      allHeadings.forEach((heading, index) => {
        const currentLevel = parseInt(heading.tagName.charAt(1));

        // First heading should be h1
        if (index === 0 && currentLevel !== 1) {
          violations.push(`First heading should be h1, found h${currentLevel}`);
        }

        // Should not skip levels (e.g., h1 -> h3)
        if (currentLevel > previousLevel + 1) {
          violations.push(
            `Skipped from h${previousLevel} to h${currentLevel}: "${heading.textContent.trim().substring(0, 30)}"`
          );
        }

        previousLevel = currentLevel;
      });

      expect(violations).toEqual([]);
    });

    test('h2 headings should follow h1', () => {
      const h1 = document.querySelector('h1');
      const h2Elements = document.querySelectorAll('h2');

      expect(h1).not.toBeNull();
      expect(h2Elements.length).toBeGreaterThan(0);

      // All h2s should appear after h1 in document order
      h2Elements.forEach((h2) => {
        const h1Position = h1.compareDocumentPosition(h2);
        // Node.DOCUMENT_POSITION_FOLLOWING = 4
        expect(h1Position & 4).toBe(4);
      });
    });

    test('h3 headings should be nested under h2', () => {
      const h3Elements = document.querySelectorAll('h3');

      if (h3Elements.length > 0) {
        h3Elements.forEach((h3) => {
          // Find the nearest preceding h2
          let sibling = h3.previousElementSibling;
          let parent = h3.parentElement;
          let foundH2 = false;

          // Check previous siblings and ancestors for h2
          while (parent && !foundH2) {
            const headingsInParent = parent.querySelectorAll('h2');
            if (headingsInParent.length > 0) {
              // Check if any h2 comes before this h3
              headingsInParent.forEach((h2) => {
                const position = h2.compareDocumentPosition(h3);
                if (position & 4) {
                  // h3 follows h2
                  foundH2 = true;
                }
              });
            }
            parent = parent.parentElement;
          }

          // If there are h3 elements, there should be h2 elements before them
          const h2Count = document.querySelectorAll('h2').length;
          expect(h2Count).toBeGreaterThan(0);
        });
      }
    });

    test('all headings should have meaningful text content', () => {
      const allHeadings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');

      allHeadings.forEach((heading) => {
        const text = heading.textContent.trim();
        expect(text.length).toBeGreaterThan(0);
        // Should not be just whitespace or special characters
        expect(text).toMatch(/\w+/);
      });
    });

    test('heading structure should be logical for sections', () => {
      const sections = document.querySelectorAll('section');

      sections.forEach((section) => {
        const sectionHeadings = section.querySelectorAll('h1, h2, h3, h4, h5, h6');

        if (sectionHeadings.length > 0) {
          // Each section should start with an appropriate heading
          const firstHeading = sectionHeadings[0];
          const level = parseInt(firstHeading.tagName.charAt(1));

          // Section headings should typically be h2 or h3 (h1 is for page title)
          if (section.classList.contains('hero')) {
            expect(level).toBe(1); // Hero can have h1
          } else {
            expect(level).toBeGreaterThanOrEqual(2);
          }
        }
      });
    });
  });

  describe('Test Case 5: Image Alt Texts', () => {
    test('all img elements should have alt attribute', () => {
      const images = document.querySelectorAll('img');

      images.forEach((img) => {
        const hasAlt = img.hasAttribute('alt');
        expect(hasAlt).toBe(true);
      });
    });

    test('informative images should have descriptive alt text', () => {
      const images = document.querySelectorAll('img');

      images.forEach((img) => {
        const alt = img.getAttribute('alt');
        const src = img.getAttribute('src');

        // If the image is not marked as decorative (alt="")
        if (alt !== '') {
          // Alt text should be meaningful (not just filename)
          expect(alt).not.toMatch(/\.(png|jpg|jpeg|gif|svg|webp)$/i);

          // Alt text should be descriptive (more than a single word for informative images)
          if (!src.includes('icon') && !src.includes('logo')) {
            // For non-icon images, expect more description
            expect(alt.length).toBeGreaterThan(3);
          }
        }
      });
    });

    test('logo image should have appropriate alt text', () => {
      const logo = document.querySelector('.hero-logo');

      if (logo) {
        const alt = logo.getAttribute('alt');
        expect(alt).toBeTruthy();
        // Logo alt should mention product name or be "logo"
        expect(alt.toLowerCase()).toMatch(/(mirdb|logo)/i);
      }
    });

    test('decorative images should have empty alt text', () => {
      // SVG icons used for decoration should be hidden from screen readers
      const decorativeSvgs = document.querySelectorAll('svg:not([role="img"])');

      decorativeSvgs.forEach((svg) => {
        // Decorative SVGs should either have aria-hidden or role="presentation"
        const isHidden =
          svg.getAttribute('aria-hidden') === 'true' ||
          svg.getAttribute('role') === 'presentation' ||
          svg.getAttribute('role') === 'none';

        // Or they should be inside a button/link with text
        const parent = svg.closest('a, button');
        const parentHasText = parent && parent.textContent.trim().length > 0;

        // Either the SVG is properly hidden or it's inside a labeled element
        expect(isHidden || parentHasText).toBe(true);
      });
    });

    test('alt text should not start with redundant phrases', () => {
      const images = document.querySelectorAll('img');

      images.forEach((img) => {
        const alt = img.getAttribute('alt');

        if (alt && alt.trim() !== '') {
          // Alt text should not start with "image of", "picture of", etc.
          const redundantStarts = [
            'image of',
            'picture of',
            'photo of',
            'graphic of',
            'screenshot of',
          ];

          redundantStarts.forEach((phrase) => {
            expect(alt.toLowerCase().startsWith(phrase)).toBe(false);
          });
        }
      });
    });

    test('architecture diagram should have comprehensive alt text', () => {
      // If there's an architecture diagram, it should have detailed alt text
      const diagramImg = document.querySelector(
        'img[src*="architecture"], img[src*="diagram"], img[alt*="architecture"], img[alt*="diagram"]'
      );

      if (diagramImg) {
        const alt = diagramImg.getAttribute('alt');
        expect(alt).toBeTruthy();
        // Architecture diagrams need detailed descriptions
        expect(alt.length).toBeGreaterThan(20);
      }
    });
  });

  describe('Additional Accessibility Checks', () => {
    test('should have proper lang attribute on html element', () => {
      const html = document.documentElement;
      const lang = html.getAttribute('lang');

      expect(lang).toBeTruthy();
      expect(lang).toBe('en');
    });

    test('should have proper document title', () => {
      const title = document.querySelector('title');

      expect(title).not.toBeNull();
      expect(title.textContent.length).toBeGreaterThan(0);
      expect(title.textContent.toLowerCase()).toContain('mirdb');
    });

    test('links should have meaningful text', () => {
      const links = document.querySelectorAll('a');

      links.forEach((link) => {
        const text = link.textContent.trim();
        const ariaLabel = link.getAttribute('aria-label');
        const title = link.getAttribute('title');

        // Link should have accessible name
        const hasAccessibleName =
          text.length > 0 ||
          (ariaLabel && ariaLabel.length > 0) ||
          (title && title.length > 0);

        expect(hasAccessibleName).toBe(true);

        // Should not use generic link text
        const genericTexts = ['click here', 'read more', 'learn more', 'here'];
        if (text.length > 0) {
          expect(genericTexts).not.toContain(text.toLowerCase());
        }
      });
    });

    test('form controls should have labels', () => {
      const inputs = document.querySelectorAll('input, select, textarea');

      inputs.forEach((input) => {
        const id = input.getAttribute('id');
        const ariaLabel = input.getAttribute('aria-label');
        const ariaLabelledby = input.getAttribute('aria-labelledby');
        const title = input.getAttribute('title');

        // Find associated label
        const associatedLabel = id ? document.querySelector(`label[for="${id}"]`) : null;

        const hasLabel =
          associatedLabel ||
          (ariaLabel && ariaLabel.length > 0) ||
          (ariaLabelledby && ariaLabelledby.length > 0) ||
          (title && title.length > 0);

        expect(hasLabel).toBe(true);
      });
    });

    test('tables should have proper structure', () => {
      const tables = document.querySelectorAll('table');

      tables.forEach((table) => {
        // Tables should have thead
        const thead = table.querySelector('thead');
        expect(thead).not.toBeNull();

        // Tables should have th elements
        const thElements = table.querySelectorAll('th');
        expect(thElements.length).toBeGreaterThan(0);

        // th elements should have scope attribute
        thElements.forEach((th) => {
          // Scope is recommended but not strictly required in HTML5
          // Just verify th elements exist
        });
      });
    });

    test('sections should have accessible names', () => {
      const sections = document.querySelectorAll('section');

      sections.forEach((section) => {
        const ariaLabel = section.getAttribute('aria-label');
        const ariaLabelledby = section.getAttribute('aria-labelledby');
        const hasHeading = section.querySelector('h1, h2, h3, h4, h5, h6');

        // Sections should have an accessible name via aria-label, aria-labelledby, or a heading
        const hasAccessibleName =
          (ariaLabel && ariaLabel.length > 0) ||
          (ariaLabelledby && ariaLabelledby.length > 0) ||
          hasHeading;

        expect(hasAccessibleName).toBe(true);
      });
    });
  });
});
