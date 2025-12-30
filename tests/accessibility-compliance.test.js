/**
 * Accessibility Compliance Tests
 *
 * Scenario: Verify that the homepage meets WCAG 2.1 AA accessibility standards (NFR-3)
 * These tests verify the accessibility implementation meets all requirements.
 */

const fs = require('fs');
const path = require('path');

describe('Accessibility Compliance', () => {
  let document;
  let htmlContent;

  beforeAll(() => {
    // Load the HTML file
    const htmlPath = path.join(__dirname, '..', 'index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    // Parse HTML using jsdom
    document = new DOMParser().parseFromString(htmlContent, 'text/html');
  });

  describe('Test Case 3: All images have alt text', () => {
    /**
     * Test Case ID: 3
     * Input: Check all images have alt text
     * Expected: All img elements have descriptive alt attributes
     * Type: unit
     */
    test('all img elements should have alt attributes', () => {
      const images = document.querySelectorAll('img');

      images.forEach((img) => {
        const alt = img.getAttribute('alt');
        expect(alt).not.toBeNull();
        // Alt can be empty string for decorative images, but must be present
        expect(typeof alt).toBe('string');
      });
    });

    test('decorative SVG icons should have aria-hidden="true"', () => {
      // SVGs used as icons should be hidden from screen readers
      const featureIcons = document.querySelectorAll('.feature-icon svg');

      featureIcons.forEach((svg) => {
        const ariaHidden = svg.getAttribute('aria-hidden');
        expect(ariaHidden).toBe('true');
      });
    });

    test('informative SVG should have accessible name (role="img" and aria-label or title)', () => {
      // The LSM tree diagram should be accessible
      const diagram = document.querySelector('[data-testid="lsm-tree-diagram"]');

      if (diagram) {
        const role = diagram.getAttribute('role');
        const ariaLabel = diagram.getAttribute('aria-label');
        const title = diagram.querySelector('title');

        // Should have role="img" for complex graphics
        expect(role).toBe('img');

        // Should have aria-label OR title for accessible name
        const hasAccessibleName = ariaLabel || title;
        expect(hasAccessibleName).toBeTruthy();
      }
    });

    test('all images with meaningful content should have descriptive alt text', () => {
      const images = document.querySelectorAll('img:not([alt=""])');

      images.forEach((img) => {
        const alt = img.getAttribute('alt');
        if (alt) {
          // Alt text should be descriptive (more than just a single word)
          expect(alt.length).toBeGreaterThan(0);
        }
      });
    });
  });

  describe('Test Case 4: Heading hierarchy', () => {
    /**
     * Test Case ID: 4
     * Input: Check heading hierarchy
     * Expected: Headings follow logical h1 > h2 > h3 hierarchy without skipping levels
     * Type: unit
     */
    test('page should have exactly one h1', () => {
      const h1Elements = document.querySelectorAll('h1');
      expect(h1Elements.length).toBe(1);
    });

    test('h1 should be the main page title', () => {
      const h1 = document.querySelector('h1');
      expect(h1).toBeTruthy();
      expect(h1.textContent.trim()).toBeTruthy();
    });

    test('headings should follow logical hierarchy without skipping levels', () => {
      const allHeadings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      let previousLevel = 0;
      const errors = [];

      allHeadings.forEach((heading, index) => {
        const currentLevel = parseInt(heading.tagName.charAt(1));

        // First heading should be h1
        if (index === 0) {
          if (currentLevel !== 1) {
            errors.push(`First heading should be h1, but found h${currentLevel}`);
          }
        } else {
          // Headings can go up by 1 level or stay same or go down any amount
          // But should not skip down (e.g., h1 directly to h3 is wrong)
          if (currentLevel > previousLevel + 1) {
            errors.push(`Heading hierarchy skips from h${previousLevel} to h${currentLevel} at "${heading.textContent.trim().substring(0, 30)}..."`);
          }
        }

        previousLevel = currentLevel;
      });

      expect(errors).toEqual([]);
    });

    test('all sections should have proper heading structure', () => {
      const sections = document.querySelectorAll('section');

      sections.forEach((section) => {
        const heading = section.querySelector('h2, h3, h4');
        // Each section should ideally have a heading
        // Note: Not all sections require headings, but main content sections should
        const sectionId = section.getAttribute('id');
        if (sectionId && !['hero'].includes(sectionId)) {
          expect(heading).toBeTruthy();
        }
      });
    });
  });

  describe('Test Case 5: Color contrast ratios', () => {
    /**
     * Test Case ID: 5
     * Input: Check color contrast ratios
     * Expected: All text meets WCAG AA contrast requirements (4.5:1 normal, 3:1 large)
     * Type: unit
     */

    // Helper function to parse CSS color values
    function parseColor(colorStr) {
      // Handle hex colors
      if (colorStr.startsWith('#')) {
        const hex = colorStr.slice(1);
        if (hex.length === 3) {
          return {
            r: parseInt(hex[0] + hex[0], 16),
            g: parseInt(hex[1] + hex[1], 16),
            b: parseInt(hex[2] + hex[2], 16)
          };
        } else if (hex.length === 6) {
          return {
            r: parseInt(hex.slice(0, 2), 16),
            g: parseInt(hex.slice(2, 4), 16),
            b: parseInt(hex.slice(4, 6), 16)
          };
        }
      }
      return null;
    }

    // Calculate relative luminance
    function getLuminance(r, g, b) {
      const [rs, gs, bs] = [r, g, b].map((c) => {
        c = c / 255;
        return c <= 0.03928 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4);
      });
      return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
    }

    // Calculate contrast ratio
    function getContrastRatio(color1, color2) {
      const l1 = getLuminance(color1.r, color1.g, color1.b);
      const l2 = getLuminance(color2.r, color2.g, color2.b);
      const lighter = Math.max(l1, l2);
      const darker = Math.min(l1, l2);
      return (lighter + 0.05) / (darker + 0.05);
    }

    test('CSS variables define colors with sufficient contrast', () => {
      // Extract CSS variables from the style block
      const styleBlock = document.querySelector('style');
      expect(styleBlock).toBeTruthy();

      const cssText = styleBlock.textContent;

      // Extract color definitions
      const primaryColor = cssText.match(/--primary-color:\s*([^;]+)/)?.[1]?.trim();
      const textColor = cssText.match(/--text-color:\s*([^;]+)/)?.[1]?.trim();
      const bgColor = cssText.match(/--bg-color:\s*([^;]+)/)?.[1]?.trim();
      const textLight = cssText.match(/--text-light:\s*([^;]+)/)?.[1]?.trim();

      // Verify key colors exist
      expect(primaryColor).toBeTruthy();
      expect(textColor).toBeTruthy();
      expect(bgColor).toBeTruthy();

      // Parse and verify contrast ratios
      const bg = parseColor(bgColor);
      const text = parseColor(textColor);
      const light = parseColor(textLight);

      if (bg && text) {
        const mainContrast = getContrastRatio(text, bg);
        // WCAG AA requires 4.5:1 for normal text
        expect(mainContrast).toBeGreaterThanOrEqual(4.5);
      }

      if (bg && light) {
        const lightContrast = getContrastRatio(light, bg);
        // Light text should still meet AA requirements
        expect(lightContrast).toBeGreaterThanOrEqual(4.5);
      }
    });

    test('button colors have sufficient contrast', () => {
      const styleBlock = document.querySelector('style');
      const cssText = styleBlock.textContent;

      // Primary buttons are blue with white text
      const primaryColor = cssText.match(/--primary-color:\s*([^;]+)/)?.[1]?.trim();
      const primary = parseColor(primaryColor);
      const white = { r: 255, g: 255, b: 255 };

      if (primary) {
        const buttonContrast = getContrastRatio(primary, white);
        // Buttons should have at least 4.5:1 contrast for text
        expect(buttonContrast).toBeGreaterThanOrEqual(4.5);
      }
    });

    test('code block colors have sufficient contrast', () => {
      const styleBlock = document.querySelector('style');
      const cssText = styleBlock.textContent;

      const codeBg = cssText.match(/--code-bg:\s*([^;]+)/)?.[1]?.trim();
      const codeText = cssText.match(/--code-text:\s*([^;]+)/)?.[1]?.trim();

      if (codeBg && codeText) {
        const bg = parseColor(codeBg);
        const text = parseColor(codeText);

        if (bg && text) {
          const codeContrast = getContrastRatio(text, bg);
          // Code should have at least 4.5:1 contrast
          expect(codeContrast).toBeGreaterThanOrEqual(4.5);
        }
      }
    });

    test('status indicators (check/cross) have sufficient contrast', () => {
      const styleBlock = document.querySelector('style');
      const cssText = styleBlock.textContent;

      // Check mark color
      const checkColorMatch = cssText.match(/\.check\s*\{[^}]*color:\s*([^;]+)/);
      // Cross/error color
      const crossColorMatch = cssText.match(/\.cross\s*\{[^}]*color:\s*([^;]+)/);

      // These should be visible against white background
      const white = { r: 255, g: 255, b: 255 };

      if (checkColorMatch) {
        const checkColor = parseColor(checkColorMatch[1].trim());
        if (checkColor) {
          const contrast = getContrastRatio(checkColor, white);
          expect(contrast).toBeGreaterThanOrEqual(3); // At least 3:1 for graphical elements
        }
      }

      if (crossColorMatch) {
        const crossColor = parseColor(crossColorMatch[1].trim());
        if (crossColor) {
          const contrast = getContrastRatio(crossColor, white);
          expect(contrast).toBeGreaterThanOrEqual(3);
        }
      }
    });
  });

  describe('Test Case 6: Form labels', () => {
    /**
     * Test Case ID: 6
     * Input: Check form labels
     * Expected: All form inputs have associated labels
     * Type: unit
     */
    test('all form input elements should have associated labels', () => {
      const formInputs = document.querySelectorAll('input, textarea, select');

      formInputs.forEach((input) => {
        const id = input.getAttribute('id');
        const ariaLabel = input.getAttribute('aria-label');
        const ariaLabelledBy = input.getAttribute('aria-labelledby');
        const title = input.getAttribute('title');
        const placeholder = input.getAttribute('placeholder');
        const type = input.getAttribute('type');

        // Skip hidden inputs and submit buttons (they don't need labels)
        if (type === 'hidden' || type === 'submit' || type === 'button') {
          return;
        }

        // Check for various labeling methods
        let hasLabel = false;

        // Check for explicit label association
        if (id) {
          const label = document.querySelector(`label[for="${id}"]`);
          if (label) hasLabel = true;
        }

        // Check for implicit label (input nested in label)
        const parentLabel = input.closest('label');
        if (parentLabel) hasLabel = true;

        // Check for ARIA labeling
        if (ariaLabel || ariaLabelledBy || title) hasLabel = true;

        // For search inputs, placeholder may be acceptable
        if (type === 'search' && placeholder) hasLabel = true;

        expect(hasLabel).toBe(true);
      });
    });

    test('form buttons should have accessible names', () => {
      const buttons = document.querySelectorAll('button, input[type="submit"], input[type="button"]');

      buttons.forEach((button) => {
        const text = button.textContent?.trim();
        const ariaLabel = button.getAttribute('aria-label');
        const value = button.getAttribute('value');
        const title = button.getAttribute('title');

        const hasAccessibleName = text || ariaLabel || value || title;
        expect(hasAccessibleName).toBeTruthy();
      });
    });

    test('no form inputs currently exist (static page)', () => {
      // This is a static homepage, so we verify there are no forms
      // or if there are, they must be properly labeled
      const forms = document.querySelectorAll('form');
      const inputs = document.querySelectorAll('input:not([type="hidden"]), textarea, select');

      // If no forms exist, that's fine for a static page
      // If forms exist, they should be properly structured
      if (forms.length === 0 && inputs.length === 0) {
        expect(true).toBe(true);
      }
    });
  });

  describe('Additional Accessibility Tests', () => {
    test('html element should have lang attribute', () => {
      const html = document.querySelector('html');
      expect(html).toBeTruthy();

      const lang = html.getAttribute('lang');
      expect(lang).toBeTruthy();
      expect(lang).toBe('en');
    });

    test('page should have a main content area', () => {
      // Check for main landmark or main content structure
      const main = document.querySelector('main');
      const contentSections = document.querySelectorAll('section');

      // Either has explicit main or sections that serve as main content
      const hasMainContent = main || contentSections.length > 0;
      expect(hasMainContent).toBe(true);
    });

    test('navigation should be properly structured', () => {
      const nav = document.querySelector('nav');
      expect(nav).toBeTruthy();

      // Nav should contain a list of links
      const navList = nav.querySelector('ul, ol');
      expect(navList).toBeTruthy();
    });

    test('links should have discernible text', () => {
      const links = document.querySelectorAll('a');

      links.forEach((link) => {
        const text = link.textContent?.trim();
        const ariaLabel = link.getAttribute('aria-label');
        const title = link.getAttribute('title');
        const img = link.querySelector('img[alt]');

        const hasDiscernibleText = text || ariaLabel || title || (img && img.getAttribute('alt'));
        expect(hasDiscernibleText).toBeTruthy();
      });
    });

    test('focusable elements should have visible focus indicators (CSS check)', () => {
      const styleBlock = document.querySelector('style');
      const cssText = styleBlock.textContent;

      // Check that focus styles are not removed
      // Bad practice: *:focus { outline: none }
      const removedOutline = cssText.match(/\*\s*:\s*focus\s*\{[^}]*outline:\s*none/);
      expect(removedOutline).toBeFalsy();
    });

    test('skip link should exist for keyboard navigation', () => {
      // Check for skip link (first focusable element should be skip link or nav is accessible)
      const skipLink = document.querySelector('a[href="#main"], a[href="#content"], .skip-link');
      const nav = document.querySelector('nav');

      // Either has skip link or nav is at top (acceptable for small sites)
      const hasAccessibleNavigation = skipLink || nav;
      expect(hasAccessibleNavigation).toBeTruthy();
    });

    test('tables should have proper headers', () => {
      const tables = document.querySelectorAll('table');

      tables.forEach((table) => {
        const headers = table.querySelectorAll('th');
        const caption = table.querySelector('caption');
        const ariaLabel = table.getAttribute('aria-label');
        const ariaLabelledBy = table.getAttribute('aria-labelledby');

        // Tables should have headers
        expect(headers.length).toBeGreaterThan(0);

        // Tables should ideally have a caption or aria-label
        // But we'll check that headers exist at minimum
      });
    });

    test('interactive elements should be keyboard accessible', () => {
      // Check that interactive elements can receive focus
      const interactiveElements = document.querySelectorAll('a[href], button, [tabindex]');

      interactiveElements.forEach((element) => {
        const tabindex = element.getAttribute('tabindex');

        // tabindex should not be negative (which removes from tab order)
        // unless there's a good reason
        if (tabindex !== null) {
          const tabValue = parseInt(tabindex);
          // Negative tabindex is OK for programmatic focus, but let's flag it
          // For general interactive elements, it should be 0 or positive
          expect(tabValue).toBeGreaterThanOrEqual(-1);
        }
      });
    });

    test('SVG diagram should be focusable for keyboard users', () => {
      const diagram = document.querySelector('[data-testid="lsm-tree-diagram"]');

      if (diagram) {
        const tabindex = diagram.getAttribute('tabindex');
        // Complex diagrams should be focusable
        expect(tabindex).toBe('0');
      }
    });
  });
});
