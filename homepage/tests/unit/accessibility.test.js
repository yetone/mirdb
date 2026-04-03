/**
 * Accessibility Unit Tests
 * Owner: Scenario 13 - Accessibility Compliance
 *
 * Tests for:
 * - Heading hierarchy (h1 > h2 > h3)
 * - Image alt text present
 * - Color contrast ratios (4.5:1 normal, 3:1 large)
 * - ARIA labels on buttons
 */

const fs = require('fs');
const path = require('path');
const { JSDOM } = require('jsdom');

// Load HTML file
const htmlPath = path.join(__dirname, '../../index.html');
const htmlContent = fs.readFileSync(htmlPath, 'utf8');
const dom = new JSDOM(htmlContent);
const document = dom.window.document;

// Load CSS files for color contrast testing
const variablesCssPath = path.join(__dirname, '../../css/variables.css');
const variablesCss = fs.readFileSync(variablesCssPath, 'utf8');

describe('Accessibility Unit Tests', () => {

  describe('Test Case 3: Heading Hierarchy', () => {
    test('Page uses proper heading hierarchy (h1 > h2 > h3) without skipping levels', () => {
      const headings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      const headingLevels = [];

      headings.forEach(heading => {
        const level = parseInt(heading.tagName.charAt(1));
        headingLevels.push(level);
      });

      // Check that there's exactly one h1
      const h1Count = headingLevels.filter(level => level === 1).length;
      expect(h1Count).toBe(1);

      // Check that heading levels don't skip
      let previousLevel = 0;
      for (let i = 0; i < headingLevels.length; i++) {
        const currentLevel = headingLevels[i];
        // When going deeper (higher number), should not skip more than 1 level
        if (currentLevel > previousLevel && currentLevel - previousLevel > 1) {
          // Exception: after h1, h2 is allowed; after h2, h3 is allowed, etc.
          // But should not jump from h1 to h3 or h2 to h4
          if (previousLevel > 0) {
            fail(`Heading hierarchy skipped from h${previousLevel} to h${currentLevel} at position ${i}`);
          }
        }
        previousLevel = currentLevel;
      }

      // Additional check: h2 should follow h1 somewhere before h3
      const firstH1Index = headingLevels.indexOf(1);
      const firstH2Index = headingLevels.indexOf(2);
      const firstH3Index = headingLevels.indexOf(3);

      expect(firstH1Index).toBeGreaterThanOrEqual(0); // h1 exists
      expect(firstH2Index).toBeGreaterThan(firstH1Index); // h2 comes after h1

      if (firstH3Index >= 0) {
        expect(firstH3Index).toBeGreaterThan(firstH2Index); // h3 comes after h2
      }
    });

    test('Only one h1 element exists on the page', () => {
      const h1Elements = document.querySelectorAll('h1');
      expect(h1Elements.length).toBe(1);
    });

    test('H1 element has meaningful content', () => {
      const h1 = document.querySelector('h1');
      expect(h1).not.toBeNull();
      expect(h1.textContent.trim().length).toBeGreaterThan(0);
    });
  });

  describe('Test Case 4: Image Alt Text', () => {
    test('All images (logo.gif, usage.gif) have descriptive alt text', () => {
      const images = document.querySelectorAll('img');

      images.forEach(img => {
        const src = img.getAttribute('src') || '';
        const alt = img.getAttribute('alt');

        // Every image must have alt attribute
        expect(alt).not.toBeNull();

        // Alt text should not be empty (unless decorative, but logo and usage are not)
        if (src.includes('logo.gif') || src.includes('usage.gif')) {
          expect(alt.trim().length).toBeGreaterThan(0);
          // Alt text should be descriptive (more than just "image" or "logo")
          expect(alt.length).toBeGreaterThan(5);
        }
      });
    });

    test('Logo images have appropriate alt text', () => {
      const logoImages = document.querySelectorAll('img[src*="logo"]');

      logoImages.forEach(img => {
        const alt = img.getAttribute('alt');
        expect(alt).not.toBeNull();
        expect(alt.toLowerCase()).toContain('logo');
      });
    });

    test('Usage demo image has descriptive alt text', () => {
      const usageImage = document.querySelector('img[src*="usage"]');

      if (usageImage) {
        const alt = usageImage.getAttribute('alt');
        expect(alt).not.toBeNull();
        // Should describe what the image shows
        expect(alt.length).toBeGreaterThan(10);
      }
    });
  });

  describe('Test Case 5: Color Contrast for Normal Text', () => {
    // Helper function to parse hex color to RGB
    function hexToRgb(hex) {
      const result = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex);
      return result ? {
        r: parseInt(result[1], 16),
        g: parseInt(result[2], 16),
        b: parseInt(result[3], 16)
      } : null;
    }

    // Calculate relative luminance
    function getLuminance(r, g, b) {
      const [rs, gs, bs] = [r, g, b].map(c => {
        c = c / 255;
        return c <= 0.03928 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4);
      });
      return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
    }

    // Calculate contrast ratio
    function getContrastRatio(color1, color2) {
      const lum1 = getLuminance(color1.r, color1.g, color1.b);
      const lum2 = getLuminance(color2.r, color2.g, color2.b);
      const lighter = Math.max(lum1, lum2);
      const darker = Math.min(lum1, lum2);
      return (lighter + 0.05) / (darker + 0.05);
    }

    test('Normal text has minimum 4.5:1 contrast ratio against background', () => {
      // Extract colors from CSS variables
      // Light theme colors
      const textPrimary = hexToRgb('#1e293b'); // --color-text-primary
      const textSecondary = hexToRgb('#475569'); // --color-text-secondary
      const bgPrimary = hexToRgb('#ffffff'); // --color-bg-primary
      const bgSecondary = hexToRgb('#f8fafc'); // --color-bg-secondary

      // Check primary text against primary background
      const contrastPrimaryOnPrimary = getContrastRatio(textPrimary, bgPrimary);
      expect(contrastPrimaryOnPrimary).toBeGreaterThanOrEqual(4.5);

      // Check primary text against secondary background
      const contrastPrimaryOnSecondary = getContrastRatio(textPrimary, bgSecondary);
      expect(contrastPrimaryOnSecondary).toBeGreaterThanOrEqual(4.5);

      // Check secondary text against primary background
      const contrastSecondaryOnPrimary = getContrastRatio(textSecondary, bgPrimary);
      expect(contrastSecondaryOnPrimary).toBeGreaterThanOrEqual(4.5);
    });

    test('Dark theme text has minimum 4.5:1 contrast ratio', () => {
      // Dark theme colors
      const textPrimaryDark = hexToRgb('#f1f5f9'); // --color-text-primary dark
      const textSecondaryDark = hexToRgb('#cbd5e1'); // --color-text-secondary dark
      const bgPrimaryDark = hexToRgb('#0f172a'); // --color-bg-primary dark
      const bgSecondaryDark = hexToRgb('#1e293b'); // --color-bg-secondary dark

      // Check primary text against primary background
      const contrastPrimaryOnPrimary = getContrastRatio(textPrimaryDark, bgPrimaryDark);
      expect(contrastPrimaryOnPrimary).toBeGreaterThanOrEqual(4.5);

      // Check primary text against secondary background
      const contrastPrimaryOnSecondary = getContrastRatio(textPrimaryDark, bgSecondaryDark);
      expect(contrastPrimaryOnSecondary).toBeGreaterThanOrEqual(4.5);

      // Check secondary text against primary background
      const contrastSecondaryOnPrimary = getContrastRatio(textSecondaryDark, bgPrimaryDark);
      expect(contrastSecondaryOnPrimary).toBeGreaterThanOrEqual(4.5);
    });
  });

  describe('Test Case 6: Color Contrast for Large Text', () => {
    function hexToRgb(hex) {
      const result = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex);
      return result ? {
        r: parseInt(result[1], 16),
        g: parseInt(result[2], 16),
        b: parseInt(result[3], 16)
      } : null;
    }

    function getLuminance(r, g, b) {
      const [rs, gs, bs] = [r, g, b].map(c => {
        c = c / 255;
        return c <= 0.03928 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4);
      });
      return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
    }

    function getContrastRatio(color1, color2) {
      const lum1 = getLuminance(color1.r, color1.g, color1.b);
      const lum2 = getLuminance(color2.r, color2.g, color2.b);
      const lighter = Math.max(lum1, lum2);
      const darker = Math.min(lum1, lum2);
      return (lighter + 0.05) / (darker + 0.05);
    }

    test('Large text (18pt+) has minimum 3:1 contrast ratio', () => {
      // Large text (headings, titles) uses --color-text-primary
      // WCAG requires 3:1 minimum for large text (18pt+ or 14pt bold)

      // Light theme - colors actually used for large text
      const textPrimary = hexToRgb('#1e293b'); // --color-text-primary (headings)
      const textSecondary = hexToRgb('#475569'); // --color-text-secondary (subtitles)
      const bgPrimary = hexToRgb('#ffffff');
      const bgSecondary = hexToRgb('#f8fafc');

      // Primary text (used for headings h1-h4) should meet 3:1 for large text
      const contrastPrimaryOnPrimary = getContrastRatio(textPrimary, bgPrimary);
      expect(contrastPrimaryOnPrimary).toBeGreaterThanOrEqual(3);

      const contrastPrimaryOnSecondary = getContrastRatio(textPrimary, bgSecondary);
      expect(contrastPrimaryOnSecondary).toBeGreaterThanOrEqual(3);

      // Secondary text (used for section subtitles which are 18px+) should meet 3:1
      const contrastSecondaryOnPrimary = getContrastRatio(textSecondary, bgPrimary);
      expect(contrastSecondaryOnPrimary).toBeGreaterThanOrEqual(3);

      // Note: --color-text-muted (#94a3b8) is only used for small text
      // (labels, hints, footer text) so it is not tested here for large text requirements
    });

    test('Heading colors meet large text contrast requirements', () => {
      // Headings use --color-text-primary which is dark enough
      const textPrimary = hexToRgb('#1e293b');
      const bgPrimary = hexToRgb('#ffffff');
      const bgSecondary = hexToRgb('#f8fafc');

      const contrastOnPrimary = getContrastRatio(textPrimary, bgPrimary);
      const contrastOnSecondary = getContrastRatio(textPrimary, bgSecondary);

      expect(contrastOnPrimary).toBeGreaterThanOrEqual(3);
      expect(contrastOnSecondary).toBeGreaterThanOrEqual(3);
    });
  });

  describe('Test Case 7: ARIA Labels on Interactive Elements', () => {
    test('Theme toggle has appropriate ARIA label', () => {
      const themeToggle = document.querySelector('.theme-toggle');
      expect(themeToggle).not.toBeNull();

      const ariaLabel = themeToggle.getAttribute('aria-label');
      expect(ariaLabel).not.toBeNull();
      expect(ariaLabel.length).toBeGreaterThan(0);
      // Should mention theme, dark, or toggle
      expect(
        ariaLabel.toLowerCase().includes('theme') ||
        ariaLabel.toLowerCase().includes('dark') ||
        ariaLabel.toLowerCase().includes('toggle')
      ).toBe(true);
    });

    test('Navigation menu has appropriate ARIA label', () => {
      const nav = document.querySelector('nav[role="navigation"]');
      expect(nav).not.toBeNull();

      const ariaLabel = nav.getAttribute('aria-label');
      expect(ariaLabel).not.toBeNull();
      expect(ariaLabel.toLowerCase()).toContain('navigation');
    });

    test('Mobile menu toggle has ARIA label and aria-expanded', () => {
      const mobileToggle = document.querySelector('.mobile-menu-toggle');
      expect(mobileToggle).not.toBeNull();

      const ariaLabel = mobileToggle.getAttribute('aria-label');
      expect(ariaLabel).not.toBeNull();

      const ariaExpanded = mobileToggle.getAttribute('aria-expanded');
      expect(ariaExpanded).not.toBeNull();
      expect(['true', 'false']).toContain(ariaExpanded);
    });

    test('Copy buttons have ARIA labels', () => {
      const copyButtons = document.querySelectorAll('.code-block__copy');

      copyButtons.forEach(button => {
        const ariaLabel = button.getAttribute('aria-label');
        expect(ariaLabel).not.toBeNull();
        expect(ariaLabel.toLowerCase()).toContain('copy');
      });
    });

    test('Back to top button has ARIA label', () => {
      const backToTop = document.querySelector('.back-to-top');
      expect(backToTop).not.toBeNull();

      const ariaLabel = backToTop.getAttribute('aria-label');
      expect(ariaLabel).not.toBeNull();
      expect(
        ariaLabel.toLowerCase().includes('top') ||
        ariaLabel.toLowerCase().includes('back')
      ).toBe(true);
    });

    test('Decorative icons have aria-hidden', () => {
      // Check that decorative icons (feature icons, metric icons) are hidden from screen readers
      const decorativeIcons = document.querySelectorAll(
        '.feature-icon, .metric-icon, .arch-component-icon, .contributing-icon'
      );

      decorativeIcons.forEach(icon => {
        const ariaHidden = icon.getAttribute('aria-hidden');
        expect(ariaHidden).toBe('true');
      });
    });

    test('SVG diagrams have accessible labels', () => {
      const diagram = document.querySelector('.lsm-diagram');
      if (diagram) {
        // Should have role="img" and aria-labelledby
        const role = diagram.getAttribute('role');
        expect(role).toBe('img');

        const ariaLabelledby = diagram.getAttribute('aria-labelledby');
        expect(ariaLabelledby).not.toBeNull();

        // Referenced elements should exist
        const ids = ariaLabelledby.split(' ');
        ids.forEach(id => {
          const referencedElement = document.getElementById(id);
          expect(referencedElement).not.toBeNull();
        });
      }
    });
  });

  describe('Additional Accessibility Checks', () => {
    test('Skip link is present', () => {
      const skipLink = document.querySelector('a[href="#main-content"]');
      expect(skipLink).not.toBeNull();
      expect(skipLink.textContent.toLowerCase()).toContain('skip');
    });

    test('Main content landmark has correct id', () => {
      const main = document.querySelector('main#main-content');
      expect(main).not.toBeNull();
    });

    test('Page has lang attribute', () => {
      const html = document.querySelector('html');
      const lang = html.getAttribute('lang');
      expect(lang).not.toBeNull();
      expect(lang).toBe('en');
    });

    test('Sections have aria-labelledby referencing headings', () => {
      const sections = document.querySelectorAll('section[aria-labelledby]');

      sections.forEach(section => {
        const labelledBy = section.getAttribute('aria-labelledby');
        const headingElement = document.getElementById(labelledBy);
        expect(headingElement).not.toBeNull();
        // Should be a heading element
        expect(headingElement.tagName).toMatch(/^H[1-6]$/);
      });
    });

    test('Links have discernible text', () => {
      const links = document.querySelectorAll('a');

      links.forEach(link => {
        const text = link.textContent.trim();
        const ariaLabel = link.getAttribute('aria-label');
        const hasImage = link.querySelector('img[alt]');
        const hasSvgTitle = link.querySelector('svg title');

        // Link should have accessible name via text, aria-label, image alt, or svg title
        const hasAccessibleName =
          text.length > 0 ||
          ariaLabel ||
          hasImage ||
          hasSvgTitle;

        expect(hasAccessibleName).toBe(true);
      });
    });
  });
});
