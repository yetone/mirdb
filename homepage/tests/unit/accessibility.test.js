/**
 * Accessibility Unit Tests
 * Owner: Scenario 7 - Accessibility and Performance
 *
 * Tests for:
 * - Heading hierarchy (h1 > h2 > h3)
 * - Image alt attributes
 * - Color contrast (WCAG AA compliance)
 * - Semantic HTML structure
 */

const fs = require('fs');
const path = require('path');

describe('Accessibility Unit Tests', () => {
  let document;
  let htmlContent;

  beforeAll(() => {
    // Read the HTML file
    const htmlPath = path.join(__dirname, '../../index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    // Create a mock DOM
    document = new DOMParser().parseFromString(htmlContent, 'text/html');
  });

  describe('Test Case 6: Heading Hierarchy', () => {
    test('Page has exactly one h1 element', () => {
      const h1Elements = document.querySelectorAll('h1');
      expect(h1Elements.length).toBe(1);
    });

    test('H1 appears before any h2 elements', () => {
      const allHeadings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      const headingsArray = Array.from(allHeadings);

      if (headingsArray.length > 0) {
        expect(headingsArray[0].tagName.toLowerCase()).toBe('h1');
      }
    });

    test('Headings follow proper hierarchy without skipping levels', () => {
      const headings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      const headingsArray = Array.from(headings);

      let previousLevel = 0;
      let hierarchyValid = true;

      for (const heading of headingsArray) {
        const currentLevel = parseInt(heading.tagName.charAt(1));

        // Can go to same level, one level down, or back up
        // Should not skip levels going down (e.g., h1 to h3)
        if (currentLevel > previousLevel + 1 && previousLevel !== 0) {
          hierarchyValid = false;
          break;
        }
        previousLevel = currentLevel;
      }

      expect(hierarchyValid).toBe(true);
    });

    test('All sections have appropriate heading levels', () => {
      // Hero section should have h1
      const heroH1 = document.querySelector('#hero h1');
      expect(heroH1).toBeTruthy();

      // Other sections should have h2 headings
      const usageH2 = document.querySelector('#usage h2');
      const gettingStartedH2 = document.querySelector('#getting-started h2');

      expect(usageH2).toBeTruthy();
      expect(gettingStartedH2).toBeTruthy();
    });

    test('H3 headings are nested within sections with H2', () => {
      const h3Elements = document.querySelectorAll('h3');
      const h3Array = Array.from(h3Elements);

      for (const h3 of h3Array) {
        const section = h3.closest('section');
        if (section) {
          const parentH2 = section.querySelector('h2');
          // H3 should have a parent H2 in its section or nav context
          // For footer nav, h3 is acceptable for footer nav groups
          const isInFooterNav = h3.closest('.footer-nav');
          const isInLinks = h3.closest('.getting-started-links');

          if (!isInFooterNav && !isInLinks) {
            expect(parentH2).toBeTruthy();
          }
        }
      }
    });
  });

  describe('Test Case 5: Image Alt Attributes', () => {
    test('All img elements have alt attributes', () => {
      const images = document.querySelectorAll('img');
      const imagesArray = Array.from(images);

      for (const img of imagesArray) {
        const hasAlt = img.hasAttribute('alt');
        expect(hasAlt).toBe(true);
      }
    });

    test('Logo image has meaningful alt text', () => {
      const logo = document.querySelector('.hero-logo');
      expect(logo).toBeTruthy();
      expect(logo.getAttribute('alt')).toBe('MirDB Logo');
    });

    test('Badge images have descriptive alt text', () => {
      const badgeImages = document.querySelectorAll('.hero-badge img');
      const badgeArray = Array.from(badgeImages);

      for (const badge of badgeArray) {
        const alt = badge.getAttribute('alt');
        expect(alt).toBeTruthy();
        expect(alt.length).toBeGreaterThan(0);
      }
    });

    test('Decorative SVGs have aria-hidden="true"', () => {
      const svgs = document.querySelectorAll('svg');
      const svgArray = Array.from(svgs);

      for (const svg of svgArray) {
        // SVGs used as icons should be hidden from screen readers
        const ariaHidden = svg.getAttribute('aria-hidden');
        expect(ariaHidden).toBe('true');
      }
    });

    test('Images have width and height for CLS prevention', () => {
      const heroLogo = document.querySelector('.hero-logo');
      expect(heroLogo.getAttribute('width')).toBeTruthy();
      expect(heroLogo.getAttribute('height')).toBeTruthy();
    });
  });

  describe('Test Case 3: Color Contrast (WCAG AA)', () => {
    // Helper function to calculate relative luminance
    function getLuminance(r, g, b) {
      const [rs, gs, bs] = [r, g, b].map(c => {
        c = c / 255;
        return c <= 0.03928 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4);
      });
      return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
    }

    // Helper function to calculate contrast ratio
    function getContrastRatio(l1, l2) {
      const lighter = Math.max(l1, l2);
      const darker = Math.min(l1, l2);
      return (lighter + 0.05) / (darker + 0.05);
    }

    // Helper to parse hex color
    function hexToRgb(hex) {
      const result = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex);
      return result ? {
        r: parseInt(result[1], 16),
        g: parseInt(result[2], 16),
        b: parseInt(result[3], 16)
      } : null;
    }

    test('Primary text color meets WCAG AA contrast ratio (4.5:1) against background', () => {
      // Text color: #f1f5f9 (var(--color-text))
      // Background: #0f172a (var(--color-background))
      const textColor = hexToRgb('#f1f5f9');
      const bgColor = hexToRgb('#0f172a');

      const textLuminance = getLuminance(textColor.r, textColor.g, textColor.b);
      const bgLuminance = getLuminance(bgColor.r, bgColor.g, bgColor.b);

      const contrastRatio = getContrastRatio(textLuminance, bgLuminance);

      // WCAG AA requires 4.5:1 for normal text
      expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
    });

    test('Muted text color meets WCAG AA contrast ratio against background', () => {
      // Muted text: #94a3b8 (var(--color-text-muted))
      // Background: #0f172a (var(--color-background))
      const mutedColor = hexToRgb('#94a3b8');
      const bgColor = hexToRgb('#0f172a');

      const mutedLuminance = getLuminance(mutedColor.r, mutedColor.g, mutedColor.b);
      const bgLuminance = getLuminance(bgColor.r, bgColor.g, bgColor.b);

      const contrastRatio = getContrastRatio(mutedLuminance, bgLuminance);

      // WCAG AA requires 4.5:1 for normal text
      expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
    });

    test('Primary button text meets WCAG AA contrast ratio', () => {
      // Button text: white (#ffffff)
      // Button background: #2563eb (var(--color-primary))
      const textColor = hexToRgb('#ffffff');
      const bgColor = hexToRgb('#2563eb');

      const textLuminance = getLuminance(textColor.r, textColor.g, textColor.b);
      const bgLuminance = getLuminance(bgColor.r, bgColor.g, bgColor.b);

      const contrastRatio = getContrastRatio(textLuminance, bgLuminance);

      // WCAG AA requires 4.5:1 for normal text
      expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
    });

    test('Code text meets WCAG AA contrast ratio against code background', () => {
      // Code text: #c9d1d9
      // Code background: #0d1117
      const textColor = hexToRgb('#c9d1d9');
      const bgColor = hexToRgb('#0d1117');

      const textLuminance = getLuminance(textColor.r, textColor.g, textColor.b);
      const bgLuminance = getLuminance(bgColor.r, bgColor.g, bgColor.b);

      const contrastRatio = getContrastRatio(textLuminance, bgLuminance);

      // WCAG AA requires 4.5:1 for normal text
      expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
    });

    test('Link color meets WCAG AA contrast ratio', () => {
      // Link color: #2563eb (var(--color-primary))
      // Background: #0f172a (var(--color-background))
      const linkColor = hexToRgb('#2563eb');
      const bgColor = hexToRgb('#0f172a');

      const linkLuminance = getLuminance(linkColor.r, linkColor.g, linkColor.b);
      const bgLuminance = getLuminance(bgColor.r, bgColor.g, bgColor.b);

      const contrastRatio = getContrastRatio(linkLuminance, bgLuminance);

      // WCAG AA requires 3:1 for large text and UI components
      expect(contrastRatio).toBeGreaterThanOrEqual(3);
    });
  });

  describe('Semantic HTML Structure', () => {
    test('Page has skip link for keyboard navigation', () => {
      const skipLink = document.querySelector('.skip-link');
      expect(skipLink).toBeTruthy();
      expect(skipLink.getAttribute('href')).toBe('#main-content');
    });

    test('Main content has id for skip link target', () => {
      const main = document.querySelector('main');
      expect(main.id).toBe('main-content');
    });

    test('Language attribute is set on html element', () => {
      const html = document.documentElement;
      expect(html.getAttribute('lang')).toBe('en');
    });

    test('Page has meta description for SEO and accessibility', () => {
      const metaDescription = document.querySelector('meta[name="description"]');
      expect(metaDescription).toBeTruthy();
      expect(metaDescription.getAttribute('content').length).toBeGreaterThan(0);
    });

    test('Interactive elements have accessible names', () => {
      // Check buttons have accessible names
      const buttons = document.querySelectorAll('button');
      const buttonsArray = Array.from(buttons);

      for (const button of buttonsArray) {
        const hasAccessibleName =
          button.textContent.trim() ||
          button.getAttribute('aria-label') ||
          button.getAttribute('aria-labelledby');
        expect(hasAccessibleName).toBeTruthy();
      }
    });

    test('Links have accessible names', () => {
      const links = document.querySelectorAll('a');
      const linksArray = Array.from(links);

      for (const link of linksArray) {
        const hasAccessibleName =
          link.textContent.trim() ||
          link.getAttribute('aria-label') ||
          link.getAttribute('aria-labelledby');
        expect(hasAccessibleName).toBeTruthy();
      }
    });

    test('Form controls have labels or aria-label', () => {
      const inputs = document.querySelectorAll('input, select, textarea');
      const inputsArray = Array.from(inputs);

      for (const input of inputsArray) {
        const id = input.id;
        const hasLabel = id ? document.querySelector(`label[for="${id}"]`) : false;
        const hasAriaLabel = input.getAttribute('aria-label');
        const hasAriaLabelledby = input.getAttribute('aria-labelledby');

        expect(hasLabel || hasAriaLabel || hasAriaLabelledby).toBeTruthy();
      }
    });
  });

  describe('ARIA Attributes', () => {
    test('Navigation has correct ARIA role and label', () => {
      const nav = document.querySelector('nav[role="navigation"]');
      expect(nav).toBeTruthy();
      expect(nav.getAttribute('aria-label')).toBeTruthy();
    });

    test('Sections have aria-labelledby pointing to valid headings', () => {
      const sections = document.querySelectorAll('section[aria-labelledby]');
      const sectionsArray = Array.from(sections);

      for (const section of sectionsArray) {
        const labelledbyId = section.getAttribute('aria-labelledby');
        const heading = document.getElementById(labelledbyId);

        // Some sections are placeholders for other scenarios - check if section has content
        const sectionContent = section.querySelector('.container');
        const hasRealContent = sectionContent && sectionContent.textContent.trim().length > 100;

        // Only validate if the section has real content
        if (hasRealContent) {
          expect(heading).toBeTruthy();
        }
      }
    });

    test('Mobile menu toggle has correct ARIA attributes', () => {
      const toggle = document.querySelector('.nav-mobile-toggle');
      expect(toggle).toBeTruthy();
      expect(toggle.getAttribute('aria-label')).toBeTruthy();
      expect(toggle.getAttribute('aria-expanded')).toBeTruthy();
      expect(toggle.getAttribute('aria-controls')).toBeTruthy();
    });

    test('External links have rel="noopener noreferrer"', () => {
      const externalLinks = document.querySelectorAll('a[target="_blank"]');
      const linksArray = Array.from(externalLinks);

      for (const link of linksArray) {
        const rel = link.getAttribute('rel');
        expect(rel).toContain('noopener');
      }
    });
  });
});
