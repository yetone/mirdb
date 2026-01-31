/**
 * Accessibility Unit Tests
 * Owner: Scenario 12 - Accessibility Compliance
 *
 * Tests:
 * - Alt text on images
 * - ARIA attributes
 * - Color contrast values
 * - Focus indicators
 *
 * Requirements: NFR-3 (WCAG 2.1 AA)
 */

const fs = require('fs');
const path = require('path');
const { JSDOM } = require('jsdom');

/**
 * Calculate relative luminance of an RGB color
 * Based on WCAG 2.1 formula: https://www.w3.org/TR/WCAG21/#dfn-relative-luminance
 */
function getLuminance(r, g, b) {
  const [rs, gs, bs] = [r, g, b].map(c => {
    c = c / 255;
    return c <= 0.03928 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4);
  });
  return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
}

/**
 * Calculate contrast ratio between two colors
 * Returns a ratio between 1 and 21
 */
function getContrastRatio(color1, color2) {
  const l1 = getLuminance(color1.r, color1.g, color1.b);
  const l2 = getLuminance(color2.r, color2.g, color2.b);
  const lighter = Math.max(l1, l2);
  const darker = Math.min(l1, l2);
  return (lighter + 0.05) / (darker + 0.05);
}

/**
 * Parse hex color to RGB
 */
function hexToRgb(hex) {
  const result = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex);
  return result ? {
    r: parseInt(result[1], 16),
    g: parseInt(result[2], 16),
    b: parseInt(result[3], 16)
  } : null;
}

describe('Accessibility Tests', () => {
  let htmlContent;
  let cssContent;
  let document;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../index.html');
    const cssPath = path.join(__dirname, '../../css/styles.css');
    htmlContent = fs.readFileSync(htmlPath, 'utf-8');
    cssContent = fs.readFileSync(cssPath, 'utf-8');

    const dom = new JSDOM(htmlContent);
    document = dom.window.document;
  });

  describe('Architecture Diagram Accessibility (Test Case 5)', () => {
    test('Architecture diagram has appropriate alt text via aria-label', () => {
      // Check that the architecture diagram container has aria-label
      const hasAriaLabel = htmlContent.includes('class="architecture-diagram"') &&
        htmlContent.includes('role="img"') &&
        htmlContent.includes('aria-label=');

      expect(hasAriaLabel).toBe(true);
    });

    test('Architecture diagram aria-label describes LSM tree structure', () => {
      // Extract the aria-label content
      const ariaLabelMatch = htmlContent.match(/class="architecture-diagram"[^>]*aria-label="([^"]*)"/);

      expect(ariaLabelMatch).not.toBeNull();

      if (ariaLabelMatch) {
        const ariaLabelContent = ariaLabelMatch[1].toLowerCase();
        // Check that the aria-label mentions key components
        expect(ariaLabelContent).toContain('lsm');
        expect(ariaLabelContent).toContain('write');
        expect(ariaLabelContent).toContain('read');
      }
    });

    test('Mermaid diagram has aria-hidden for screen readers', () => {
      // The mermaid pre element should have aria-hidden="true"
      // since we provide a separate accessible description
      const hasMermaidAriaHidden = htmlContent.includes('class="mermaid"') &&
        htmlContent.includes('aria-hidden="true"');

      expect(hasMermaidAriaHidden).toBe(true);
    });

    test('Screen reader only text is provided for diagram description', () => {
      // Check for sr-only class with description
      const hasSrOnlyDescription = htmlContent.includes('class="sr-only"') &&
        htmlContent.includes('The diagram shows');

      expect(hasSrOnlyDescription).toBe(true);
    });
  });

  describe('Test Case 3: Color Contrast Compliance', () => {
    // PRD Color Palette:
    // - Text: #333333 (dark gray)
    // - Background: #FFFFFF (white) and #F5F5F5 (light gray)
    // WCAG 2.1 AA requires 4.5:1 for normal text, 3:1 for large text

    test('Body text (#333333) against white background (#FFFFFF) has at least 4.5:1 contrast', () => {
      const textColor = hexToRgb('#333333');
      const bgColor = hexToRgb('#FFFFFF');
      const contrastRatio = getContrastRatio(textColor, bgColor);

      expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
    });

    test('Body text (#333333) against light gray background (#F5F5F5) has at least 4.5:1 contrast', () => {
      const textColor = hexToRgb('#333333');
      const bgColor = hexToRgb('#F5F5F5');
      const contrastRatio = getContrastRatio(textColor, bgColor);

      expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
    });

    test('Accent color (#008080) against white background has at least 4.5:1 contrast', () => {
      const accentColor = hexToRgb('#008080');
      const bgColor = hexToRgb('#FFFFFF');
      const contrastRatio = getContrastRatio(accentColor, bgColor);

      // Teal #008080 has a contrast ratio of ~4.9:1 against white
      expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
    });

    test('Inverse text (#FFFFFF) against dark secondary (#2D2D2D) has at least 4.5:1 contrast', () => {
      const textColor = hexToRgb('#FFFFFF');
      const bgColor = hexToRgb('#2D2D2D');
      const contrastRatio = getContrastRatio(textColor, bgColor);

      expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
    });

    test('CSS defines proper color variables for accessibility', () => {
      // Check that CSS contains the expected color variables
      expect(cssContent).toContain('--color-text: #333333');
      expect(cssContent).toContain('--color-background: #FFFFFF');
      expect(cssContent).toContain('--color-background-alt: #F5F5F5');
      expect(cssContent).toContain('--color-accent: #008080');
    });

    test('Primary button text has sufficient contrast', () => {
      // Primary button: background #DEA584, text #2D2D2D
      const textColor = hexToRgb('#2D2D2D');
      const bgColor = hexToRgb('#DEA584');
      const contrastRatio = getContrastRatio(textColor, bgColor);

      // Should meet at least 4.5:1 for normal text or 3:1 for large text (buttons often use larger text)
      expect(contrastRatio).toBeGreaterThanOrEqual(3);
    });
  });

  describe('Test Case 4: Image Alt Text', () => {
    test('All img elements have alt attributes', () => {
      const images = document.querySelectorAll('img');

      images.forEach(img => {
        expect(img.hasAttribute('alt')).toBe(true);
      });
    });

    test('SVG elements used as images have appropriate accessibility attributes', () => {
      // SVGs used as images should have role="img" and aria-label or aria-hidden
      const svgs = document.querySelectorAll('svg');

      svgs.forEach(svg => {
        const parentRole = svg.parentElement?.getAttribute('role');
        const hasAriaHidden = svg.getAttribute('aria-hidden') === 'true';
        const parentHasAriaLabel = svg.parentElement?.getAttribute('aria-label');

        // SVG should either be aria-hidden or have accessible labeling
        expect(hasAriaHidden || parentRole === 'img' || parentHasAriaLabel).toBe(true);
      });
    });

    test('Decorative icons have aria-hidden="true"', () => {
      const featureIcons = document.querySelectorAll('.feature-icon');

      featureIcons.forEach(icon => {
        const isAriaHidden = icon.getAttribute('aria-hidden') === 'true';
        expect(isAriaHidden).toBe(true);
      });
    });

    test('Hero logo has proper role and aria-label', () => {
      const heroLogo = document.querySelector('.hero-logo');
      expect(heroLogo).not.toBeNull();
      expect(heroLogo.getAttribute('role')).toBe('img');
      expect(heroLogo.getAttribute('aria-label')).not.toBeNull();
      expect(heroLogo.getAttribute('aria-label').length).toBeGreaterThan(0);
    });

    test('Footer decorative icon has aria-hidden', () => {
      const footerLogo = document.querySelector('.footer-logo');
      if (footerLogo) {
        expect(footerLogo.getAttribute('aria-hidden')).toBe('true');
      }
    });
  });

  describe('Skip Link Accessibility (Test Case 5)', () => {
    test('Skip link to main content exists', () => {
      const skipLink = document.querySelector('a[href="#main-content"], a.skip-link');
      // Skip link should exist or we need to add it
      expect(skipLink !== null || htmlContent.includes('skip-link')).toBe(true);
    });
  });

  describe('Focus Indicators (Test Case 6)', () => {
    test('CSS includes focus-visible styles', () => {
      expect(cssContent).toContain(':focus-visible');
    });

    test('Focus styles include outline', () => {
      expect(cssContent).toContain('outline');
    });

    test('Focus styles include outline-offset for better visibility', () => {
      expect(cssContent).toContain('outline-offset');
    });

    test('Buttons have minimum touch target size (44px)', () => {
      // Check that buttons have min-height and min-width of 44px
      expect(cssContent).toContain('min-height: 44px');
      expect(cssContent).toContain('min-width: 44px');
    });
  });

  describe('Screen Reader Support', () => {
    test('sr-only class is defined in CSS', () => {
      expect(cssContent).toContain('.sr-only');
    });

    test('sr-only class hides content visually but keeps it accessible', () => {
      // Standard sr-only class properties
      expect(cssContent).toContain('position: absolute');
      expect(cssContent).toContain('width: 1px');
      expect(cssContent).toContain('height: 1px');
      expect(cssContent).toContain('overflow: hidden');
    });
  });

  describe('Keyboard Navigation Support', () => {
    test('Interactive elements are natively focusable or have tabindex', () => {
      const links = document.querySelectorAll('a[href]');
      const buttons = document.querySelectorAll('button');

      // All links and buttons should be inherently focusable
      expect(links.length).toBeGreaterThan(0);
      expect(buttons.length).toBeGreaterThan(0);
    });

    test('Hamburger menu button has aria-expanded attribute', () => {
      const hamburger = document.querySelector('.hamburger-menu');
      if (hamburger) {
        expect(hamburger.hasAttribute('aria-expanded')).toBe(true);
      }
    });

    test('Navigation links are in logical order', () => {
      const navLinks = document.querySelectorAll('nav[aria-label="Main navigation"] a');
      expect(navLinks.length).toBeGreaterThan(0);

      // Links should follow the page flow (Features -> Architecture -> Quick Start -> etc.)
      const linkTexts = Array.from(navLinks).map(link => link.textContent.trim());
      const expectedOrder = ['Features', 'Architecture', 'Quick Start'];

      expectedOrder.forEach((expected, index) => {
        if (index < linkTexts.length) {
          expect(linkTexts[index]).toContain(expected);
        }
      });
    });
  });
});
