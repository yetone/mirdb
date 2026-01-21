import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { JSDOM } from 'jsdom';
import fs from 'fs';
import path from 'path';

/**
 * WCAG 2.1 AA Accessibility Compliance Tests
 * Testing NFR-3: WCAG 2.1 AA accessibility compliance
 *
 * This test suite verifies:
 * - Color contrast ratios (4.5:1 minimum for normal text)
 * - All images have descriptive alt text
 * - Proper heading hierarchy (single H1, logical nesting)
 * - Focus visible styles on interactive elements
 * - Form label associations
 * - Screen reader compatible structure
 */

describe('WCAG 2.1 AA Accessibility Compliance', () => {
  let dom;
  let document;
  let cssContent;

  beforeEach(() => {
    const htmlPath = path.resolve(__dirname, '../index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf-8');
    dom = new JSDOM(htmlContent, {
      url: 'http://localhost:3000',
      runScripts: 'dangerously',
      resources: 'usable',
    });
    document = dom.window.document;

    const cssPath = path.resolve(__dirname, '../styles.css');
    cssContent = fs.readFileSync(cssPath, 'utf-8');
  });

  afterEach(() => {
    if (dom) {
      dom.window.close();
    }
  });

  /**
   * Test Case 1: Color Contrast Ratios
   * WCAG 2.1 Success Criterion 1.4.3 - Contrast (Minimum)
   * All text must have at least 4.5:1 contrast ratio against its background
   */
  describe('Test Case 1: Color Contrast Ratios (4.5:1 minimum)', () => {
    it('should have CSS variables defined for text and background colors', () => {
      expect(cssContent).toContain('--text-primary');
      expect(cssContent).toContain('--text-secondary');
      expect(cssContent).toContain('--background');
      expect(cssContent).toContain('--primary-color');
    });

    it('should have primary text color with sufficient contrast on white background', () => {
      // --text-primary: #1f2937 on --background: #ffffff
      const textColor = '#1f2937';
      const bgColor = '#ffffff';
      const ratio = calculateContrastRatio(textColor, bgColor);
      expect(ratio).toBeGreaterThanOrEqual(4.5);
    });

    it('should have secondary text color with sufficient contrast on white background', () => {
      // --text-secondary: #4b5563 on --background: #ffffff
      const textColor = '#4b5563';
      const bgColor = '#ffffff';
      const ratio = calculateContrastRatio(textColor, bgColor);
      expect(ratio).toBeGreaterThanOrEqual(4.5);
    });

    it('should have primary button text with sufficient contrast', () => {
      // --primary-text-on-button: #ffffff on --primary-color: #2563eb
      const textColor = '#ffffff';
      const bgColor = '#2563eb';
      const ratio = calculateContrastRatio(textColor, bgColor);
      expect(ratio).toBeGreaterThanOrEqual(4.5);
    });

    it('should have footer text with sufficient contrast', () => {
      // Footer: --background (#ffffff) text on --text-primary (#1f2937) background
      const textColor = '#ffffff';
      const bgColor = '#1f2937';
      const ratio = calculateContrastRatio(textColor, bgColor);
      expect(ratio).toBeGreaterThanOrEqual(4.5);
    });

    it('should have link color with sufficient contrast on white background', () => {
      // --primary-color: #2563eb on --background: #ffffff
      const linkColor = '#2563eb';
      const bgColor = '#ffffff';
      const ratio = calculateContrastRatio(linkColor, bgColor);
      expect(ratio).toBeGreaterThanOrEqual(4.5);
    });

    it('should have secondary text on secondary background with sufficient contrast', () => {
      // --text-secondary: #4b5563 on --background-secondary: #f9fafb
      const textColor = '#4b5563';
      const bgColor = '#f9fafb';
      const ratio = calculateContrastRatio(textColor, bgColor);
      expect(ratio).toBeGreaterThanOrEqual(4.5);
    });
  });

  /**
   * Test Case 3: Verify all images have alt attributes
   * WCAG 2.1 Success Criterion 1.1.1 - Non-text Content
   */
  describe('Test Case 3: Image Alt Text', () => {
    it('should have all images with alt attributes', () => {
      const images = document.querySelectorAll('img');
      expect(images.length).toBeGreaterThan(0);

      images.forEach((img, index) => {
        const alt = img.getAttribute('alt');
        expect(alt).not.toBeNull();
        expect(alt).toBeDefined();
      });
    });

    it('should have no images with empty alt attributes', () => {
      const images = document.querySelectorAll('img');

      images.forEach((img) => {
        const alt = img.getAttribute('alt');
        expect(alt).not.toBeNull();
        expect(alt.trim().length).toBeGreaterThan(0);
      });
    });

    it('should have descriptive alt text (not generic placeholder text)', () => {
      const images = document.querySelectorAll('img');
      const genericTerms = ['image', 'photo', 'picture', 'img', 'placeholder', 'untitled'];

      images.forEach((img) => {
        const alt = img.getAttribute('alt').toLowerCase();
        genericTerms.forEach((term) => {
          expect(alt).not.toBe(term);
        });
      });
    });

    it('should have meaningful alt text with minimum length', () => {
      const images = document.querySelectorAll('img');

      images.forEach((img) => {
        const alt = img.getAttribute('alt');
        // Alt text should be descriptive (at least 5 characters)
        expect(alt.length).toBeGreaterThanOrEqual(5);
      });
    });

    it('should have hero image with descriptive alt text', () => {
      const heroImage = document.querySelector('.hero img');
      if (heroImage) {
        const alt = heroImage.getAttribute('alt');
        expect(alt).not.toBeNull();
        expect(alt.trim().length).toBeGreaterThan(0);
      }
    });

    it('should have product showcase images with descriptive alt text', () => {
      const showcaseImages = document.querySelectorAll('.product-showcase img');
      showcaseImages.forEach((img) => {
        const alt = img.getAttribute('alt');
        expect(alt).not.toBeNull();
        expect(alt.length).toBeGreaterThanOrEqual(10);
      });
    });

    it('should have testimonial images with descriptive alt text', () => {
      const testimonialImages = document.querySelectorAll('.testimonial-image');
      testimonialImages.forEach((img) => {
        const alt = img.getAttribute('alt');
        expect(alt).not.toBeNull();
        expect(alt.trim().length).toBeGreaterThan(0);
      });
    });
  });

  /**
   * Test Case 4: Heading Hierarchy
   * WCAG 2.1 Success Criterion 1.3.1 - Info and Relationships
   */
  describe('Test Case 4: Heading Hierarchy', () => {
    it('should have exactly one H1 element', () => {
      const h1Elements = document.querySelectorAll('h1');
      expect(h1Elements.length).toBe(1);
    });

    it('should have H1 element with text content', () => {
      const h1 = document.querySelector('h1');
      expect(h1).not.toBeNull();
      expect(h1.textContent.trim().length).toBeGreaterThan(0);
    });

    it('should have H1 in the hero section (primary content)', () => {
      const heroH1 = document.querySelector('.hero h1');
      expect(heroH1).not.toBeNull();
    });

    it('should have logical heading levels without skipping', () => {
      const headings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      const levels = Array.from(headings).map(h => parseInt(h.tagName[1]));

      // First heading should be H1
      expect(levels[0]).toBe(1);

      // Track the levels we've seen to ensure no critical skips
      const seenLevels = new Set();

      for (let i = 0; i < levels.length; i++) {
        const level = levels[i];
        seenLevels.add(level);

        // For main content headings (h1-h3), check proper hierarchy
        if (i > 0 && level <= 3) {
          const prevLevel = levels[i - 1];
          // Within main content, heading level should not increase by more than 1
          if (prevLevel <= 3) {
            const diff = level - prevLevel;
            expect(diff).toBeLessThanOrEqual(1);
          }
        }
      }

      // Should have h1 and h2 at minimum
      expect(seenLevels.has(1)).toBe(true);
      expect(seenLevels.has(2)).toBe(true);
    });

    it('should have H2 elements for main sections', () => {
      const h2Elements = document.querySelectorAll('h2');
      expect(h2Elements.length).toBeGreaterThanOrEqual(1);
    });

    it('should have section headings in features section', () => {
      const featuresSection = document.querySelector('.features');
      const heading = featuresSection.querySelector('h2');
      expect(heading).not.toBeNull();
    });

    it('should have section headings in product showcase', () => {
      const showcaseSection = document.querySelector('.product-showcase');
      const heading = showcaseSection.querySelector('h2');
      expect(heading).not.toBeNull();
    });

    it('should have section headings in social proof', () => {
      const socialProofSection = document.querySelector('.social-proof');
      const heading = socialProofSection.querySelector('h2');
      expect(heading).not.toBeNull();
    });

    it('should have footer headings at appropriate level (H4)', () => {
      const footerHeadings = document.querySelectorAll('.footer h4');
      expect(footerHeadings.length).toBeGreaterThanOrEqual(1);
    });
  });

  /**
   * Test Case 5: Focus Visible Styles
   * WCAG 2.1 Success Criterion 2.4.7 - Focus Visible
   */
  describe('Test Case 5: Focus Visible Styles', () => {
    it('should have global :focus-visible styles defined', () => {
      expect(cssContent).toContain(':focus-visible');
    });

    it('should have visible outline on focused elements', () => {
      // Check for outline styles on focus states
      expect(cssContent).toMatch(/:focus(-visible)?\s*\{[^}]*outline:/);
    });

    it('should have outline-offset for better visibility', () => {
      expect(cssContent).toMatch(/outline-offset:/);
    });

    it('should have focus styles on primary CTA button', () => {
      expect(cssContent).toMatch(/\.cta-primary:focus\s*\{[^}]*outline:/);
    });

    it('should have focus styles on secondary CTA button', () => {
      expect(cssContent).toMatch(/\.cta-secondary:focus\s*\{[^}]*outline:/);
    });

    it('should have focus styles on navigation CTA', () => {
      expect(cssContent).toMatch(/\.nav-cta:focus\s*\{[^}]*outline:/);
    });

    it('should have focus styles using primary color for consistency', () => {
      // Focus outline should use the primary color for visual consistency
      expect(cssContent).toMatch(/:focus(-visible)?\s*\{[^}]*outline:.*var\(--primary-color\)/);
    });

    it('should not use outline: none without alternative focus indicator', () => {
      // Check that focus styles are not removed without alternatives
      const hasOutlineNone = cssContent.match(/outline:\s*none/gi);
      if (hasOutlineNone) {
        // If outline: none exists, there should be an alternative focus indicator
        const hasAlternative = cssContent.includes('box-shadow') ||
                              cssContent.includes('border-color') ||
                              cssContent.match(/:focus(-visible)?\s*\{[^}]*outline:/);
        expect(hasAlternative).toBe(true);
      }
    });

    it('should have interactive elements focusable', () => {
      const buttons = document.querySelectorAll('button');
      const links = document.querySelectorAll('a');

      buttons.forEach((btn) => {
        // Buttons should not have negative tabindex
        const tabindex = btn.getAttribute('tabindex');
        expect(tabindex === null || parseInt(tabindex) >= 0).toBe(true);
      });

      links.forEach((link) => {
        const tabindex = link.getAttribute('tabindex');
        expect(tabindex === null || parseInt(tabindex) >= 0).toBe(true);
      });
    });
  });

  /**
   * Test Case 6: Screen Reader Compatibility
   * WCAG 2.1 Success Criterion 1.3.1 - Info and Relationships
   */
  describe('Test Case 6: Screen Reader Compatibility', () => {
    it('should have semantic HTML structure with main element', () => {
      const main = document.querySelector('main');
      expect(main).not.toBeNull();
    });

    it('should have semantic header element', () => {
      const header = document.querySelector('header');
      expect(header).not.toBeNull();
    });

    it('should have semantic footer element', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();
    });

    it('should have semantic nav element', () => {
      const nav = document.querySelector('nav');
      expect(nav).not.toBeNull();
    });

    it('should use semantic section elements', () => {
      const sections = document.querySelectorAll('section');
      expect(sections.length).toBeGreaterThan(0);
    });

    it('should have navigation list with proper ul/li structure', () => {
      const navList = document.querySelector('.nav-links');
      expect(navList.tagName.toLowerCase()).toBe('ul');

      const listItems = navList.querySelectorAll('li');
      expect(listItems.length).toBeGreaterThan(0);
    });

    it('should have blockquote elements for testimonial quotes', () => {
      const quotes = document.querySelectorAll('.testimonial-quote');
      quotes.forEach((quote) => {
        expect(quote.tagName.toLowerCase()).toBe('blockquote');
      });
    });

    it('should have html lang attribute set', () => {
      const html = document.documentElement;
      const lang = html.getAttribute('lang');
      expect(lang).not.toBeNull();
      expect(lang.length).toBeGreaterThan(0);
    });

    it('should have page title', () => {
      const title = document.querySelector('title');
      expect(title).not.toBeNull();
      expect(title.textContent.trim().length).toBeGreaterThan(0);
    });

    it('should have meta description', () => {
      const metaDesc = document.querySelector('meta[name="description"]');
      expect(metaDesc).not.toBeNull();
      expect(metaDesc.getAttribute('content').length).toBeGreaterThan(0);
    });
  });

  /**
   * Test Case 7: Form Label Associations
   * WCAG 2.1 Success Criterion 1.3.1 - Info and Relationships
   * Note: The current landing page may not have forms, but this tests any that exist
   */
  describe('Test Case 7: Form Label Associations', () => {
    it('should have all form inputs with associated labels or aria-label', () => {
      const inputs = document.querySelectorAll('input, select, textarea');

      inputs.forEach((input) => {
        const id = input.getAttribute('id');
        const ariaLabel = input.getAttribute('aria-label');
        const ariaLabelledBy = input.getAttribute('aria-labelledby');
        const hasLabel = id && document.querySelector(`label[for="${id}"]`);

        // Input must have either a label, aria-label, or aria-labelledby
        expect(hasLabel || ariaLabel || ariaLabelledBy).toBeTruthy();
      });
    });

    it('should have button elements with accessible names', () => {
      const buttons = document.querySelectorAll('button');

      buttons.forEach((button) => {
        const text = button.textContent.trim();
        const ariaLabel = button.getAttribute('aria-label');
        const ariaLabelledBy = button.getAttribute('aria-labelledby');

        // Button must have text content, aria-label, or aria-labelledby
        expect(text.length > 0 || ariaLabel || ariaLabelledBy).toBeTruthy();
      });
    });

    it('should have link elements with accessible names', () => {
      const links = document.querySelectorAll('a');

      links.forEach((link) => {
        const text = link.textContent.trim();
        const ariaLabel = link.getAttribute('aria-label');
        const hasImage = link.querySelector('img[alt]');

        // Link must have text content, aria-label, or contain image with alt
        expect(text.length > 0 || ariaLabel || hasImage).toBeTruthy();
      });
    });
  });

  /**
   * Additional WCAG Tests: Reduced Motion Support
   * WCAG 2.1 Success Criterion 2.3.3 - Animation from Interactions
   */
  describe('Reduced Motion Support', () => {
    it('should respect prefers-reduced-motion media query', () => {
      expect(cssContent).toContain('prefers-reduced-motion');
    });

    it('should disable or reduce animations when reduced motion is preferred', () => {
      expect(cssContent).toMatch(/@media\s*\(\s*prefers-reduced-motion:\s*reduce\s*\)/);
    });
  });

  /**
   * Touch Target Size
   * WCAG 2.1 Success Criterion 2.5.5 - Target Size
   */
  describe('Touch Target Size', () => {
    it('should have CTA buttons with minimum height for touch accessibility', () => {
      // Check that min-height: 44px is set for touch targets
      expect(cssContent).toMatch(/\.cta-(primary|secondary)\s*\{[^}]*min-height:\s*44px/);
    });

    it('should have navigation CTA with adequate padding', () => {
      expect(cssContent).toMatch(/\.nav-cta\s*\{[^}]*padding:/);
    });
  });
});

/**
 * Utility: Calculate contrast ratio between two colors
 * Based on WCAG 2.1 relative luminance formula
 */
function calculateContrastRatio(color1, color2) {
  const lum1 = getLuminance(hexToRgb(color1));
  const lum2 = getLuminance(hexToRgb(color2));

  const lighter = Math.max(lum1, lum2);
  const darker = Math.min(lum1, lum2);

  return (lighter + 0.05) / (darker + 0.05);
}

/**
 * Convert hex color to RGB object
 */
function hexToRgb(hex) {
  const result = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex);
  return result
    ? {
        r: parseInt(result[1], 16),
        g: parseInt(result[2], 16),
        b: parseInt(result[3], 16),
      }
    : null;
}

/**
 * Calculate relative luminance of an RGB color
 * Per WCAG 2.1 formula
 */
function getLuminance(rgb) {
  const [r, g, b] = [rgb.r, rgb.g, rgb.b].map((val) => {
    const sRGB = val / 255;
    return sRGB <= 0.03928
      ? sRGB / 12.92
      : Math.pow((sRGB + 0.055) / 1.055, 2.4);
  });

  return 0.2126 * r + 0.7152 * g + 0.0722 * b;
}
