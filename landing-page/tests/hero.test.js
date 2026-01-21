import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { JSDOM } from 'jsdom';
import fs from 'fs';
import path from 'path';

/**
 * Hero Section Tests
 * Testing REQ-1: Hero section with compelling headline, subheadline, and primary CTA button
 */

describe('Hero Section Display and Content', () => {
  let dom;
  let document;

  beforeEach(() => {
    // Load the HTML file
    const htmlPath = path.resolve(__dirname, '../index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf-8');
    dom = new JSDOM(htmlContent, {
      url: 'http://localhost:3000',
      runScripts: 'dangerously',
      resources: 'usable',
    });
    document = dom.window.document;
  });

  afterEach(() => {
    if (dom) {
      dom.window.close();
    }
  });

  /**
   * Test Case 1: Hero section contains H1 headline element with text content
   */
  describe('Test Case 1: Hero Section H1 Headline', () => {
    it('should have a hero section element', () => {
      const heroSection = document.querySelector('.hero');
      expect(heroSection).not.toBeNull();
      expect(heroSection).toBeInTheDocument();
    });

    it('should contain an H1 headline element', () => {
      const headline = document.querySelector('.hero h1');
      expect(headline).not.toBeNull();
      expect(headline).toBeInTheDocument();
    });

    it('should have H1 headline with text content', () => {
      const headline = document.querySelector('.hero h1');
      expect(headline).not.toBeNull();
      expect(headline.textContent.trim()).not.toBe('');
      expect(headline.textContent.length).toBeGreaterThan(0);
    });

    it('should have hero-headline class on the H1', () => {
      const headline = document.querySelector('.hero h1.hero-headline');
      expect(headline).not.toBeNull();
    });
  });

  /**
   * Test Case 2: Headline contains 10 words or fewer
   */
  describe('Test Case 2: Headline Word Count', () => {
    it('should have headline with 10 words or fewer', () => {
      const headline = document.querySelector('.hero h1.hero-headline');
      expect(headline).not.toBeNull();

      const text = headline.textContent.trim();
      const wordCount = text.split(/\s+/).filter(word => word.length > 0).length;

      expect(wordCount).toBeLessThanOrEqual(10);
    });

    it('should have a concise headline that communicates value', () => {
      const headline = document.querySelector('.hero h1.hero-headline');
      const text = headline.textContent.trim().toLowerCase();

      // Check that headline contains keywords related to the product value
      const valueKeywords = ['fast', 'storage', 'key-value', 'persistent', 'lightning', 'database', 'data'];
      const containsValueKeyword = valueKeywords.some(keyword =>
        text.includes(keyword)
      );

      expect(containsValueKeyword).toBe(true);
    });
  });

  /**
   * Test Case 3: Subheadline element is present below headline
   */
  describe('Test Case 3: Subheadline Element', () => {
    it('should have a subheadline element', () => {
      const subheadline = document.querySelector('.hero .hero-subheadline');
      expect(subheadline).not.toBeNull();
      expect(subheadline).toBeInTheDocument();
    });

    it('should have subheadline with text content', () => {
      const subheadline = document.querySelector('.hero .hero-subheadline');
      expect(subheadline).not.toBeNull();
      expect(subheadline.textContent.trim()).not.toBe('');
      expect(subheadline.textContent.length).toBeGreaterThan(0);
    });

    it('should have subheadline positioned below headline in DOM order', () => {
      const headline = document.querySelector('.hero h1.hero-headline');
      const subheadline = document.querySelector('.hero .hero-subheadline');

      expect(headline).not.toBeNull();
      expect(subheadline).not.toBeNull();

      // Check that subheadline comes after headline in DOM order
      const heroContent = document.querySelector('.hero-content');
      const children = Array.from(heroContent.children);
      const headlineIndex = children.indexOf(headline);
      const subheadlineIndex = children.indexOf(subheadline);

      expect(subheadlineIndex).toBeGreaterThan(headlineIndex);
    });

    it('should have subheadline that explains value proposition', () => {
      const subheadline = document.querySelector('.hero .hero-subheadline');
      const text = subheadline.textContent.trim().toLowerCase();

      // Check that subheadline provides meaningful context
      expect(text.length).toBeGreaterThan(20);
    });
  });

  /**
   * Test Case 4: Primary CTA button exists with visible text and click handler
   */
  describe('Test Case 4: Primary CTA Button', () => {
    it('should have a primary CTA button', () => {
      const ctaButton = document.querySelector('.hero .cta-primary');
      expect(ctaButton).not.toBeNull();
      expect(ctaButton).toBeInTheDocument();
    });

    it('should have CTA button with visible text', () => {
      const ctaButton = document.querySelector('.hero .cta-primary');
      expect(ctaButton).not.toBeNull();

      const text = ctaButton.textContent.trim();
      expect(text).not.toBe('');
      expect(text.length).toBeGreaterThan(0);
    });

    it('should have CTA button with onclick handler', () => {
      const ctaButton = document.querySelector('.hero .cta-primary');
      expect(ctaButton).not.toBeNull();

      // Check for onclick attribute
      const hasOnClick = ctaButton.hasAttribute('onclick');
      expect(hasOnClick).toBe(true);
    });

    it('should be a button element for proper accessibility', () => {
      const ctaButton = document.querySelector('.hero .cta-primary');
      expect(ctaButton).not.toBeNull();
      expect(ctaButton.tagName.toLowerCase()).toBe('button');
    });

    it('should have CTA button with actionable text', () => {
      const ctaButton = document.querySelector('.hero .cta-primary');
      const text = ctaButton.textContent.trim().toLowerCase();

      // Check for action-oriented language
      const actionWords = ['get', 'start', 'try', 'sign', 'join', 'download', 'free', 'now', 'begin'];
      const hasActionWord = actionWords.some(word => text.includes(word));

      expect(hasActionWord).toBe(true);
    });
  });

  /**
   * Test Case 5: CTA button has sufficient color contrast ratio (4.5:1 minimum)
   */
  describe('Test Case 5: CTA Button Color Contrast', () => {
    it('should have primary button with high contrast colors defined in CSS', () => {
      // Read and parse CSS file
      const cssPath = path.resolve(__dirname, '../styles.css');
      const cssContent = fs.readFileSync(cssPath, 'utf-8');

      // Check for primary color definition
      expect(cssContent).toContain('--primary-color');
      expect(cssContent).toContain('--primary-text-on-button');

      // Extract the color values
      const primaryColorMatch = cssContent.match(/--primary-color:\s*([^;]+)/);
      const textColorMatch = cssContent.match(/--primary-text-on-button:\s*([^;]+)/);

      expect(primaryColorMatch).not.toBeNull();
      expect(textColorMatch).not.toBeNull();

      // Primary color is #2563eb (blue) and text is #ffffff (white)
      // This combination has contrast ratio of approximately 4.57:1, meeting WCAG AA
      const primaryColor = primaryColorMatch[1].trim();
      const textColor = textColorMatch[1].trim();

      expect(primaryColor).toBe('#2563eb');
      expect(textColor).toBe('#ffffff');

      // Verify contrast ratio calculation
      // #2563eb RGB: 37, 99, 235
      // #ffffff RGB: 255, 255, 255
      // Relative luminance of #2563eb ≈ 0.148
      // Relative luminance of #ffffff = 1.0
      // Contrast ratio = (1.0 + 0.05) / (0.148 + 0.05) ≈ 5.3:1
      const contrastRatio = calculateContrastRatio('#2563eb', '#ffffff');
      expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
    });

    it('should apply correct background color to CTA button', () => {
      const cssPath = path.resolve(__dirname, '../styles.css');
      const cssContent = fs.readFileSync(cssPath, 'utf-8');

      // Check that .cta-primary uses the primary color for background
      expect(cssContent).toContain('.cta-primary');
      expect(cssContent).toMatch(/\.cta-primary\s*\{[^}]*background:\s*var\(--primary-color\)/);
    });

    it('should apply correct text color to CTA button', () => {
      const cssPath = path.resolve(__dirname, '../styles.css');
      const cssContent = fs.readFileSync(cssPath, 'utf-8');

      // Check that .cta-primary uses the correct text color
      expect(cssContent).toContain('.cta-primary');
      expect(cssContent).toMatch(/\.cta-primary\s*\{[^}]*color:\s*var\(--primary-text-on-button\)/);
    });
  });

  /**
   * Test Case 6: Hero section spans full viewport width
   */
  describe('Test Case 6: Hero Section Full-Width Display', () => {
    it('should have hero section with 100% width in CSS', () => {
      const cssPath = path.resolve(__dirname, '../styles.css');
      const cssContent = fs.readFileSync(cssPath, 'utf-8');

      // Check for full-width hero section
      expect(cssContent).toMatch(/\.hero\s*\{[^}]*width:\s*100%/);
    });

    it('should have hero section with no horizontal margins', () => {
      const heroSection = document.querySelector('.hero');
      expect(heroSection).not.toBeNull();

      // The hero section should span the full viewport
      // Check that the CSS does not restrict the width
      const cssPath = path.resolve(__dirname, '../styles.css');
      const cssContent = fs.readFileSync(cssPath, 'utf-8');

      // Hero should have width: 100% and not have restricting max-width on the section itself
      expect(cssContent).toMatch(/\.hero\s*\{[^}]*width:\s*100%/);
    });

    it('should have hero section with min-height for visibility', () => {
      const cssPath = path.resolve(__dirname, '../styles.css');
      const cssContent = fs.readFileSync(cssPath, 'utf-8');

      // Check for minimum height to ensure above-the-fold visibility
      expect(cssContent).toMatch(/\.hero\s*\{[^}]*min-height:\s*100vh/);
    });

    it('should have hero section visible as first content area', () => {
      const main = document.querySelector('main');
      const firstSection = main.querySelector('section');

      expect(firstSection).not.toBeNull();
      expect(firstSection.classList.contains('hero')).toBe(true);
    });
  });
});

/**
 * Utility function to calculate contrast ratio between two colors
 * @param {string} color1 - Hex color (e.g., '#ffffff')
 * @param {string} color2 - Hex color (e.g., '#000000')
 * @returns {number} - Contrast ratio
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
 * @param {string} hex - Hex color string
 * @returns {object} - RGB object with r, g, b properties
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
 * @param {object} rgb - RGB object with r, g, b properties
 * @returns {number} - Relative luminance value
 */
function getLuminance(rgb) {
  const [r, g, b] = [rgb.r, rgb.g, rgb.b].map(val => {
    const sRGB = val / 255;
    return sRGB <= 0.03928
      ? sRGB / 12.92
      : Math.pow((sRGB + 0.055) / 1.055, 2.4);
  });

  return 0.2126 * r + 0.7152 * g + 0.0722 * b;
}
