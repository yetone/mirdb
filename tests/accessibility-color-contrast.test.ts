import { describe, it, expect, beforeAll } from 'vitest';
import { JSDOM } from 'jsdom';
import * as fs from 'fs';
import * as path from 'path';

/**
 * Color contrast utility functions for WCAG 2.1 compliance testing
 * Based on WCAG 2.1 Level AA requirements:
 * - Normal text (< 18pt or < 14pt bold): 4.5:1 minimum contrast ratio
 * - Large text (>= 18pt or >= 14pt bold): 3:1 minimum contrast ratio
 */

/**
 * Parse a hex color string to RGB values
 */
function hexToRgb(hex: string): { r: number; g: number; b: number } {
  const result = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex);
  if (!result) {
    throw new Error(`Invalid hex color: ${hex}`);
  }
  return {
    r: parseInt(result[1], 16),
    g: parseInt(result[2], 16),
    b: parseInt(result[3], 16),
  };
}

/**
 * Calculate relative luminance of a color
 * Formula from WCAG 2.1: https://www.w3.org/WAI/GL/wiki/Relative_luminance
 */
function getRelativeLuminance(r: number, g: number, b: number): number {
  const [rs, gs, bs] = [r, g, b].map((c) => {
    const sRGB = c / 255;
    return sRGB <= 0.03928
      ? sRGB / 12.92
      : Math.pow((sRGB + 0.055) / 1.055, 2.4);
  });
  return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
}

/**
 * Calculate contrast ratio between two colors
 * Formula from WCAG 2.1: (L1 + 0.05) / (L2 + 0.05)
 * where L1 is the lighter color and L2 is the darker color
 */
function getContrastRatio(color1: string, color2: string): number {
  const rgb1 = hexToRgb(color1);
  const rgb2 = hexToRgb(color2);

  const l1 = getRelativeLuminance(rgb1.r, rgb1.g, rgb1.b);
  const l2 = getRelativeLuminance(rgb2.r, rgb2.g, rgb2.b);

  const lighter = Math.max(l1, l2);
  const darker = Math.min(l1, l2);

  return (lighter + 0.05) / (darker + 0.05);
}

/**
 * Check if contrast ratio meets WCAG 2.1 AA for normal text (4.5:1)
 */
function meetsNormalTextContrast(foreground: string, background: string): boolean {
  return getContrastRatio(foreground, background) >= 4.5;
}

/**
 * Check if contrast ratio meets WCAG 2.1 AA for large text (3:1)
 */
function meetsLargeTextContrast(foreground: string, background: string): boolean {
  return getContrastRatio(foreground, background) >= 3.0;
}

// CSS color variables extracted from styles.css
const cssColors = {
  primaryColor: '#2563eb',
  primaryDark: '#1d4ed8',
  secondaryColor: '#1e293b',
  textColor: '#334155',
  textLight: '#64748b',
  bgColor: '#ffffff',
  bgSecondary: '#f8fafc',
  bgDark: '#0f172a',
  borderColor: '#e2e8f0',
  codeBg: '#1e293b',
  successColor: '#22c55e',
  taglineColor: '#94a3b8',
  footerTextColor: '#94a3b8',
  white: '#ffffff',
};

describe('Accessibility - Color Contrast', () => {
  let document: Document;
  let cssContent: string;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '..', 'index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf-8');
    const dom = new JSDOM(htmlContent);
    document = dom.window.document;

    const cssPath = path.join(__dirname, '..', 'styles.css');
    cssContent = fs.readFileSync(cssPath, 'utf-8');
  });

  describe('Body Text Contrast (WCAG 2.1 AA - 4.5:1)', () => {
    it('should have body text (--text-color) meeting 4.5:1 contrast against white background', () => {
      const ratio = getContrastRatio(cssColors.textColor, cssColors.bgColor);

      expect(ratio).toBeGreaterThanOrEqual(4.5);
      expect(meetsNormalTextContrast(cssColors.textColor, cssColors.bgColor)).toBe(true);
    });

    it('should have body text (--text-color) meeting 4.5:1 contrast against secondary background', () => {
      const ratio = getContrastRatio(cssColors.textColor, cssColors.bgSecondary);

      expect(ratio).toBeGreaterThanOrEqual(4.5);
      expect(meetsNormalTextContrast(cssColors.textColor, cssColors.bgSecondary)).toBe(true);
    });

    it('should have light text (--text-light) meeting 4.5:1 contrast against white background', () => {
      const ratio = getContrastRatio(cssColors.textLight, cssColors.bgColor);

      expect(ratio).toBeGreaterThanOrEqual(4.5);
      expect(meetsNormalTextContrast(cssColors.textLight, cssColors.bgColor)).toBe(true);
    });

    it('should have light text (--text-light) meeting 4.5:1 contrast against secondary background', () => {
      const ratio = getContrastRatio(cssColors.textLight, cssColors.bgSecondary);

      // Note: This may fail if #64748b on #f8fafc doesn't meet 4.5:1
      // If so, we need to adjust the CSS
      expect(ratio).toBeGreaterThanOrEqual(4.5);
      expect(meetsNormalTextContrast(cssColors.textLight, cssColors.bgSecondary)).toBe(true);
    });

    it('should have nav link text meeting 4.5:1 contrast against navbar background', () => {
      const ratio = getContrastRatio(cssColors.textColor, cssColors.bgColor);

      expect(ratio).toBeGreaterThanOrEqual(4.5);
      expect(meetsNormalTextContrast(cssColors.textColor, cssColors.bgColor)).toBe(true);
    });

    it('should have feature card text meeting 4.5:1 contrast against card background', () => {
      // Feature card uses --text-light for paragraph text on white background
      const ratio = getContrastRatio(cssColors.textLight, cssColors.bgColor);

      expect(ratio).toBeGreaterThanOrEqual(4.5);
    });
  });

  describe('Heading Text Contrast (WCAG 2.1 AA - 3:1 for large text)', () => {
    it('should have hero heading (white text) meeting 3:1 contrast against dark gradient background', () => {
      // Hero uses linear gradient from --bg-dark (#0f172a) to --secondary-color (#1e293b)
      // Both endpoints need to be checked
      const ratioDark = getContrastRatio(cssColors.white, cssColors.bgDark);
      const ratioSecondary = getContrastRatio(cssColors.white, cssColors.secondaryColor);

      expect(ratioDark).toBeGreaterThanOrEqual(3.0);
      expect(ratioSecondary).toBeGreaterThanOrEqual(3.0);
      expect(meetsLargeTextContrast(cssColors.white, cssColors.bgDark)).toBe(true);
      expect(meetsLargeTextContrast(cssColors.white, cssColors.secondaryColor)).toBe(true);
    });

    it('should have section headings (h2) meeting 3:1 contrast against backgrounds', () => {
      // Section headings use --secondary-color on white or secondary background
      const ratioWhite = getContrastRatio(cssColors.secondaryColor, cssColors.bgColor);
      const ratioSecondary = getContrastRatio(cssColors.secondaryColor, cssColors.bgSecondary);

      expect(ratioWhite).toBeGreaterThanOrEqual(3.0);
      expect(ratioSecondary).toBeGreaterThanOrEqual(3.0);
    });

    it('should have feature card headings (h3) meeting 3:1 contrast against card background', () => {
      // Feature card headings use --secondary-color on white background
      const ratio = getContrastRatio(cssColors.secondaryColor, cssColors.bgColor);

      expect(ratio).toBeGreaterThanOrEqual(3.0);
      expect(meetsLargeTextContrast(cssColors.secondaryColor, cssColors.bgColor)).toBe(true);
    });

    it('should have getting started headings (h3) meeting 3:1 contrast against white background', () => {
      const ratio = getContrastRatio(cssColors.secondaryColor, cssColors.bgColor);

      expect(ratio).toBeGreaterThanOrEqual(3.0);
    });

    it('should have hero tagline text meeting contrast requirements for large text', () => {
      // Tagline uses #94a3b8 on dark gradient background
      const ratioDark = getContrastRatio(cssColors.taglineColor, cssColors.bgDark);
      const ratioSecondary = getContrastRatio(cssColors.taglineColor, cssColors.secondaryColor);

      // Tagline is 1.25rem (20px) which qualifies as large text (>= 18pt)
      expect(ratioDark).toBeGreaterThanOrEqual(3.0);
      expect(ratioSecondary).toBeGreaterThanOrEqual(3.0);
    });
  });

  describe('Button and Interactive Element Contrast', () => {
    it('should have primary button text (white) meeting 4.5:1 contrast against primary color', () => {
      const ratio = getContrastRatio(cssColors.white, cssColors.primaryColor);

      expect(ratio).toBeGreaterThanOrEqual(4.5);
      expect(meetsNormalTextContrast(cssColors.white, cssColors.primaryColor)).toBe(true);
    });

    it('should have primary button text meeting 4.5:1 contrast against hover state', () => {
      const ratio = getContrastRatio(cssColors.white, cssColors.primaryDark);

      expect(ratio).toBeGreaterThanOrEqual(4.5);
    });

    it('should have secondary button text (white) visible against dark background', () => {
      // Secondary button has white text, needs to be visible on dark hero background
      const ratioDark = getContrastRatio(cssColors.white, cssColors.bgDark);

      expect(ratioDark).toBeGreaterThanOrEqual(4.5);
    });

    it('should have nav link hover state meeting 4.5:1 contrast', () => {
      // Hover state uses --primary-color on white background
      const ratio = getContrastRatio(cssColors.primaryColor, cssColors.bgColor);

      expect(ratio).toBeGreaterThanOrEqual(4.5);
    });

    it('should have footer links meeting 4.5:1 contrast against dark background', () => {
      // Footer links use #94a3b8 on --bg-dark (#0f172a)
      const ratio = getContrastRatio(cssColors.footerTextColor, cssColors.bgDark);

      expect(ratio).toBeGreaterThanOrEqual(4.5);
    });

    it('should have command code elements meeting 4.5:1 contrast', () => {
      // Command codes use --primary-color on white background
      const ratio = getContrastRatio(cssColors.primaryColor, cssColors.bgColor);

      expect(ratio).toBeGreaterThanOrEqual(4.5);
    });

    it('should have code blocks text meeting contrast requirements', () => {
      // Code blocks use light text on --code-bg (#1e293b)
      // Assuming code text is white or light colored
      const ratio = getContrastRatio(cssColors.white, cssColors.codeBg);

      expect(ratio).toBeGreaterThanOrEqual(4.5);
    });
  });

  describe('Contrast Ratio Calculation Verification', () => {
    it('should correctly calculate contrast ratio for black on white (21:1)', () => {
      const ratio = getContrastRatio('#000000', '#ffffff');

      expect(ratio).toBeCloseTo(21, 0);
    });

    it('should correctly calculate contrast ratio for white on black (21:1)', () => {
      const ratio = getContrastRatio('#ffffff', '#000000');

      expect(ratio).toBeCloseTo(21, 0);
    });

    it('should correctly calculate contrast ratio for same colors (1:1)', () => {
      const ratio = getContrastRatio('#ffffff', '#ffffff');

      expect(ratio).toBeCloseTo(1, 0);
    });

    it('should correctly identify insufficient contrast', () => {
      // Light gray on white should fail normal text contrast
      const ratio = getContrastRatio('#cccccc', '#ffffff');

      expect(ratio).toBeLessThan(4.5);
      expect(meetsNormalTextContrast('#cccccc', '#ffffff')).toBe(false);
    });
  });

  describe('CSS Variable Verification', () => {
    it('should have --text-color defined in CSS', () => {
      expect(cssContent).toContain('--text-color:');
    });

    it('should have --text-light defined in CSS', () => {
      expect(cssContent).toContain('--text-light:');
    });

    it('should have --bg-color defined in CSS', () => {
      expect(cssContent).toContain('--bg-color:');
    });

    it('should have --bg-secondary defined in CSS', () => {
      expect(cssContent).toContain('--bg-secondary:');
    });

    it('should have --primary-color defined in CSS', () => {
      expect(cssContent).toContain('--primary-color:');
    });

    it('should have --secondary-color defined in CSS', () => {
      expect(cssContent).toContain('--secondary-color:');
    });
  });

  describe('Real Color Values from DOM', () => {
    it('should have text color variable set to a value meeting contrast requirements', () => {
      // Verify the actual hex value in the CSS matches what we're testing
      const textColorMatch = cssContent.match(/--text-color:\s*(#[a-fA-F0-9]{6})/);

      expect(textColorMatch).not.toBeNull();
      if (textColorMatch) {
        const actualColor = textColorMatch[1].toLowerCase();
        expect(actualColor).toBe(cssColors.textColor.toLowerCase());
      }
    });

    it('should have background color variable set correctly', () => {
      const bgColorMatch = cssContent.match(/--bg-color:\s*(#[a-fA-F0-9]{6})/);

      expect(bgColorMatch).not.toBeNull();
      if (bgColorMatch) {
        const actualColor = bgColorMatch[1].toLowerCase();
        expect(actualColor).toBe(cssColors.bgColor.toLowerCase());
      }
    });
  });
});
