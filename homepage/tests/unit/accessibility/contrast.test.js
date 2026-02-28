/**
 * Color Contrast Unit Tests
 * Owner: Scenario 9 - Color Contrast Accessibility
 *
 * Tests:
 * - Body text contrast >= 4.5:1
 * - Heading contrast >= 3:1
 * - Button text contrast >= 4.5:1
 * - Focus indicator contrast >= 3:1
 *
 * WCAG 2.1 AA Requirements:
 * - Normal text (<18px or <14px bold): 4.5:1 minimum contrast ratio
 * - Large text (>=18px bold or >=24px): 3:1 minimum contrast ratio
 * - UI components and focus indicators: 3:1 minimum contrast ratio
 */

import { describe, it, expect, beforeAll } from 'vitest';
import fs from 'fs';
import path from 'path';

/**
 * Calculate relative luminance of a color
 * Based on WCAG 2.1 formula: https://www.w3.org/TR/WCAG21/#dfn-relative-luminance
 * @param {number} r - Red component (0-255)
 * @param {number} g - Green component (0-255)
 * @param {number} b - Blue component (0-255)
 * @returns {number} Relative luminance (0-1)
 */
function getRelativeLuminance(r, g, b) {
  const [rs, gs, bs] = [r, g, b].map(c => {
    const sRGB = c / 255;
    return sRGB <= 0.03928
      ? sRGB / 12.92
      : Math.pow((sRGB + 0.055) / 1.055, 2.4);
  });
  return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
}

/**
 * Calculate contrast ratio between two colors
 * Based on WCAG 2.1 formula: https://www.w3.org/TR/WCAG21/#dfn-contrast-ratio
 * @param {string} color1 - Hex color (e.g., '#ffffff')
 * @param {string} color2 - Hex color (e.g., '#000000')
 * @returns {number} Contrast ratio (1-21)
 */
function getContrastRatio(color1, color2) {
  const rgb1 = hexToRgb(color1);
  const rgb2 = hexToRgb(color2);

  const l1 = getRelativeLuminance(rgb1.r, rgb1.g, rgb1.b);
  const l2 = getRelativeLuminance(rgb2.r, rgb2.g, rgb2.b);

  const lighter = Math.max(l1, l2);
  const darker = Math.min(l1, l2);

  return (lighter + 0.05) / (darker + 0.05);
}

/**
 * Convert hex color to RGB object
 * @param {string} hex - Hex color (e.g., '#ffffff' or '#fff')
 * @returns {{r: number, g: number, b: number}} RGB object
 */
function hexToRgb(hex) {
  const cleanHex = hex.replace('#', '');
  const fullHex = cleanHex.length === 3
    ? cleanHex.split('').map(c => c + c).join('')
    : cleanHex;

  return {
    r: parseInt(fullHex.slice(0, 2), 16),
    g: parseInt(fullHex.slice(2, 4), 16),
    b: parseInt(fullHex.slice(4, 6), 16)
  };
}

/**
 * Parse CSS custom properties from variables.css (:root section only)
 * @returns {Object} Map of CSS variable names to their hex values
 */
function parseCSSVariables() {
  const cssPath = path.resolve(__dirname, '../../../src/styles/variables.css');
  const cssContent = fs.readFileSync(cssPath, 'utf-8');

  const variables = {};

  // Extract only the :root section to avoid dark theme overrides
  const rootMatch = cssContent.match(/:root\s*\{([^}]+)\}/);
  if (!rootMatch) {
    throw new Error('Could not find :root section in variables.css');
  }

  const rootContent = rootMatch[1];

  // Match CSS custom properties with hex color values
  const regex = /--([\w-]+):\s*(#[0-9a-fA-F]{3,6})/g;
  let match;

  while ((match = regex.exec(rootContent)) !== null) {
    variables[`--${match[1]}`] = match[2].toLowerCase();
  }

  return variables;
}

/**
 * Parse dark theme CSS custom properties
 * @returns {Object} Map of CSS variable names to their hex values for dark theme
 */
function parseDarkThemeVariables() {
  const cssPath = path.resolve(__dirname, '../../../src/styles/variables.css');
  const cssContent = fs.readFileSync(cssPath, 'utf-8');

  const variables = {};

  // Extract dark theme section
  const darkThemeMatch = cssContent.match(/\[data-theme="dark"\]\s*\{([^}]+)\}/);
  if (darkThemeMatch) {
    const darkContent = darkThemeMatch[1];
    const regex = /--([\w-]+):\s*(#[0-9a-fA-F]{3,6})/g;
    let match;

    while ((match = regex.exec(darkContent)) !== null) {
      variables[`--${match[1]}`] = match[2].toLowerCase();
    }
  }

  return variables;
}

// WCAG 2.1 AA minimum contrast ratios
const WCAG_AA_NORMAL_TEXT = 4.5; // For text < 18px or < 14px bold
const WCAG_AA_LARGE_TEXT = 3.0;  // For text >= 18px bold or >= 24px
const WCAG_AA_UI_COMPONENTS = 3.0; // For UI components and focus indicators

describe('Color Contrast Accessibility - WCAG 2.1 AA Compliance', () => {
  let lightThemeVars;
  let darkThemeVars;

  beforeAll(() => {
    lightThemeVars = parseCSSVariables();
    darkThemeVars = parseDarkThemeVariables();
  });

  describe('Light Theme', () => {
    describe('Test Case 1: Body Text Contrast', () => {
      it('should have body text color (#1e293b) with >= 4.5:1 contrast against white background', () => {
        const textColor = lightThemeVars['--color-text'];
        const bgColor = lightThemeVars['--color-background'];

        expect(textColor).toBeDefined();
        expect(bgColor).toBeDefined();

        const ratio = getContrastRatio(textColor, bgColor);

        expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
      });

      it('should have primary text color with >= 4.5:1 contrast against white background', () => {
        const textColor = lightThemeVars['--color-text-primary'];
        const bgColor = lightThemeVars['--color-background'];

        expect(textColor).toBeDefined();
        expect(bgColor).toBeDefined();

        const ratio = getContrastRatio(textColor, bgColor);

        expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
      });

      it('should have secondary text color with >= 4.5:1 contrast against white background', () => {
        const textColor = lightThemeVars['--color-text-secondary'];
        const bgColor = lightThemeVars['--color-background'];

        expect(textColor).toBeDefined();
        expect(bgColor).toBeDefined();

        const ratio = getContrastRatio(textColor, bgColor);

        expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
      });

      it('should have body text with >= 4.5:1 contrast against surface background', () => {
        const textColor = lightThemeVars['--color-text'];
        const bgColor = lightThemeVars['--color-surface'];

        expect(textColor).toBeDefined();
        expect(bgColor).toBeDefined();

        const ratio = getContrastRatio(textColor, bgColor);

        expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
      });
    });

    describe('Test Case 2: Heading Text Contrast', () => {
      it('should have primary text (used for headings) with >= 3:1 contrast against white background', () => {
        const headingColor = lightThemeVars['--color-text-primary'];
        const bgColor = lightThemeVars['--color-background'];

        expect(headingColor).toBeDefined();
        expect(bgColor).toBeDefined();

        const ratio = getContrastRatio(headingColor, bgColor);

        // Large text (headings) require 3:1 minimum
        expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_LARGE_TEXT);
      });

      it('should have heading text with >= 3:1 contrast against surface background', () => {
        const headingColor = lightThemeVars['--color-text-primary'];
        const bgColor = lightThemeVars['--color-surface'];

        expect(headingColor).toBeDefined();
        expect(bgColor).toBeDefined();

        const ratio = getContrastRatio(headingColor, bgColor);

        expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_LARGE_TEXT);
      });

      it('should have main text color with >= 3:1 contrast against surface hover', () => {
        const headingColor = lightThemeVars['--color-text'];
        const bgColor = lightThemeVars['--color-surface-hover'];

        expect(headingColor).toBeDefined();
        expect(bgColor).toBeDefined();

        const ratio = getContrastRatio(headingColor, bgColor);

        expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_LARGE_TEXT);
      });
    });

    describe('Test Case 3: CTA Button Text Contrast', () => {
      it('should have white text on primary button with >= 4.5:1 contrast', () => {
        const buttonTextColor = '#ffffff'; // White text on primary buttons
        const buttonBgColor = lightThemeVars['--color-primary'];

        expect(buttonBgColor).toBeDefined();

        const ratio = getContrastRatio(buttonTextColor, buttonBgColor);

        expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
      });

      it('should have white text on primary button hover with >= 4.5:1 contrast', () => {
        const buttonTextColor = '#ffffff';
        const buttonBgColor = lightThemeVars['--color-primary-hover'];

        expect(buttonBgColor).toBeDefined();

        const ratio = getContrastRatio(buttonTextColor, buttonBgColor);

        expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
      });

      it('should have secondary button text with >= 4.5:1 contrast against white background', () => {
        const buttonTextColor = lightThemeVars['--color-text'];
        const buttonBgColor = lightThemeVars['--color-background'];

        expect(buttonTextColor).toBeDefined();
        expect(buttonBgColor).toBeDefined();

        const ratio = getContrastRatio(buttonTextColor, buttonBgColor);

        expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
      });
    });

    describe('Test Case 4: Focus Indicator Contrast', () => {
      it('should have focus color with >= 3:1 contrast against white background', () => {
        const focusColor = lightThemeVars['--color-focus'];
        const bgColor = lightThemeVars['--color-background'];

        expect(focusColor).toBeDefined();
        expect(bgColor).toBeDefined();

        const ratio = getContrastRatio(focusColor, bgColor);

        expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_UI_COMPONENTS);
      });

      it('should have focus color with >= 3:1 contrast against surface background', () => {
        const focusColor = lightThemeVars['--color-focus'];
        const bgColor = lightThemeVars['--color-surface'];

        expect(focusColor).toBeDefined();
        expect(bgColor).toBeDefined();

        const ratio = getContrastRatio(focusColor, bgColor);

        expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_UI_COMPONENTS);
      });

      it('should have primary color (link focus) with >= 3:1 contrast against white background', () => {
        const linkColor = lightThemeVars['--color-primary'];
        const bgColor = lightThemeVars['--color-background'];

        expect(linkColor).toBeDefined();
        expect(bgColor).toBeDefined();

        const ratio = getContrastRatio(linkColor, bgColor);

        expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_UI_COMPONENTS);
      });
    });
  });

  describe('Dark Theme', () => {
    describe('Body Text Contrast', () => {
      it('should have body text with >= 4.5:1 contrast against dark background', () => {
        const textColor = darkThemeVars['--color-text'];
        const bgColor = darkThemeVars['--color-background'];

        expect(textColor).toBeDefined();
        expect(bgColor).toBeDefined();

        const ratio = getContrastRatio(textColor, bgColor);

        expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
      });

      it('should have primary text with >= 4.5:1 contrast against dark background', () => {
        const textColor = darkThemeVars['--color-text-primary'];
        const bgColor = darkThemeVars['--color-background'];

        expect(textColor).toBeDefined();
        expect(bgColor).toBeDefined();

        const ratio = getContrastRatio(textColor, bgColor);

        expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
      });

      it('should have secondary text with >= 4.5:1 contrast against dark background', () => {
        const textColor = darkThemeVars['--color-text-secondary'];
        const bgColor = darkThemeVars['--color-background'];

        expect(textColor).toBeDefined();
        expect(bgColor).toBeDefined();

        const ratio = getContrastRatio(textColor, bgColor);

        expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
      });
    });

    describe('Heading Text Contrast', () => {
      it('should have heading text with >= 3:1 contrast against dark surface', () => {
        const headingColor = darkThemeVars['--color-text-primary'];
        const bgColor = darkThemeVars['--color-surface'];

        expect(headingColor).toBeDefined();
        expect(bgColor).toBeDefined();

        const ratio = getContrastRatio(headingColor, bgColor);

        expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_LARGE_TEXT);
      });
    });

    describe('CTA Button Text Contrast', () => {
      it('should have white text on dark theme primary button with >= 4.5:1 contrast', () => {
        const buttonTextColor = '#ffffff';
        const buttonBgColor = darkThemeVars['--color-primary'];

        expect(buttonBgColor).toBeDefined();

        const ratio = getContrastRatio(buttonTextColor, buttonBgColor);

        expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
      });
    });

    describe('Focus Indicator Contrast', () => {
      it('should have focus color with >= 3:1 contrast against dark background', () => {
        const focusColor = darkThemeVars['--color-focus'];
        const bgColor = darkThemeVars['--color-background'];

        expect(focusColor).toBeDefined();
        expect(bgColor).toBeDefined();

        const ratio = getContrastRatio(focusColor, bgColor);

        expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_UI_COMPONENTS);
      });

      it('should have focus color with >= 3:1 contrast against dark surface', () => {
        const focusColor = darkThemeVars['--color-focus'];
        const bgColor = darkThemeVars['--color-surface'];

        expect(focusColor).toBeDefined();
        expect(bgColor).toBeDefined();

        const ratio = getContrastRatio(focusColor, bgColor);

        expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_UI_COMPONENTS);
      });
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

    it('should correctly calculate contrast ratio for gray on white (~4.5:1)', () => {
      // #767676 is the lightest gray that passes 4.5:1 on white
      const ratio = getContrastRatio('#767676', '#ffffff');
      expect(ratio).toBeGreaterThanOrEqual(4.5);
    });

    it('should return 1 for same colors', () => {
      const ratio = getContrastRatio('#ffffff', '#ffffff');
      expect(ratio).toBe(1);
    });
  });
});
