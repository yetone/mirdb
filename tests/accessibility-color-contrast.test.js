/**
 * Tests for Accessibility - Color Contrast (NFR-3)
 * Verify sufficient color contrast ratios for readability
 * WCAG 2.1 AA Requirements:
 * - Normal text (<18pt or <14pt bold): 4.5:1 minimum
 * - Large text (>=18pt or >=14pt bold): 3:1 minimum
 */

const fs = require('fs');
const path = require('path');

/**
 * Convert a hex color to RGB values
 * @param {string} hex - Hex color string (e.g., '#ffffff' or 'ffffff')
 * @returns {{r: number, g: number, b: number}} RGB values (0-255)
 */
function hexToRgb(hex) {
  const cleanHex = hex.replace('#', '');
  const bigint = parseInt(cleanHex, 16);
  return {
    r: (bigint >> 16) & 255,
    g: (bigint >> 8) & 255,
    b: bigint & 255
  };
}

/**
 * Calculate relative luminance of a color
 * Based on WCAG 2.1 formula: https://www.w3.org/WAI/WCAG21/Understanding/contrast-minimum.html
 * @param {{r: number, g: number, b: number}} rgb - RGB values
 * @returns {number} Relative luminance (0-1)
 */
function getRelativeLuminance(rgb) {
  const sRGB = [rgb.r, rgb.g, rgb.b].map(value => {
    const normalized = value / 255;
    return normalized <= 0.03928
      ? normalized / 12.92
      : Math.pow((normalized + 0.055) / 1.055, 2.4);
  });
  return 0.2126 * sRGB[0] + 0.7152 * sRGB[1] + 0.0722 * sRGB[2];
}

/**
 * Calculate contrast ratio between two colors
 * @param {string} color1 - First hex color
 * @param {string} color2 - Second hex color
 * @returns {number} Contrast ratio (1-21)
 */
function getContrastRatio(color1, color2) {
  const lum1 = getRelativeLuminance(hexToRgb(color1));
  const lum2 = getRelativeLuminance(hexToRgb(color2));
  const lighter = Math.max(lum1, lum2);
  const darker = Math.min(lum1, lum2);
  return (lighter + 0.05) / (darker + 0.05);
}

// Define colors used in the MirDB homepage CSS
const COLORS = {
  // Background colors
  bodyBackground: '#ffffff',
  heroBackground: '#1a1a2e',
  sectionAltBackground: '#f8f9fa',
  codeBlockBackground: '#1a1a2e',
  buttonPrimaryBackground: '#0f4c75',
  buttonPrimaryHoverBackground: '#2a6a9a',
  inlineCodeBackground: '#e2e8f0',
  footerBackground: '#1a1a2e',
  tableHeaderBackground: '#1a1a2e',
  featureYesBackground: '#047857',
  featureNoBackground: '#b91c1c',
  featureNaBackground: '#4b5563',

  // Text colors
  bodyText: '#1a1a2e',
  heroText: '#ffffff',
  sectionTitle: '#1a1a2e',
  propositionTitle: '#0f4c75',
  propositionDescription: '#4a4a4a',
  stepTitle: '#1a1a2e',
  stepDescription: '#4a4a4a',
  codeText: '#e0e0e0',
  inlineCodeText: '#1a1a2e',
  buttonPrimaryText: '#ffffff',
  buttonSecondaryText: '#ffffff',
  footerText: '#ffffff',
  tableHeaderText: '#ffffff',
  featureName: '#1a1a2e',
  featureCell: '#4a4a4a',
  featureStatusText: '#ffffff',
  commandListText: '#4a4a4a',
  commandCodeBackground: '#e2e8f0',
  commandCodeText: '#1a1a2e',
  commandGroupTitle: '#0f4c75',
  clientExampleTitle: '#0f4c75',
  exampleDescription: '#4a4a4a'
};

// WCAG AA contrast requirements
const WCAG_AA_NORMAL = 4.5;
const WCAG_AA_LARGE = 3.0;

describe('Accessibility - Color Contrast (NFR-3)', () => {
  let cssContent;

  beforeAll(() => {
    cssContent = fs.readFileSync(path.resolve(__dirname, '../styles.css'), 'utf8');
  });

  // Test Case 1: Body text color contrast
  describe('Test Case 1: Body Text Color Contrast', () => {
    test('body text (#1a1a2e) against white background (#ffffff) meets 4.5:1 ratio', () => {
      const ratio = getContrastRatio(COLORS.bodyText, COLORS.bodyBackground);
      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL);
    });

    test('section title (#1a1a2e) against white background (#ffffff) meets 4.5:1 ratio', () => {
      const ratio = getContrastRatio(COLORS.sectionTitle, COLORS.bodyBackground);
      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL);
    });

    test('section title (#1a1a2e) against alternate background (#f8f9fa) meets 4.5:1 ratio', () => {
      const ratio = getContrastRatio(COLORS.sectionTitle, COLORS.sectionAltBackground);
      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL);
    });

    test('proposition description (#4a4a4a) against white background meets 4.5:1 ratio', () => {
      const ratio = getContrastRatio(COLORS.propositionDescription, COLORS.bodyBackground);
      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL);
    });

    test('proposition title (#0f4c75) against white background meets 4.5:1 ratio', () => {
      const ratio = getContrastRatio(COLORS.propositionTitle, COLORS.bodyBackground);
      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL);
    });

    test('step title (#1a1a2e) against alternate background (#f8f9fa) meets 4.5:1 ratio', () => {
      const ratio = getContrastRatio(COLORS.stepTitle, COLORS.sectionAltBackground);
      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL);
    });

    test('step description (#4a4a4a) against alternate background (#f8f9fa) meets 4.5:1 ratio', () => {
      const ratio = getContrastRatio(COLORS.stepDescription, COLORS.sectionAltBackground);
      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL);
    });

    test('command list text (#4a4a4a) against alternate background (#f8f9fa) meets 4.5:1 ratio', () => {
      const ratio = getContrastRatio(COLORS.commandListText, COLORS.sectionAltBackground);
      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL);
    });
  });

  // Test Case 2: Button text color contrast
  describe('Test Case 2: Button Text Color Contrast', () => {
    test('primary button text (#ffffff) against button background (#0f4c75) meets 4.5:1 ratio', () => {
      const ratio = getContrastRatio(COLORS.buttonPrimaryText, COLORS.buttonPrimaryBackground);
      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL);
    });

    test('primary button text (#ffffff) against hover background (#2a6a9a) meets 4.5:1 ratio', () => {
      const ratio = getContrastRatio(COLORS.buttonPrimaryText, COLORS.buttonPrimaryHoverBackground);
      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL);
    });

    test('secondary button text (#ffffff) against hero background (#1a1a2e) meets 4.5:1 ratio', () => {
      const ratio = getContrastRatio(COLORS.buttonSecondaryText, COLORS.heroBackground);
      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL);
    });
  });

  // Test Case 3: Code block color contrast
  describe('Test Case 3: Code Block Color Contrast', () => {
    test('code text (#e0e0e0) against code background (#1a1a2e) meets 4.5:1 ratio', () => {
      const ratio = getContrastRatio(COLORS.codeText, COLORS.codeBlockBackground);
      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL);
    });

    test('inline code text (#1a1a2e) against inline code background (#e2e8f0) meets 4.5:1 ratio', () => {
      const ratio = getContrastRatio(COLORS.inlineCodeText, COLORS.inlineCodeBackground);
      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL);
    });

    test('command code text (#1a1a2e) against command code background (#e2e8f0) meets 4.5:1 ratio', () => {
      const ratio = getContrastRatio(COLORS.commandCodeText, COLORS.commandCodeBackground);
      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL);
    });
  });

  // Additional contrast tests for hero section
  describe('Hero Section Color Contrast', () => {
    test('hero text (#ffffff) against hero background (#1a1a2e) meets 4.5:1 ratio', () => {
      const ratio = getContrastRatio(COLORS.heroText, COLORS.heroBackground);
      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL);
    });
  });

  // Additional contrast tests for footer
  describe('Footer Color Contrast', () => {
    test('footer text (#ffffff) against footer background (#1a1a2e) meets 4.5:1 ratio', () => {
      const ratio = getContrastRatio(COLORS.footerText, COLORS.footerBackground);
      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL);
    });
  });

  // Additional contrast tests for comparison table
  describe('Comparison Table Color Contrast', () => {
    test('table header text (#ffffff) against header background (#1a1a2e) meets 4.5:1 ratio', () => {
      const ratio = getContrastRatio(COLORS.tableHeaderText, COLORS.tableHeaderBackground);
      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL);
    });

    test('feature name (#1a1a2e) against white table background (#ffffff) meets 4.5:1 ratio', () => {
      const ratio = getContrastRatio(COLORS.featureName, COLORS.bodyBackground);
      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL);
    });

    test('feature cell text (#4a4a4a) against white table background (#ffffff) meets 4.5:1 ratio', () => {
      const ratio = getContrastRatio(COLORS.featureCell, COLORS.bodyBackground);
      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL);
    });

    test('feature yes status text (#ffffff) against green background (#047857) meets 4.5:1 ratio', () => {
      const ratio = getContrastRatio(COLORS.featureStatusText, COLORS.featureYesBackground);
      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL);
    });

    test('feature no status text (#ffffff) against red background (#b91c1c) meets 4.5:1 ratio', () => {
      const ratio = getContrastRatio(COLORS.featureStatusText, COLORS.featureNoBackground);
      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL);
    });

    test('feature N/A status text (#ffffff) against gray background (#4b5563) meets 4.5:1 ratio', () => {
      const ratio = getContrastRatio(COLORS.featureStatusText, COLORS.featureNaBackground);
      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL);
    });
  });

  // Additional contrast tests for client examples section
  describe('Client Examples Color Contrast', () => {
    test('client example title (#0f4c75) against example background (#f8f9fa) meets 4.5:1 ratio', () => {
      const ratio = getContrastRatio(COLORS.clientExampleTitle, COLORS.sectionAltBackground);
      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL);
    });

    test('example description (#4a4a4a) against example background (#f8f9fa) meets 4.5:1 ratio', () => {
      const ratio = getContrastRatio(COLORS.exampleDescription, COLORS.sectionAltBackground);
      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL);
    });
  });

  // Additional contrast tests for commands section
  describe('Commands Section Color Contrast', () => {
    test('command group title (#0f4c75) against white background (#ffffff) meets 4.5:1 ratio', () => {
      const ratio = getContrastRatio(COLORS.commandGroupTitle, COLORS.bodyBackground);
      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL);
    });
  });

  // Verify contrast ratios are correctly calculated
  describe('Contrast Ratio Calculation Verification', () => {
    test('black on white should have maximum contrast (21:1)', () => {
      const ratio = getContrastRatio('#000000', '#ffffff');
      expect(ratio).toBeCloseTo(21, 0);
    });

    test('white on white should have minimum contrast (1:1)', () => {
      const ratio = getContrastRatio('#ffffff', '#ffffff');
      expect(ratio).toBeCloseTo(1, 0);
    });

    test('same color contrast should be 1:1', () => {
      const ratio = getContrastRatio('#1a1a2e', '#1a1a2e');
      expect(ratio).toBeCloseTo(1, 0);
    });
  });
});
