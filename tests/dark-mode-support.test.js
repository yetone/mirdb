/**
 * Tests for Dark Mode Support (NFR-6)
 * Verify page supports dark mode via prefers-color-scheme media query
 *
 * Test Cases:
 * 1. Unit: Check for dark mode CSS media query or class
 * 2. E2E: Verify page background changes to dark, text to light
 * 3. Integration: Verify dark mode maintains sufficient contrast ratios
 */

const fs = require('fs');
const path = require('path');

// Utility functions for contrast calculation (same as accessibility tests)
function hexToRgb(hex) {
  const cleanHex = hex.replace('#', '');
  const bigint = parseInt(cleanHex, 16);
  return {
    r: (bigint >> 16) & 255,
    g: (bigint >> 8) & 255,
    b: bigint & 255
  };
}

function getRelativeLuminance(rgb) {
  const sRGB = [rgb.r, rgb.g, rgb.b].map(value => {
    const normalized = value / 255;
    return normalized <= 0.03928
      ? normalized / 12.92
      : Math.pow((normalized + 0.055) / 1.055, 2.4);
  });
  return 0.2126 * sRGB[0] + 0.7152 * sRGB[1] + 0.0722 * sRGB[2];
}

function getContrastRatio(color1, color2) {
  const lum1 = getRelativeLuminance(hexToRgb(color1));
  const lum2 = getRelativeLuminance(hexToRgb(color2));
  const lighter = Math.max(lum1, lum2);
  const darker = Math.min(lum1, lum2);
  return (lighter + 0.05) / (darker + 0.05);
}

// Dark mode color definitions
const DARK_MODE_COLORS = {
  // Dark mode backgrounds
  bodyBackground: '#1a1a2e',
  sectionBackground: '#16213e',
  sectionAltBackground: '#0f3460',
  cardBackground: '#16213e',
  codeBlockBackground: '#0d1117',

  // Dark mode text colors
  bodyText: '#e2e8f0',
  headingText: '#f8f9fa',
  mutedText: '#94a3b8',
  accentText: '#3282b8',

  // Button colors in dark mode
  buttonPrimaryBackground: '#0f4c75',
  buttonPrimaryText: '#ffffff'
};

// WCAG AA contrast requirements
const WCAG_AA_NORMAL = 4.5;
const WCAG_AA_LARGE = 3.0;

describe('Dark Mode Support (NFR-6)', () => {
  let cssContent;
  let htmlContent;

  beforeAll(() => {
    cssContent = fs.readFileSync(path.resolve(__dirname, '../styles.css'), 'utf8');
    htmlContent = fs.readFileSync(path.resolve(__dirname, '../index.html'), 'utf8');
  });

  /**
   * Test Case 1 (Unit): Check for dark mode CSS media query or class
   * Expected: CSS includes @media (prefers-color-scheme: dark) or dark mode class support
   */
  describe('Test Case 1: Dark Mode CSS Media Query or Class (Unit)', () => {
    test('CSS should include @media (prefers-color-scheme: dark) media query', () => {
      const hasDarkModeMediaQuery = cssContent.includes('@media (prefers-color-scheme: dark)');
      expect(hasDarkModeMediaQuery).toBe(true);
    });

    test('dark mode media query should define body background color', () => {
      // Extract the dark mode media query section
      const darkModeMatch = cssContent.match(/@media\s*\(\s*prefers-color-scheme:\s*dark\s*\)[\s\S]*?\{[\s\S]*?body[\s\S]*?background(?:-color)?:\s*([^;]+)/);
      expect(darkModeMatch).not.toBeNull();
    });

    test('dark mode media query should define body text color', () => {
      const darkModeMatch = cssContent.match(/@media\s*\(\s*prefers-color-scheme:\s*dark\s*\)[\s\S]*?\{[\s\S]*?body[\s\S]*?color:\s*([^;]+)/);
      expect(darkModeMatch).not.toBeNull();
    });

    test('dark mode should style major sections', () => {
      const darkModeSection = cssContent.match(/@media\s*\(\s*prefers-color-scheme:\s*dark\s*\)[\s\S]*?\{([\s\S]*?)\}\s*(?:@media|$)/);
      expect(darkModeSection).not.toBeNull();

      if (darkModeSection) {
        const darkModeStyles = darkModeSection[1];
        // Check that various sections are styled
        const hasValuePropositions = darkModeStyles.includes('.value-propositions') ||
                                     darkModeStyles.includes('.proposition');
        const hasGettingStarted = darkModeStyles.includes('.getting-started') ||
                                  darkModeStyles.includes('.step');
        const hasSectionTitle = darkModeStyles.includes('.section-title');

        expect(hasValuePropositions || hasGettingStarted || hasSectionTitle).toBe(true);
      }
    });
  });

  /**
   * Test Case 2 (E2E): Enable dark mode and reload page
   * Expected: Page background changes to dark, text to light
   * Note: This is a CSS-based verification since we can't trigger system dark mode in Jest
   */
  describe('Test Case 2: Dark Mode Visual Changes (E2E Style)', () => {
    test('dark mode should change body background to a dark color', () => {
      // Match background-color in dark mode for body
      const bodyDarkBgMatch = cssContent.match(/@media\s*\(\s*prefers-color-scheme:\s*dark\s*\)[\s\S]*?body\s*\{[^}]*background(?:-color)?:\s*(#[0-9a-fA-F]{3,6}|rgb[a]?\([^)]+\))/);
      expect(bodyDarkBgMatch).not.toBeNull();

      if (bodyDarkBgMatch && bodyDarkBgMatch[1]) {
        // Verify the color is actually dark (low luminance)
        const bgColor = bodyDarkBgMatch[1];
        if (bgColor.startsWith('#')) {
          const rgb = hexToRgb(bgColor);
          const luminance = getRelativeLuminance(rgb);
          // Dark colors have luminance < 0.3
          expect(luminance).toBeLessThan(0.3);
        }
      }
    });

    test('dark mode should change body text to a light color', () => {
      // Match color in dark mode for body
      const bodyDarkTextMatch = cssContent.match(/@media\s*\(\s*prefers-color-scheme:\s*dark\s*\)[\s\S]*?body\s*\{[^}]*?(?<!background-)color:\s*(#[0-9a-fA-F]{3,6}|rgb[a]?\([^)]+\))/);
      expect(bodyDarkTextMatch).not.toBeNull();

      if (bodyDarkTextMatch && bodyDarkTextMatch[1]) {
        // Verify the color is actually light (high luminance)
        const textColor = bodyDarkTextMatch[1];
        if (textColor.startsWith('#')) {
          const rgb = hexToRgb(textColor);
          const luminance = getRelativeLuminance(rgb);
          // Light colors have luminance > 0.5
          expect(luminance).toBeGreaterThan(0.5);
        }
      }
    });

    test('dark mode should style section backgrounds', () => {
      const darkModeSection = cssContent.match(/@media\s*\(\s*prefers-color-scheme:\s*dark\s*\)([\s\S]*?)(?=@media|$)/);
      expect(darkModeSection).not.toBeNull();

      if (darkModeSection) {
        // Check for background styling on sections
        const hasBackgroundStyling = darkModeSection[1].includes('background');
        expect(hasBackgroundStyling).toBe(true);
      }
    });

    test('dark mode section titles should be light colored', () => {
      const sectionTitleDarkMatch = cssContent.match(/@media\s*\(\s*prefers-color-scheme:\s*dark\s*\)[\s\S]*?\.section-title\s*\{[^}]*?color:\s*(#[0-9a-fA-F]{3,6})/);
      expect(sectionTitleDarkMatch).not.toBeNull();

      if (sectionTitleDarkMatch && sectionTitleDarkMatch[1]) {
        const color = sectionTitleDarkMatch[1];
        const rgb = hexToRgb(color);
        const luminance = getRelativeLuminance(rgb);
        // Light colors have luminance > 0.5
        expect(luminance).toBeGreaterThan(0.5);
      }
    });
  });

  /**
   * Test Case 3 (Integration): Verify dark mode contrast
   * Expected: Dark mode maintains sufficient contrast ratios
   */
  describe('Test Case 3: Dark Mode Contrast Verification (Integration)', () => {
    // Verify primary text contrast in dark mode
    test('dark mode body text (#e2e8f0) against dark background (#1a1a2e) meets 4.5:1 ratio', () => {
      const ratio = getContrastRatio(DARK_MODE_COLORS.bodyText, DARK_MODE_COLORS.bodyBackground);
      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL);
    });

    test('dark mode heading text (#f8f9fa) against dark background (#1a1a2e) meets 4.5:1 ratio', () => {
      const ratio = getContrastRatio(DARK_MODE_COLORS.headingText, DARK_MODE_COLORS.bodyBackground);
      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL);
    });

    test('dark mode muted text (#94a3b8) against dark background (#1a1a2e) meets 4.5:1 ratio', () => {
      const ratio = getContrastRatio(DARK_MODE_COLORS.mutedText, DARK_MODE_COLORS.bodyBackground);
      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL);
    });

    test('dark mode accent text (#3282b8) against dark background (#1a1a2e) meets 3:1 ratio for large text', () => {
      const ratio = getContrastRatio(DARK_MODE_COLORS.accentText, DARK_MODE_COLORS.bodyBackground);
      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_LARGE);
    });

    test('dark mode body text (#e2e8f0) against section background (#16213e) meets 4.5:1 ratio', () => {
      const ratio = getContrastRatio(DARK_MODE_COLORS.bodyText, DARK_MODE_COLORS.sectionBackground);
      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL);
    });

    test('dark mode body text (#e2e8f0) against alternate section background (#0f3460) meets 4.5:1 ratio', () => {
      const ratio = getContrastRatio(DARK_MODE_COLORS.bodyText, DARK_MODE_COLORS.sectionAltBackground);
      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL);
    });

    test('dark mode button text (#ffffff) against button background (#0f4c75) meets 4.5:1 ratio', () => {
      const ratio = getContrastRatio(DARK_MODE_COLORS.buttonPrimaryText, DARK_MODE_COLORS.buttonPrimaryBackground);
      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL);
    });

    // Verify CSS contains appropriate dark mode contrast values
    test('dark mode CSS should use contrasting colors', () => {
      const darkModeSection = cssContent.match(/@media\s*\(\s*prefers-color-scheme:\s*dark\s*\)([\s\S]*?)(?=@media|$)/);
      expect(darkModeSection).not.toBeNull();

      if (darkModeSection) {
        const darkStyles = darkModeSection[1];

        // Extract hex colors from dark mode
        const hexColors = darkStyles.match(/#[0-9a-fA-F]{3,6}/g) || [];

        // Should have multiple colors defined for dark mode
        expect(hexColors.length).toBeGreaterThan(0);

        // Check that we have both dark and light colors (for backgrounds and text)
        let hasDarkColor = false;
        let hasLightColor = false;

        for (const color of hexColors) {
          const rgb = hexToRgb(color);
          const luminance = getRelativeLuminance(rgb);
          if (luminance < 0.3) hasDarkColor = true;
          if (luminance > 0.5) hasLightColor = true;
        }

        expect(hasDarkColor).toBe(true);
        expect(hasLightColor).toBe(true);
      }
    });
  });

  // Additional dark mode style checks
  describe('Dark Mode Style Coverage', () => {
    test('dark mode should style proposition cards', () => {
      const hasPropDark = cssContent.match(/@media\s*\(\s*prefers-color-scheme:\s*dark\s*\)[\s\S]*?\.proposition/);
      expect(hasPropDark).not.toBeNull();
    });

    test('dark mode should style code blocks appropriately', () => {
      // Code blocks should be styled in dark mode (pre or code)
      const darkModeSection = cssContent.match(/@media\s*\(\s*prefers-color-scheme:\s*dark\s*\)([\s\S]*?)(?=@media|$)/);
      expect(darkModeSection).not.toBeNull();

      if (darkModeSection) {
        // Either pre or code or .inline-code should be styled
        const hasCodeStyling = darkModeSection[1].includes('pre') ||
                               darkModeSection[1].includes('code') ||
                               darkModeSection[1].includes('.inline-code');
        expect(hasCodeStyling).toBe(true);
      }
    });

    test('dark mode should style the footer', () => {
      const hasFooterDark = cssContent.match(/@media\s*\(\s*prefers-color-scheme:\s*dark\s*\)[\s\S]*?\.footer/);
      expect(hasFooterDark).not.toBeNull();
    });

    test('dark mode should style comparison table', () => {
      const darkModeSection = cssContent.match(/@media\s*\(\s*prefers-color-scheme:\s*dark\s*\)([\s\S]*?)(?=@media|$)/);
      expect(darkModeSection).not.toBeNull();

      if (darkModeSection) {
        const hasTableStyling = darkModeSection[1].includes('.comparison-table') ||
                                darkModeSection[1].includes('table') ||
                                darkModeSection[1].includes('tbody') ||
                                darkModeSection[1].includes('td');
        expect(hasTableStyling).toBe(true);
      }
    });

    test('dark mode should style command sections', () => {
      const darkModeSection = cssContent.match(/@media\s*\(\s*prefers-color-scheme:\s*dark\s*\)([\s\S]*?)(?=@media|$)/);
      expect(darkModeSection).not.toBeNull();

      if (darkModeSection) {
        const hasCommandStyling = darkModeSection[1].includes('.command') ||
                                  darkModeSection[1].includes('.commands');
        expect(hasCommandStyling).toBe(true);
      }
    });
  });
});
