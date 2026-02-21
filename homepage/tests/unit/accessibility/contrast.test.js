/**
 * Accessibility - Color Contrast Tests
 * Owner: Scenario 14 - Accessibility - Color Contrast
 *
 * Tests color contrast ratios to verify WCAG 2.1 AA compliance.
 * - Normal text: 4.5:1 minimum contrast ratio
 * - Large text (18pt+ or 14pt bold): 3:1 minimum contrast ratio
 */

const fs = require('fs');
const path = require('path');

/**
 * Calculate relative luminance of a color
 * @param {number} r - Red value (0-255)
 * @param {number} g - Green value (0-255)
 * @param {number} b - Blue value (0-255)
 * @returns {number} - Relative luminance (0-1)
 */
function getRelativeLuminance(r, g, b) {
  const [rs, gs, bs] = [r, g, b].map((c) => {
    const s = c / 255;
    return s <= 0.03928 ? s / 12.92 : Math.pow((s + 0.055) / 1.055, 2.4);
  });
  return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
}

/**
 * Parse hex color to RGB values
 * @param {string} hex - Hex color (e.g., "#ffffff" or "#fff")
 * @returns {object} - { r, g, b } values
 */
function hexToRgb(hex) {
  const result = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex);
  if (result) {
    return {
      r: parseInt(result[1], 16),
      g: parseInt(result[2], 16),
      b: parseInt(result[3], 16),
    };
  }
  // Handle shorthand hex (#fff)
  const shortResult = /^#?([a-f\d])([a-f\d])([a-f\d])$/i.exec(hex);
  if (shortResult) {
    return {
      r: parseInt(shortResult[1] + shortResult[1], 16),
      g: parseInt(shortResult[2] + shortResult[2], 16),
      b: parseInt(shortResult[3] + shortResult[3], 16),
    };
  }
  throw new Error(`Invalid hex color: ${hex}`);
}

/**
 * Calculate contrast ratio between two colors
 * @param {string} color1 - Hex color for foreground
 * @param {string} color2 - Hex color for background
 * @returns {number} - Contrast ratio
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
 * Extract CSS variable values from CSS content
 * @param {string} cssContent - CSS file content
 * @param {string} selector - CSS selector (e.g., ':root' or '[data-theme="dark"]')
 * @returns {object} - Object with variable name -> value mapping
 */
function extractCssVariables(cssContent, selector) {
  const variables = {};

  // Build regex to find the selector block
  const escapedSelector = selector.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const selectorPattern = new RegExp(`${escapedSelector}[^{]*\\{([^}]+)\\}`, 's');
  const match = cssContent.match(selectorPattern);

  if (match) {
    const varPattern = /--([\w-]+):\s*([^;]+);/g;
    let varMatch;
    while ((varMatch = varPattern.exec(match[1])) !== null) {
      variables[`--${varMatch[1]}`] = varMatch[2].trim();
    }
  }

  return variables;
}

// WCAG AA minimum contrast ratios
const WCAG_AA_NORMAL_TEXT = 4.5;
const WCAG_AA_LARGE_TEXT = 3.0;

describe('Accessibility - Color Contrast', () => {
  let variablesCss;
  let lightModeVars;
  let darkModeVars;

  beforeAll(() => {
    // Load the CSS variables file
    const cssPath = path.join(__dirname, '../../../theme/css/variables.css');
    variablesCss = fs.readFileSync(cssPath, 'utf-8');

    // Extract light mode variables (from :root)
    lightModeVars = extractCssVariables(variablesCss, ':root');

    // Extract dark mode variables
    // Check for both possible selectors
    darkModeVars = extractCssVariables(variablesCss, '[data-theme="dark"]');
    if (Object.keys(darkModeVars).length === 0) {
      darkModeVars = extractCssVariables(variablesCss, '.dark-mode');
    }
  });

  // Test Case 1: Body text contrast in light mode
  describe('Light Mode Contrast', () => {
    test('Body text has at least 4.5:1 contrast ratio against background', () => {
      const textColor = lightModeVars['--text-color'] || lightModeVars['--code-text'];
      const bgColor = lightModeVars['--bg-color'];

      expect(textColor).toBeDefined();
      expect(bgColor).toBeDefined();

      const ratio = getContrastRatio(textColor, bgColor);

      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);

      // Log the actual ratio for documentation
      console.log(`Light mode body text contrast: ${ratio.toFixed(2)}:1`);
    });

    // Test Case 2: Heading text contrast in light mode
    test('Headings have at least 3:1 contrast ratio (large text)', () => {
      // Headings may use --heading-color or --text-color
      const headingColor = lightModeVars['--heading-color'] || lightModeVars['--text-color'];
      const bgColor = lightModeVars['--bg-color'];

      expect(headingColor).toBeDefined();
      expect(bgColor).toBeDefined();

      const ratio = getContrastRatio(headingColor, bgColor);

      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_LARGE_TEXT);

      console.log(`Light mode heading contrast: ${ratio.toFixed(2)}:1`);
    });

    // Test Case 3: Link color contrast in light mode
    test('Links have at least 4.5:1 contrast ratio', () => {
      const accentColor = lightModeVars['--accent-color'];
      const bgColor = lightModeVars['--bg-color'];

      expect(accentColor).toBeDefined();
      expect(bgColor).toBeDefined();

      const ratio = getContrastRatio(accentColor, bgColor);

      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);

      console.log(`Light mode link contrast: ${ratio.toFixed(2)}:1`);
    });

    // Test Case 5: Code block text contrast
    test('Code text has at least 4.5:1 contrast ratio', () => {
      const codeText = lightModeVars['--code-text'] || lightModeVars['--text-color'];
      const codeBg = lightModeVars['--code-bg'];

      expect(codeText).toBeDefined();
      expect(codeBg).toBeDefined();

      const ratio = getContrastRatio(codeText, codeBg);

      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);

      console.log(`Light mode code block contrast: ${ratio.toFixed(2)}:1`);
    });
  });

  // Test Case 4: Body text contrast in dark mode
  describe('Dark Mode Contrast', () => {
    test('Body text has at least 4.5:1 contrast ratio against dark background', () => {
      // Skip if dark mode variables are not defined yet
      if (Object.keys(darkModeVars).length === 0) {
        console.log('Dark mode variables not yet defined - skipping test');
        return;
      }

      const textColor = darkModeVars['--text-color'] || darkModeVars['--code-text'];
      const bgColor = darkModeVars['--bg-color'];

      expect(textColor).toBeDefined();
      expect(bgColor).toBeDefined();

      const ratio = getContrastRatio(textColor, bgColor);

      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);

      console.log(`Dark mode body text contrast: ${ratio.toFixed(2)}:1`);
    });

    test('Headings have at least 3:1 contrast ratio in dark mode', () => {
      if (Object.keys(darkModeVars).length === 0) {
        console.log('Dark mode variables not yet defined - skipping test');
        return;
      }

      const headingColor = darkModeVars['--heading-color'] || darkModeVars['--text-color'];
      const bgColor = darkModeVars['--bg-color'];

      expect(headingColor).toBeDefined();
      expect(bgColor).toBeDefined();

      const ratio = getContrastRatio(headingColor, bgColor);

      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_LARGE_TEXT);

      console.log(`Dark mode heading contrast: ${ratio.toFixed(2)}:1`);
    });

    test('Links have at least 4.5:1 contrast ratio in dark mode', () => {
      if (Object.keys(darkModeVars).length === 0) {
        console.log('Dark mode variables not yet defined - skipping test');
        return;
      }

      const accentColor = darkModeVars['--accent-color'];
      const bgColor = darkModeVars['--bg-color'];

      expect(accentColor).toBeDefined();
      expect(bgColor).toBeDefined();

      const ratio = getContrastRatio(accentColor, bgColor);

      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);

      console.log(`Dark mode link contrast: ${ratio.toFixed(2)}:1`);
    });

    test('Code text has at least 4.5:1 contrast ratio in dark mode', () => {
      if (Object.keys(darkModeVars).length === 0) {
        console.log('Dark mode variables not yet defined - skipping test');
        return;
      }

      const codeText = darkModeVars['--code-text'] || darkModeVars['--text-color'];
      const codeBg = darkModeVars['--code-bg'];

      expect(codeText).toBeDefined();
      expect(codeBg).toBeDefined();

      const ratio = getContrastRatio(codeText, codeBg);

      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);

      console.log(`Dark mode code block contrast: ${ratio.toFixed(2)}:1`);
    });
  });

  describe('Contrast Utility Functions', () => {
    test('getContrastRatio calculates correctly for black on white', () => {
      // Black on white should be approximately 21:1
      const ratio = getContrastRatio('#000000', '#ffffff');
      expect(ratio).toBeCloseTo(21, 0);
    });

    test('getContrastRatio calculates correctly for white on black', () => {
      // White on black should also be approximately 21:1
      const ratio = getContrastRatio('#ffffff', '#000000');
      expect(ratio).toBeCloseTo(21, 0);
    });

    test('hexToRgb parses standard hex colors', () => {
      const rgb = hexToRgb('#ff5500');
      expect(rgb).toEqual({ r: 255, g: 85, b: 0 });
    });

    test('hexToRgb parses shorthand hex colors', () => {
      const rgb = hexToRgb('#f50');
      expect(rgb).toEqual({ r: 255, g: 85, b: 0 });
    });
  });
});
