/**
 * CSS Variables Unit Tests
 * Owner: Scenario 14 - Dark Mode Support
 *
 * Tests:
 * - All required CSS variables defined
 * - Light mode values
 * - Dark mode values
 * - Contrast ratios meet AA standards
 */

import { readFileSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Helper function to parse CSS file
function parseCSSFile(filePath) {
  const cssContent = readFileSync(filePath, 'utf-8');
  return cssContent;
}

// Helper function to extract CSS variables from content
function extractCSSVariables(cssContent, selector = ':root') {
  const variables = {};
  // Match the selector block
  const selectorRegex = new RegExp(`${selector.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\s*\\{([^}]+)\\}`, 'g');
  let match;

  while ((match = selectorRegex.exec(cssContent)) !== null) {
    const block = match[1];
    // Extract variable declarations
    const varRegex = /--([\w-]+)\s*:\s*([^;]+);/g;
    let varMatch;
    while ((varMatch = varRegex.exec(block)) !== null) {
      variables[`--${varMatch[1]}`] = varMatch[2].trim();
    }
  }

  return variables;
}

// Helper function to convert hex to RGB
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

// Helper function to calculate relative luminance
function getLuminance(r, g, b) {
  const [rs, gs, bs] = [r, g, b].map((c) => {
    c = c / 255;
    return c <= 0.03928 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4);
  });
  return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
}

// Helper function to calculate contrast ratio
function getContrastRatio(color1, color2) {
  const rgb1 = hexToRgb(color1);
  const rgb2 = hexToRgb(color2);

  if (!rgb1 || !rgb2) return null;

  const l1 = getLuminance(rgb1.r, rgb1.g, rgb1.b);
  const l2 = getLuminance(rgb2.r, rgb2.g, rgb2.b);

  const lighter = Math.max(l1, l2);
  const darker = Math.min(l1, l2);

  return (lighter + 0.05) / (darker + 0.05);
}

describe('CSS Variables', () => {
  const variablesPath = join(__dirname, '../../css/utilities/variables.css');
  let cssContent;

  beforeAll(() => {
    cssContent = parseCSSFile(variablesPath);
  });

  describe('Required CSS Variables', () => {
    test('should define all required color variables in :root', () => {
      const rootVariables = extractCSSVariables(cssContent, ':root');

      // Required color variables
      const requiredColors = [
        '--color-primary',
        '--color-background',
        '--color-surface',
        '--color-text',
        '--color-text-secondary',
        '--color-border',
      ];

      requiredColors.forEach((varName) => {
        expect(rootVariables[varName]).toBeDefined();
        expect(rootVariables[varName]).not.toBe('');
      });
    });

    test('should define code block color variables', () => {
      const rootVariables = extractCSSVariables(cssContent, ':root');

      const codeColors = [
        '--color-code-bg',
        '--color-code-text',
        '--color-code-keyword',
        '--color-code-string',
        '--color-code-comment',
      ];

      codeColors.forEach((varName) => {
        expect(rootVariables[varName]).toBeDefined();
        expect(rootVariables[varName]).not.toBe('');
      });
    });

    test('should define font variables', () => {
      const rootVariables = extractCSSVariables(cssContent, ':root');

      expect(rootVariables['--font-sans']).toBeDefined();
      expect(rootVariables['--font-mono']).toBeDefined();
    });
  });

  describe('Light Mode Values', () => {
    test('should have light background color in :root', () => {
      const rootVariables = extractCSSVariables(cssContent, ':root');

      // Light mode background should be white or very light
      const bgColor = rootVariables['--color-background'];
      expect(bgColor).toBeDefined();

      // Should be #ffffff or similar light color
      if (bgColor.startsWith('#')) {
        const rgb = hexToRgb(bgColor);
        if (rgb) {
          // All RGB values should be high (light color)
          expect(rgb.r).toBeGreaterThan(240);
          expect(rgb.g).toBeGreaterThan(240);
          expect(rgb.b).toBeGreaterThan(240);
        }
      }
    });

    test('should have dark text color in :root for light mode', () => {
      const rootVariables = extractCSSVariables(cssContent, ':root');

      const textColor = rootVariables['--color-text'];
      expect(textColor).toBeDefined();

      // Text should be dark for readability
      if (textColor.startsWith('#')) {
        const rgb = hexToRgb(textColor);
        if (rgb) {
          // At least one RGB value should be low (dark color)
          expect(rgb.r + rgb.g + rgb.b).toBeLessThan(400);
        }
      }
    });
  });

  describe('Dark Mode Values', () => {
    test('should define dark mode variables in [data-theme="dark"] selector', () => {
      // Check for dark mode styles
      expect(cssContent).toMatch(/\[data-theme="dark"\]/);
    });

    test('should override background color in dark mode', () => {
      // Extract dark mode variables
      const darkModeMatch = cssContent.match(/\[data-theme="dark"\]\s*\{([^}]+)\}/);
      expect(darkModeMatch).toBeTruthy();

      if (darkModeMatch) {
        const darkBlock = darkModeMatch[1];
        expect(darkBlock).toMatch(/--color-background/);
      }
    });

    test('should override text color in dark mode', () => {
      const darkModeMatch = cssContent.match(/\[data-theme="dark"\]\s*\{([^}]+)\}/);
      expect(darkModeMatch).toBeTruthy();

      if (darkModeMatch) {
        const darkBlock = darkModeMatch[1];
        expect(darkBlock).toMatch(/--color-text/);
      }
    });

    test('should have prefers-color-scheme media query for system preference', () => {
      // Check for prefers-color-scheme media query
      expect(cssContent).toMatch(/@media\s*\(prefers-color-scheme:\s*dark\)/);
    });
  });

  describe('Contrast Ratios', () => {
    test('should have sufficient contrast between text and background in light mode', () => {
      const rootVariables = extractCSSVariables(cssContent, ':root');

      const textColor = rootVariables['--color-text'];
      const bgColor = rootVariables['--color-background'];

      if (textColor && bgColor && textColor.startsWith('#') && bgColor.startsWith('#')) {
        const ratio = getContrastRatio(textColor, bgColor);
        // WCAG AA requires at least 4.5:1 for normal text
        expect(ratio).toBeGreaterThanOrEqual(4.5);
      }
    });

    test('should have sufficient contrast between secondary text and background in light mode', () => {
      const rootVariables = extractCSSVariables(cssContent, ':root');

      const secondaryTextColor = rootVariables['--color-text-secondary'];
      const bgColor = rootVariables['--color-background'];

      if (
        secondaryTextColor &&
        bgColor &&
        secondaryTextColor.startsWith('#') &&
        bgColor.startsWith('#')
      ) {
        const ratio = getContrastRatio(secondaryTextColor, bgColor);
        // Should be at least 4.5:1
        expect(ratio).toBeGreaterThanOrEqual(4.5);
      }
    });

    test('dark mode text should have sufficient contrast with dark background', () => {
      // Parse dark mode values from CSS
      const darkModeMatch = cssContent.match(/\[data-theme="dark"\]\s*\{([^}]+)\}/);

      if (darkModeMatch) {
        const darkBlock = darkModeMatch[1];

        // Extract values
        const textMatch = darkBlock.match(/--color-text\s*:\s*([^;]+);/);
        const bgMatch = darkBlock.match(/--color-background\s*:\s*([^;]+);/);

        if (textMatch && bgMatch) {
          const textColor = textMatch[1].trim();
          const bgColor = bgMatch[1].trim();

          if (textColor.startsWith('#') && bgColor.startsWith('#')) {
            const ratio = getContrastRatio(textColor, bgColor);
            // WCAG AA requires at least 4.5:1
            expect(ratio).toBeGreaterThanOrEqual(4.5);
          }
        }
      }
    });
  });

  describe('CSS Syntax', () => {
    test('should have valid CSS syntax', () => {
      // Basic checks for common CSS issues
      expect(cssContent).not.toMatch(/:\s*;/); // No empty values
      // Note: }} is valid for nested selectors inside @media blocks
    });

    test('should close all selector blocks', () => {
      const openBraces = (cssContent.match(/{/g) || []).length;
      const closeBraces = (cssContent.match(/}/g) || []).length;
      expect(openBraces).toBe(closeBraces);
    });
  });
});
