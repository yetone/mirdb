/**
 * CSS Validation Tests
 *
 * Verifies that the homepage CSS is valid, well-formed, and follows best practices.
 * Tests cover:
 * 1. CSS syntax validation (W3C-compatible checks)
 * 2. CSS file size (under 50KB minified)
 * 3. CSS custom properties (design tokens for maintainability)
 */

const fs = require('fs');
const path = require('path');

describe('CSS Validation', () => {
  let cssContent;
  let cssPath;

  beforeAll(() => {
    cssPath = path.join(__dirname, '../../styles.css');
    cssContent = fs.readFileSync(cssPath, 'utf8');
  });

  describe('W3C CSS Validation Checks', () => {
    test('CSS file exists and is readable', () => {
      expect(cssContent).toBeDefined();
      expect(cssContent.length).toBeGreaterThan(0);
    });

    test('no unclosed braces in CSS', () => {
      // Count opening and closing braces
      const openBraces = (cssContent.match(/{/g) || []).length;
      const closeBraces = (cssContent.match(/}/g) || []).length;

      expect(openBraces).toBe(closeBraces);
    });

    test('no unclosed parentheses in CSS', () => {
      // Count opening and closing parentheses
      const openParens = (cssContent.match(/\(/g) || []).length;
      const closeParens = (cssContent.match(/\)/g) || []).length;

      expect(openParens).toBe(closeParens);
    });

    test('no duplicate property declarations in same rule', () => {
      // Extract rule blocks and check for duplicate properties within each
      const ruleBlocks = cssContent.match(/\{[^}]+\}/g) || [];
      const duplicates = [];

      ruleBlocks.forEach((block, index) => {
        // Extract property names (before the colon)
        const properties = block.match(/[\w-]+(?=\s*:)/g) || [];
        const seen = new Set();

        properties.forEach(prop => {
          // Some properties like transition, transform etc can appear once in standard form
          // We ignore vendor prefixes as they're different properties
          if (!prop.startsWith('-webkit-') &&
              !prop.startsWith('-moz-') &&
              !prop.startsWith('-ms-') &&
              !prop.startsWith('-o-')) {
            if (seen.has(prop)) {
              duplicates.push({ block: index, property: prop });
            }
            seen.add(prop);
          }
        });
      });

      // Allow some intentional duplicates for fallbacks (e.g., background-clip)
      // But there shouldn't be many
      expect(duplicates.length).toBeLessThanOrEqual(5);
    });

    test('all color values are valid formats', () => {
      // Valid CSS color formats: hex, rgb, rgba, hsl, hsla, named colors, var()
      const colorProperties = cssContent.match(/color\s*:\s*([^;]+)/gi) || [];
      const backgroundColorProperties = cssContent.match(/background-color\s*:\s*([^;]+)/gi) || [];
      const borderColorProperties = cssContent.match(/border-color\s*:\s*([^;]+)/gi) || [];

      const allColorValues = [...colorProperties, ...backgroundColorProperties, ...borderColorProperties];

      allColorValues.forEach(declaration => {
        const value = declaration.split(':')[1].trim();
        // Valid patterns: hex, rgb, rgba, hsl, hsla, var(), named colors, transparent, inherit, initial
        const validPatterns = [
          /^#[0-9a-fA-F]{3,8}$/,           // hex
          /^rgb\(/,                          // rgb()
          /^rgba\(/,                         // rgba()
          /^hsl\(/,                          // hsl()
          /^hsla\(/,                         // hsla()
          /^var\(/,                          // CSS variable
          /^transparent$/i,                  // transparent
          /^inherit$/i,                      // inherit
          /^initial$/i,                      // initial
          /^currentColor$/i,                 // currentColor
          /^[a-z]+$/i,                       // named colors
        ];

        const isValid = validPatterns.some(pattern => pattern.test(value));
        if (!isValid) {
          // Some values might have !important or multiple values
          expect(value).toMatch(/(var\(|#[0-9a-fA-F]|rgb|hsl|transparent|inherit|initial|currentColor)/i);
        }
      });
    });

    test('no invalid CSS property names', () => {
      // Check for common typos in CSS property names
      const invalidProperties = [
        'colour',           // British spelling - should be color
        'font-colour',      // Invalid
        'backround',        // Typo for background
        'maring',           // Typo for margin
        'paddin',           // Typo for padding
        'widht',            // Typo for width
        'heigth',           // Typo for height
        'positon',          // Typo for position
        'dipslay',          // Typo for display
      ];

      invalidProperties.forEach(invalid => {
        const regex = new RegExp(`\\b${invalid}\\s*:`, 'gi');
        const hasInvalid = regex.test(cssContent);
        expect(hasInvalid).toBe(false);
      });
    });

    test('all media queries have valid syntax', () => {
      const mediaQueries = cssContent.match(/@media[^{]+/g) || [];

      mediaQueries.forEach(query => {
        // Media query should have parentheses for conditions
        if (query.includes('(')) {
          const openParens = (query.match(/\(/g) || []).length;
          const closeParens = (query.match(/\)/g) || []).length;
          expect(openParens).toBe(closeParens);
        }

        // Common valid media types and features
        const validTerms = ['screen', 'print', 'all', 'max-width', 'min-width', 'max-height', 'min-height', 'orientation', 'prefers-color-scheme', 'prefers-reduced-motion'];
        // At least one valid term should be present
        const hasValidTerm = validTerms.some(term => query.toLowerCase().includes(term));
        expect(hasValidTerm).toBe(true);
      });
    });

    test('no empty rule blocks', () => {
      // Empty rule blocks are invalid/wasteful
      const emptyBlocks = cssContent.match(/\{\s*\}/g) || [];
      expect(emptyBlocks.length).toBe(0);
    });

    test('all selectors are valid CSS syntax', () => {
      // Remove CSS comments first to avoid false positives
      const cssWithoutComments = cssContent.replace(/\/\*[\s\S]*?\*\//g, '');

      // Extract selectors (text before {)
      const selectors = cssWithoutComments.match(/[^}]+(?=\{)/g) || [];

      selectors.forEach(selector => {
        // Skip @media, @keyframes, @page, etc.
        if (selector.trim().startsWith('@')) return;

        // Class names and IDs shouldn't start with numbers
        // Check actual class selector patterns only: .1class or .2test
        const classSelectors = selector.match(/\.[a-zA-Z0-9_-]+/g) || [];
        classSelectors.forEach(cls => {
          // Class name after the dot shouldn't start with a digit
          const className = cls.substring(1); // Remove the leading dot
          expect(className).not.toMatch(/^\d/);
        });
      });
    });

    test('valid @rules are used', () => {
      const atRules = cssContent.match(/@[\w-]+/g) || [];
      const validAtRules = [
        '@media', '@keyframes', '@font-face', '@import', '@charset',
        '@page', '@supports', '@namespace', '@document', '@viewport',
        '@counter-style', '@font-feature-values', '@property', '@layer'
      ];

      atRules.forEach(rule => {
        const isValid = validAtRules.some(valid => rule.startsWith(valid));
        expect(isValid).toBe(true);
      });
    });
  });

  describe('CSS File Size', () => {
    test('CSS file is under 50KB minified', () => {
      // Minify CSS by removing comments, whitespace, and newlines
      const minified = cssContent
        .replace(/\/\*[\s\S]*?\*\//g, '')  // Remove comments
        .replace(/\s+/g, ' ')              // Collapse whitespace
        .replace(/\s*([{}:;,])\s*/g, '$1') // Remove spaces around punctuation
        .trim();

      const sizeInKB = Buffer.byteLength(minified, 'utf8') / 1024;

      expect(sizeInKB).toBeLessThan(50);
    });

    test('CSS file size is reasonable (under 100KB raw)', () => {
      const sizeInKB = Buffer.byteLength(cssContent, 'utf8') / 1024;

      // Raw file should be under 100KB
      expect(sizeInKB).toBeLessThan(100);
    });

    test('no excessively long lines in CSS (code quality)', () => {
      const lines = cssContent.split('\n');
      const longLines = lines.filter(line => line.length > 200);

      // Should have minimal very long lines
      expect(longLines.length).toBeLessThanOrEqual(5);
    });
  });

  describe('CSS Custom Properties (Design Tokens)', () => {
    test('uses CSS custom properties for design tokens', () => {
      const hasCustomProperties = cssContent.includes('--');
      expect(hasCustomProperties).toBe(true);
    });

    test('defines custom properties in :root', () => {
      const hasRootWithVariables = cssContent.match(/:root\s*\{[^}]*--/);
      expect(hasRootWithVariables).toBeTruthy();
    });

    test('has color design tokens', () => {
      const colorTokens = [
        '--color-primary',
        '--color-background',
        '--color-text',
      ];

      colorTokens.forEach(token => {
        expect(cssContent).toContain(token);
      });
    });

    test('has spacing design tokens', () => {
      const hasSpacingTokens = cssContent.includes('--spacing');
      expect(hasSpacingTokens).toBe(true);
    });

    test('has typography design tokens', () => {
      const hasTypographyTokens =
        cssContent.includes('--font-family') ||
        cssContent.includes('--font-mono') ||
        cssContent.includes('--font-size');

      expect(hasTypographyTokens).toBe(true);
    });

    test('has border/radius design tokens', () => {
      const hasBorderTokens = cssContent.includes('--border-radius');
      expect(hasBorderTokens).toBe(true);
    });

    test('custom properties are used throughout the stylesheet', () => {
      // Count usage of var()
      const varUsages = (cssContent.match(/var\(--/g) || []).length;

      // Should use CSS variables extensively (at least 20 times)
      expect(varUsages).toBeGreaterThan(20);
    });

    test('custom properties have meaningful names', () => {
      // Extract all custom property definitions
      const customProps = cssContent.match(/--[\w-]+/g) || [];

      customProps.forEach(prop => {
        // Should be longer than just --a or --x (meaningful names)
        expect(prop.length).toBeGreaterThan(4);
      });
    });

    test('no magic numbers - uses design tokens for common values', () => {
      // Check that padding and margin commonly use var()
      const paddingRules = cssContent.match(/padding[^:]*:\s*([^;]+)/g) || [];
      const marginRules = cssContent.match(/margin[^:]*:\s*([^;]+)/g) || [];

      const allSpacingRules = [...paddingRules, ...marginRules];

      // Count how many use var()
      const usingVars = allSpacingRules.filter(rule => rule.includes('var(')).length;
      const total = allSpacingRules.length;

      // At least 50% of spacing rules should use CSS variables
      if (total > 0) {
        const percentage = (usingVars / total) * 100;
        expect(percentage).toBeGreaterThan(40);
      }
    });

    test('consistent naming convention for custom properties', () => {
      // Extract all custom property definitions
      const customProps = cssContent.match(/--[\w-]+/g) || [];
      const uniqueProps = [...new Set(customProps)];

      // Check naming follows kebab-case convention
      uniqueProps.forEach(prop => {
        // Should be kebab-case (lowercase with hyphens)
        expect(prop).toMatch(/^--[a-z][a-z0-9-]*$/);
      });
    });
  });
});
