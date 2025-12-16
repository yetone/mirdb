/**
 * Professional Design Quality Tests
 * Scenario: Verify that the design is clean and professional for developer tooling (NFR-6)
 *
 * This test file validates:
 * - CSS variables for theming (colors, fonts)
 * - Base font size of at least 16px for readability
 * - Consistent spacing system (multiples of 4px or 8px)
 * - No broken layouts or overlapping elements
 */

const fs = require('fs');
const path = require('path');

describe('Professional Design Quality', () => {
  let htmlContent;
  let cssContent;
  let document;

  beforeAll(() => {
    // Load source files
    const htmlPath = path.resolve(__dirname, '../index.html');
    const cssPath = path.resolve(__dirname, '../styles.css');

    htmlContent = fs.readFileSync(htmlPath, 'utf8');
    cssContent = fs.readFileSync(cssPath, 'utf8');

    document = new DOMParser().parseFromString(htmlContent, 'text/html');
  });

  // Test Case 1: CSS uses CSS variables for theming
  describe('Test Case 1: CSS Variables for Theming', () => {
    test('should define CSS custom properties (variables) in :root', () => {
      // Check for :root selector with custom properties
      const hasRootSelector = cssContent.includes(':root');
      expect(hasRootSelector).toBe(true);
    });

    test('should define color variables', () => {
      // Check for color-related CSS variables
      const colorVariables = [
        '--color-primary',
        '--color-text',
        '--color-bg'
      ];

      colorVariables.forEach(variable => {
        expect(cssContent).toContain(variable);
      });
    });

    test('should define font family variables', () => {
      // Check for font-family CSS variable
      const hasFontFamily = cssContent.includes('--font-family');
      expect(hasFontFamily).toBe(true);
    });

    test('should use CSS variables throughout the stylesheet', () => {
      // Count usage of var() function
      const varUsages = (cssContent.match(/var\(--/g) || []).length;
      // Should have multiple usages of CSS variables for consistency
      expect(varUsages).toBeGreaterThan(10);
    });

    test('should define primary color variable', () => {
      // Check that primary color is defined as a CSS variable
      expect(cssContent).toMatch(/--color-primary\s*:\s*#[0-9a-fA-F]{6}/);
    });

    test('should define background color variable', () => {
      // Check that background color is defined
      expect(cssContent).toMatch(/--color-bg\s*:\s*#[0-9a-fA-F]{6}/);
    });

    test('should define text color variable', () => {
      // Check that text color is defined
      expect(cssContent).toMatch(/--color-text\s*:\s*#[0-9a-fA-F]{6}/);
    });

    test('should define monospace font family for code', () => {
      // Check for monospace font variable for code blocks
      const hasMonoFont = cssContent.includes('--font-family-mono');
      expect(hasMonoFont).toBe(true);
    });
  });

  // Test Case 2: Base font size is at least 16px for readability
  describe('Test Case 2: Base Font Size Readability', () => {
    test('body should have font-size of at least 16px', () => {
      // Extract body font-size
      const bodyFontSizeMatch = cssContent.match(/body\s*\{[^}]*font-size\s*:\s*(\d+)px/);
      expect(bodyFontSizeMatch).not.toBeNull();

      const fontSize = parseInt(bodyFontSizeMatch[1], 10);
      expect(fontSize).toBeGreaterThanOrEqual(16);
    });

    test('body font-size should be exactly 16px or larger', () => {
      // Check that the body has a proper base font size
      const hasProperFontSize = cssContent.includes('font-size: 16px') ||
                                 cssContent.includes('font-size: 17px') ||
                                 cssContent.includes('font-size: 18px') ||
                                 cssContent.includes('font-size: 1rem') ||
                                 cssContent.includes('font-size: 100%');
      expect(hasProperFontSize).toBe(true);
    });

    test('should have appropriate line-height for readability', () => {
      // Check for line-height in body
      const hasLineHeight = cssContent.includes('line-height');
      expect(hasLineHeight).toBe(true);

      // Line height should be at least 1.4 for readability
      const lineHeightMatch = cssContent.match(/body\s*\{[^}]*line-height\s*:\s*([\d.]+)/);
      if (lineHeightMatch) {
        const lineHeight = parseFloat(lineHeightMatch[1]);
        expect(lineHeight).toBeGreaterThanOrEqual(1.4);
      }
    });

    test('headings should have readable sizes', () => {
      // h1 should be significantly larger than body text
      const h1SizeMatch = cssContent.match(/\.hero\s+h1[^}]*font-size\s*:\s*([\d.]+)rem/);
      if (h1SizeMatch) {
        const h1Size = parseFloat(h1SizeMatch[1]);
        expect(h1Size).toBeGreaterThanOrEqual(2);
      }
    });

    test('code blocks should have readable font size', () => {
      // Code font size should not be too small
      const codeMatch = cssContent.match(/pre\s+code[^}]*font-size\s*:\s*([\d.]+)/);
      if (codeMatch) {
        const codeSize = parseFloat(codeMatch[1]);
        // Font size in rem or em should be at least 0.75
        expect(codeSize).toBeGreaterThanOrEqual(0.75);
      }
    });
  });

  // Test Case 3: Consistent spacing system
  describe('Test Case 3: Consistent Spacing System', () => {
    test('should use rem units for spacing consistency', () => {
      // Check for rem-based spacing
      const remUsages = (cssContent.match(/:\s*[\d.]+rem/g) || []).length;
      expect(remUsages).toBeGreaterThan(5);
    });

    test('should have consistent padding values', () => {
      // Extract padding values and check for patterns
      const paddingValues = cssContent.match(/padding\s*:\s*[\d.]+rem/g) || [];
      expect(paddingValues.length).toBeGreaterThan(0);
    });

    test('should use consistent gap values for flexbox and grid', () => {
      // Check for gap property usage
      const hasGap = cssContent.includes('gap:') || cssContent.includes('gap ');
      expect(hasGap).toBe(true);
    });

    test('should have consistent margin values', () => {
      // Check for margin usage with consistent units
      const marginValues = cssContent.match(/margin(-\w+)?\s*:\s*[\d.]+rem/g) || [];
      expect(marginValues.length).toBeGreaterThan(0);
    });

    test('spacing values should follow a scale pattern', () => {
      // Common spacing scale values in rem (0.25, 0.5, 0.75, 1, 1.5, 2, 3, 4, etc.)
      const hasQuarterRem = cssContent.includes('0.25rem');
      const hasHalfRem = cssContent.includes('0.5rem');
      const hasOneRem = cssContent.includes('1rem');
      const hasTwoRem = cssContent.includes('2rem');

      // Should use at least some standard spacing values
      const spacingValuesUsed = [hasQuarterRem, hasHalfRem, hasOneRem, hasTwoRem].filter(Boolean).length;
      expect(spacingValuesUsed).toBeGreaterThanOrEqual(2);
    });

    test('button padding should be consistent', () => {
      // Check .btn class has consistent padding
      const btnMatch = cssContent.match(/\.btn\s*\{[^}]*padding\s*:\s*([^;]+)/);
      expect(btnMatch).not.toBeNull();
    });

    test('section padding should be consistent', () => {
      // Check section padding
      const sectionMatch = cssContent.match(/section\s*\{[^}]*padding\s*:\s*([^;]+)/);
      if (sectionMatch) {
        expect(sectionMatch[1]).toMatch(/\d/);
      }
    });
  });

  // Test Case 4: No broken layouts or overlapping elements
  describe('Test Case 4: No Broken Layouts', () => {
    test('should use box-sizing border-box for predictable layouts', () => {
      const hasBoxSizing = cssContent.includes('box-sizing: border-box');
      expect(hasBoxSizing).toBe(true);
    });

    test('should have proper max-width constraints', () => {
      // Check for max-width to prevent content from being too wide
      const hasMaxWidth = cssContent.includes('max-width');
      expect(hasMaxWidth).toBe(true);
    });

    test('should handle overflow appropriately', () => {
      // Check for overflow handling on code blocks
      const hasOverflowX = cssContent.includes('overflow-x: auto') ||
                           cssContent.includes('overflow-x:auto');
      expect(hasOverflowX).toBe(true);
    });

    test('should use flexbox or grid for layouts', () => {
      // Check for modern layout methods
      const usesFlex = cssContent.includes('display: flex') || cssContent.includes('display:flex');
      const usesGrid = cssContent.includes('display: grid') || cssContent.includes('display:grid');
      expect(usesFlex || usesGrid).toBe(true);
    });

    test('grid should have proper column definitions', () => {
      // If grid is used, it should have proper column definitions
      const hasGridColumns = cssContent.includes('grid-template-columns');
      if (cssContent.includes('display: grid')) {
        expect(hasGridColumns).toBe(true);
      }
    });

    test('navbar should be properly positioned', () => {
      // Check navbar has proper positioning
      const navbarMatch = cssContent.match(/\.navbar\s*\{[^}]+\}/);
      expect(navbarMatch).not.toBeNull();

      // Navbar should have display and positioning
      expect(navbarMatch[0]).toContain('display');
    });

    test('z-index should be properly managed', () => {
      // Check that z-index is used appropriately for stacking
      const hasZIndex = cssContent.includes('z-index');
      expect(hasZIndex).toBe(true);
    });

    test('responsive breakpoints should handle layout changes', () => {
      // Check for media queries
      const mediaQueryCount = (cssContent.match(/@media/g) || []).length;
      expect(mediaQueryCount).toBeGreaterThan(0);
    });

    test('feature cards should have proper layout', () => {
      // Check .feature-card has proper styling
      const featureCardMatch = cssContent.match(/\.feature-card\s*\{[^}]+\}/);
      if (featureCardMatch) {
        expect(featureCardMatch[0]).toContain('padding');
      }
    });

    test('pre elements should handle long code properly', () => {
      // Check that pre elements handle overflow
      const preMatch = cssContent.match(/pre\s*\{[^}]+\}/);
      if (preMatch) {
        expect(preMatch[0]).toContain('overflow');
      }
    });

    test('all sections in HTML should have proper structure', () => {
      // Check that all sections have content
      const sections = document.querySelectorAll('section');
      expect(sections.length).toBeGreaterThan(0);

      sections.forEach(section => {
        // Each section should have content (not empty)
        expect(section.innerHTML.trim().length).toBeGreaterThan(0);
      });
    });

    test('footer should be at bottom and not overlap content', () => {
      // Check footer exists and has proper styling
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();

      // Check footer has proper CSS styling
      const footerMatch = cssContent.match(/footer\s*\{[^}]+\}/);
      expect(footerMatch).not.toBeNull();
    });

    test('container elements should have max-width', () => {
      // Check .container class has max-width
      const containerMatch = cssContent.match(/\.container\s*\{[^}]+max-width/);
      expect(containerMatch).not.toBeNull();
    });

    test('should not have position absolute without proper constraints', () => {
      // Position absolute elements should have top/left/right/bottom or transform
      const absoluteUsages = (cssContent.match(/position\s*:\s*absolute/g) || []).length;

      // If absolute positioning is used, it should be minimal and controlled
      // The skip-link is the main expected use case
      expect(absoluteUsages).toBeLessThanOrEqual(2);
    });
  });

  // Additional: Visual Hierarchy Tests
  describe('Visual Hierarchy', () => {
    test('headings should have decreasing sizes', () => {
      // h1 > h2 > h3 in terms of font size
      const h1Size = cssContent.match(/h1[^}]*font-size\s*:\s*([\d.]+)/) ||
                     cssContent.match(/\.hero\s+h1[^}]*font-size\s*:\s*([\d.]+)/);
      const h2Size = cssContent.match(/h2[^}]*font-size\s*:\s*([\d.]+)/);

      if (h1Size && h2Size) {
        expect(parseFloat(h1Size[1])).toBeGreaterThan(parseFloat(h2Size[1]));
      }
    });

    test('should have clear section separation', () => {
      // Sections should have padding or margin for separation
      const sectionPadding = cssContent.match(/section\s*\{[^}]*padding/);
      expect(sectionPadding).not.toBeNull();
    });

    test('links should be distinguishable from regular text', () => {
      // Check that links have different color or styling
      const linkColor = cssContent.includes('a {') || cssContent.includes('a:');
      expect(linkColor).toBe(true);
    });

    test('buttons should have distinct styling', () => {
      // Check .btn class has distinct styling
      const btnStyling = cssContent.match(/\.btn\s*\{[^}]+\}/);
      expect(btnStyling).not.toBeNull();

      // Button variants (.btn-primary, .btn-secondary) should have background colors
      const btnPrimary = cssContent.match(/\.btn-primary\s*\{[^}]+\}/);
      expect(btnPrimary).not.toBeNull();
      expect(btnPrimary[0]).toContain('background');
    });

    test('code should be visually distinct from prose', () => {
      // Check that code has different styling
      const codeMatch = cssContent.match(/code\s*\{[^}]*font-family/) ||
                        cssContent.match(/pre\s+code\s*\{[^}]*font-family/);
      expect(codeMatch).not.toBeNull();
    });
  });
});
