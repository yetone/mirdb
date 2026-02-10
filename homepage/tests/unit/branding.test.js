/**
 * Brand Consistency and Visual Design Unit Tests
 * Owner: Scenario 9 - Brand Consistency and Visual Design
 *
 * Unit tests for validating brand consistency, logo usage, typography,
 * and visual design of the MirDB homepage.
 *
 * Requirements traced:
 * - NFR-2: Brand consistency with project
 * - Design Requirements: Clean, professional design
 */

const fs = require('fs');
const path = require('path');

describe('Brand Consistency and Visual Design', () => {
  let htmlContent;
  let cssContent;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../index.html');
    const cssPath = path.join(__dirname, '../../css/styles.css');
    htmlContent = fs.readFileSync(htmlPath, 'utf8');
    cssContent = fs.readFileSync(cssPath, 'utf8');
  });

  describe('Logo Usage (Test Case 1)', () => {
    test('should have logo image referencing logo file', () => {
      // Check for logo image with src containing logo.gif or logo.png (optimized for performance)
      const logoPattern = /<img[^>]*src\s*=\s*["']([^"']*logo\.(gif|png))["'][^>]*>/i;
      const match = htmlContent.match(logoPattern);
      expect(match).toBeTruthy();
      expect(match[1]).toMatch(/logo\.(gif|png)$/);
    });

    test('should have logo image inside header or logo container', () => {
      // Check that logo is in header
      const headerSection = htmlContent.match(/<header[^>]*>[\s\S]*?<\/header>/i);
      expect(headerSection).toBeTruthy();
      expect(headerSection[0]).toMatch(/<img[^>]*src\s*=\s*["'][^"']*logo\.(gif|png)["'][^>]*>/i);
    });

    test('should have alt attribute on logo image for accessibility', () => {
      // Check that logo has alt text (accept .gif or .png)
      const logoWithAlt = /<img[^>]*src\s*=\s*["'][^"']*logo\.(gif|png)["'][^>]*alt\s*=\s*["'][^"']+["'][^>]*>/i;
      const logoWithAltReverse = /<img[^>]*alt\s*=\s*["'][^"']+["'][^>]*src\s*=\s*["'][^"']*logo\.(gif|png)["'][^>]*>/i;
      expect(htmlContent.match(logoWithAlt) || htmlContent.match(logoWithAltReverse)).toBeTruthy();
    });
  });

  describe('Consistent Font Usage (Test Case 3)', () => {
    test('should define a main font-family CSS variable', () => {
      // Check for --font-family custom property
      const fontFamilyVar = /--font-family\s*:\s*[^;]+;/i;
      expect(cssContent).toMatch(fontFamilyVar);
    });

    test('should use sans-serif font stack for body text', () => {
      // Check that body uses the font family variable or a sans-serif stack
      const bodyFontPattern = /body[^{]*\{[^}]*font-family\s*:/i;
      expect(cssContent).toMatch(bodyFontPattern);

      // Should reference --font-family variable or use sans-serif
      const fontDeclaration = cssContent.match(/--font-family\s*:\s*([^;]+);/i);
      if (fontDeclaration) {
        const fontValue = fontDeclaration[1].toLowerCase();
        expect(fontValue).toMatch(/sans-serif|system|segoe|roboto|helvetica|arial/i);
      }
    });

    test('should have consistent heading font styles', () => {
      // Check for heading styles
      const headingStyles = /h1\s*,\s*h2\s*,\s*h3|h[1-6][^{]*\{[^}]*font/i;
      expect(cssContent).toMatch(headingStyles);
    });

    test('should define font-weight for headings', () => {
      // Check that headings have font-weight defined
      const headingFontWeight = /h[1-6][^{]*\{[^}]*font-weight\s*:/i;
      expect(cssContent).toMatch(headingFontWeight);
    });
  });

  describe('Monospace Font for Code (Test Case 5)', () => {
    test('should define a monospace font-family CSS variable', () => {
      // Check for --font-family-mono custom property
      const monoFontVar = /--font-family-mono\s*:\s*[^;]+;/i;
      expect(cssContent).toMatch(monoFontVar);
    });

    test('should use monospace font for code elements', () => {
      // Check that code/pre elements use monospace font
      const codeMonospace = /(?:code|pre)[^{]*\{[^}]*font-family\s*:[^;]*(?:monospace|mono|consolas|menlo|courier)/i;
      expect(cssContent).toMatch(codeMonospace);
    });

    test('should have code elements in HTML that will use monospace styling', () => {
      // Verify there are code elements in the HTML
      expect(htmlContent).toMatch(/<code[^>]*>/i);
      expect(htmlContent).toMatch(/<pre[^>]*>/i);
    });
  });

  describe('CSS Custom Properties for Brand Colors', () => {
    test('should define primary color variable', () => {
      expect(cssContent).toMatch(/--color-primary\s*:\s*[^;]+;/i);
    });

    test('should define background color variable', () => {
      expect(cssContent).toMatch(/--color-background\s*:\s*[^;]+;/i);
    });

    test('should define text color variable', () => {
      expect(cssContent).toMatch(/--color-text\s*:\s*[^;]+;/i);
    });

    test('should use CSS variables for consistent color scheme', () => {
      // Check that colors are used via variables
      const varUsage = /var\s*\(\s*--color-/gi;
      const matches = cssContent.match(varUsage);
      expect(matches).toBeTruthy();
      expect(matches.length).toBeGreaterThan(5); // Multiple color variable usages
    });
  });

  describe('Typography and Readability', () => {
    test('should define base font-size', () => {
      // Check for body font-size
      const fontSizePattern = /body[^{]*\{[^}]*font-size\s*:/i;
      expect(cssContent).toMatch(fontSizePattern);
    });

    test('should define line-height for readability', () => {
      // Check for body line-height
      const lineHeightPattern = /body[^{]*\{[^}]*line-height\s*:/i;
      expect(cssContent).toMatch(lineHeightPattern);
    });

    test('should have different font sizes for heading levels', () => {
      // Check for h1, h2, h3 font-size definitions
      expect(cssContent).toMatch(/h1[^{]*\{[^}]*font-size\s*:/i);
      expect(cssContent).toMatch(/h2[^{]*\{[^}]*font-size\s*:/i);
      expect(cssContent).toMatch(/h3[^{]*\{[^}]*font-size\s*:/i);
    });
  });

  describe('Spacing CSS Variables for Whitespace', () => {
    test('should define spacing variables', () => {
      // Check for spacing custom properties
      expect(cssContent).toMatch(/--spacing-\w+\s*:\s*[^;]+;/i);
    });

    test('should have multiple spacing scale values', () => {
      // Check for different spacing sizes (sm, md, lg, etc.)
      const spacingMatches = cssContent.match(/--spacing-\w+\s*:\s*[^;]+;/gi);
      expect(spacingMatches).toBeTruthy();
      expect(spacingMatches.length).toBeGreaterThanOrEqual(3);
    });

    test('should use spacing variables in section styles', () => {
      // Check that sections use spacing variables
      const paddingWithVar = /padding\s*:[^;]*var\s*\(\s*--spacing/i;
      const marginWithVar = /margin\s*:[^;]*var\s*\(\s*--spacing/i;
      expect(cssContent.match(paddingWithVar) || cssContent.match(marginWithVar)).toBeTruthy();
    });
  });

  describe('Professional Design Elements', () => {
    test('should define border-radius for consistent rounded corners', () => {
      expect(cssContent).toMatch(/--border-radius\s*:\s*[^;]+;/i);
    });

    test('should define box-shadow variables for depth', () => {
      expect(cssContent).toMatch(/--shadow-\w+\s*:\s*[^;]+;/i);
    });

    test('should have a maximum content width defined', () => {
      expect(cssContent).toMatch(/--max-width\s*:\s*[^;]+;|max-width\s*:\s*\d+/i);
    });

    test('should have container class with max-width', () => {
      const containerPattern = /\.container[^{]*\{[^}]*max-width\s*:/i;
      expect(cssContent).toMatch(containerPattern);
    });
  });
});
