/**
 * Brand Consistency Unit Tests
 * Owner: Scenario 9 - Brand Consistency and Visual Design
 *
 * Unit tests for validating brand consistency including logo usage,
 * consistent font definitions, and typography styles.
 *
 * Requirements traced:
 * - NFR-2: Homepage shall maintain consistency with existing project branding
 * - Design Requirements: Professional appearance and readable typography
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

  // Test Case 1: Check logo image source
  describe('Logo Usage (Test Case 1)', () => {
    test('logo image src references logo file', () => {
      // Test Case 1: Check logo image source (accept .gif or .png for performance optimization)
      const logoImgMatch = htmlContent.match(/<img[^>]*src\s*=\s*["']([^"']*logo\.(gif|png))["'][^>]*>/i);
      expect(logoImgMatch).toBeTruthy();
      const logoSrc = logoImgMatch[1];
      // Should reference logo.gif or logo.png
      expect(logoSrc).toMatch(/logo\.(gif|png)$/i);
    });

    test('logo is in header section', () => {
      // Verify logo is within header for proper branding placement
      const headerMatch = htmlContent.match(/<header[^>]*>[\s\S]*?<\/header>/i);
      expect(headerMatch).toBeTruthy();
      expect(headerMatch[0]).toMatch(/<img[^>]*logo\.(gif|png)/i);
    });

    test('logo image has alt attribute for accessibility', () => {
      // Ensure logo has proper alt text for accessibility and branding
      const logoImgMatch = htmlContent.match(/<img[^>]*src\s*=\s*["'][^"']*logo\.(gif|png)["'][^>]*>/i);
      expect(logoImgMatch).toBeTruthy();
      expect(logoImgMatch[0]).toMatch(/alt\s*=\s*["'][^"']+["']/i);
    });
  });

  // Test Case 3: Verify consistent font usage
  describe('Consistent Font Usage (Test Case 3)', () => {
    test('page uses consistent font family throughout (defined in CSS)', () => {
      // Test Case 3: Page uses consistent font family throughout (defined in CSS)
      expect(cssContent).toMatch(/--font-family\s*:/);
    });

    test('CSS uses variable for body font-family', () => {
      // Ensure body uses the defined font variable for consistency
      expect(cssContent).toMatch(/body\s*\{[\s\S]*?font-family\s*:\s*var\(--font-family/);
    });

    test('CSS has sans-serif font family defined', () => {
      // Professional technical sites typically use sans-serif fonts
      const fontMatch = cssContent.match(/--font-family\s*:\s*([^;]+);/);
      expect(fontMatch).toBeTruthy();
      const fontValue = fontMatch[1].toLowerCase();
      expect(fontValue).toMatch(/sans-serif/);
    });

    test('CSS custom properties define typography variables', () => {
      // Check for font-family custom property
      expect(cssContent).toMatch(/--font-family\s*:/);
      // Check for monospace font custom property
      expect(cssContent).toMatch(/--font-family-mono\s*:/);
    });

    test('headings use consistent font weight', () => {
      // Headings should use the same font family for brand consistency
      // Check that headings don't have a direct font-family override (they should inherit from body)
      const headingRules = cssContent.match(/h[1-6]\s*\{[^}]*\}/g) || [];
      const hasDirectFontOverride = headingRules.some((rule) => {
        // Check if the specific heading rule has a font-family that's not the variable
        return /font-family\s*:\s*(?!var\(--font-family)/.test(rule);
      });
      // Headings should either have no font-family (inherit) or use the variable
      expect(hasDirectFontOverride).toBe(false);
    });
  });

  // Test Case 5: Verify monospace font for code
  describe('Monospace Font for Code (Test Case 5)', () => {
    test('code examples use monospace font family', () => {
      // Test Case 5: Code examples use monospace font family
      expect(cssContent).toMatch(/--font-family-mono\s*:/);
    });

    test('monospace font family includes common monospace fonts', () => {
      const monoMatch = cssContent.match(/--font-family-mono\s*:\s*([^;]+);/);
      expect(monoMatch).toBeTruthy();
      const monoValue = monoMatch[1].toLowerCase();
      const hasMonospaceFonts =
        monoValue.includes('consolas') ||
        monoValue.includes('menlo') ||
        monoValue.includes('monaco') ||
        monoValue.includes('monospace') ||
        monoValue.includes('sfmono');
      expect(hasMonospaceFonts).toBe(true);
    });

    test('code elements use monospace font variable', () => {
      // Code elements should use the monospace font variable
      expect(cssContent).toMatch(/code[\s\S]*?font-family\s*:\s*var\(--font-family-mono/);
    });

    test('pre elements have code-specific styling', () => {
      // Pre elements should also have background and padding for code blocks
      expect(cssContent).toMatch(/pre\s*\{[^}]*background/s);
      expect(cssContent).toMatch(/pre\s*\{[^}]*padding/s);
    });
  });

  // Color scheme consistency
  describe('Color Scheme Consistency', () => {
    test('CSS defines primary color variable', () => {
      expect(cssContent).toMatch(/--color-primary\s*:/);
    });

    test('CSS defines secondary color variable', () => {
      expect(cssContent).toMatch(/--color-secondary\s*:/);
    });

    test('CSS defines text and background color variables', () => {
      expect(cssContent).toMatch(/--color-text\s*:/);
      expect(cssContent).toMatch(/--color-background\s*:/);
    });

    test('CSS defines surface and border color variables', () => {
      expect(cssContent).toMatch(/--color-surface\s*:/);
      expect(cssContent).toMatch(/--color-border\s*:/);
    });

    test('CSS uses color custom properties throughout for consistent theming', () => {
      // Count usage of var(--color- to verify consistent variable usage
      const colorVarUsage = cssContent.match(/var\(--color-/g);
      expect(colorVarUsage).toBeTruthy();
      // Should be used multiple times for consistency
      expect(colorVarUsage.length).toBeGreaterThan(10);
    });
  });

  // Spacing consistency
  describe('Spacing Consistency', () => {
    test('CSS defines spacing variables', () => {
      expect(cssContent).toMatch(/--spacing-sm\s*:/);
      expect(cssContent).toMatch(/--spacing-md\s*:/);
      expect(cssContent).toMatch(/--spacing-lg\s*:/);
      expect(cssContent).toMatch(/--spacing-xl\s*:/);
    });

    test('CSS defines multiple spacing scale values', () => {
      // Professional design uses a spacing scale
      const spacingVars = cssContent.match(/--spacing-[a-z0-9]+\s*:/gi);
      expect(spacingVars).toBeTruthy();
      expect(spacingVars.length).toBeGreaterThanOrEqual(3);
    });

    test('sections use spacing variables for padding', () => {
      // Check that major sections use spacing variables
      expect(cssContent).toMatch(/padding\s*:\s*var\(--spacing/);
    });
  });

  // Professional design elements
  describe('Professional Design Elements', () => {
    test('CSS defines border-radius for consistent corners', () => {
      expect(cssContent).toMatch(/--border-radius\s*:/);
    });

    test('CSS defines shadow variables for depth', () => {
      expect(cssContent).toMatch(/--shadow-sm\s*:|--shadow-md\s*:|--shadow-lg\s*:/);
    });

    test('CSS defines max-width for content containment', () => {
      expect(cssContent).toMatch(/--max-width\s*:/);
    });

    test('container has max-width for readability', () => {
      expect(cssContent).toMatch(/\.container[^{]*\{[^}]*max-width/s);
    });
  });

  // Typography readability
  describe('Typography Readability', () => {
    test('body has base font-size set', () => {
      expect(cssContent).toMatch(/body\s*\{[\s\S]*?font-size\s*:/);
    });

    test('body has line-height set for readability', () => {
      expect(cssContent).toMatch(/body\s*\{[\s\S]*?line-height\s*:/);
    });

    test('line-height is at least 1.5 for body text', () => {
      const bodyMatch = cssContent.match(/body\s*\{[\s\S]*?line-height\s*:\s*([\d.]+)/);
      expect(bodyMatch).toBeTruthy();
      const lineHeight = parseFloat(bodyMatch[1]);
      expect(lineHeight).toBeGreaterThanOrEqual(1.5);
    });

    test('heading sizes follow appropriate hierarchy', () => {
      // Check that h1, h2, h3 have defined font sizes
      expect(cssContent).toMatch(/h1\s*\{[\s\S]*?font-size\s*:/);
      expect(cssContent).toMatch(/h2\s*\{[\s\S]*?font-size\s*:/);
      expect(cssContent).toMatch(/h3\s*\{[\s\S]*?font-size\s*:/);
    });
  });
});
