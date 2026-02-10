/**
 * Brand Consistency Unit Tests
 * Owner: Scenario 9 - Brand Consistency and Visual Design
 *
 * Unit tests for validating brand consistency including logo usage,
 * consistent font definitions, and typography styles.
 *
 * Requirements traced:
 * - NFR-2: Homepage shall maintain consistency with existing project branding
 * - Design Requirements: Typography optimized for readability
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
    test('should have logo image referencing assets/logo.gif or equivalent path', () => {
      // Test Case 1: Check logo image source references assets/logo.gif or equivalent path
      const logoImgMatch = htmlContent.match(/<img[^>]*src\s*=\s*["']([^"']*logo\.gif)["'][^>]*>/i);
      expect(logoImgMatch).toBeTruthy();
      const logoSrc = logoImgMatch[1];
      // Should reference logo.gif from assets folder (may be ../assets/logo.gif or similar)
      expect(logoSrc).toMatch(/logo\.gif$/i);
    });

    test('should have logo in header section', () => {
      // Verify logo is within header for proper branding placement
      const headerMatch = htmlContent.match(/<header[^>]*>[\s\S]*?<\/header>/i);
      expect(headerMatch).toBeTruthy();
      expect(headerMatch[0]).toMatch(/<img[^>]*logo\.gif/i);
    });

    test('should have alt text for logo image', () => {
      // Ensure logo has proper alt text for accessibility and branding
      const logoImgMatch = htmlContent.match(/<img[^>]*src\s*=\s*["'][^"']*logo\.gif["'][^>]*>/i);
      expect(logoImgMatch).toBeTruthy();
      expect(logoImgMatch[0]).toMatch(/alt\s*=\s*["'][^"']+["']/i);
    });
  });

  describe('Consistent Font Usage (Test Case 3)', () => {
    test('should define a primary font family in CSS custom properties', () => {
      // Test Case 3: Page uses consistent font family throughout (defined in CSS)
      expect(cssContent).toMatch(/--font-family\s*:/);
    });

    test('should use CSS variable for body font-family', () => {
      // Ensure body uses the defined font variable for consistency
      expect(cssContent).toMatch(/body\s*\{[\s\S]*?font-family\s*:\s*var\(--font-family/);
    });

    test('should have sans-serif font family defined', () => {
      // Professional technical sites typically use sans-serif fonts
      const fontMatch = cssContent.match(/--font-family\s*:\s*([^;]+);/);
      expect(fontMatch).toBeTruthy();
      const fontValue = fontMatch[1].toLowerCase();
      expect(fontValue).toMatch(/sans-serif/);
    });

    test('should use consistent font family for headings', () => {
      // Headings should use the same font family for brand consistency
      // Check that headings don't have a direct font-family override (they should inherit from body)
      // Look for h1-h6 rules with their own font-family declarations
      const headingRules = cssContent.match(/h[1-6]\s*\{[^}]*\}/g) || [];
      const hasDirectFontOverride = headingRules.some((rule) => {
        // Check if the specific heading rule has a font-family that's not the variable
        return /font-family\s*:\s*(?!var\(--font-family)/.test(rule);
      });
      // Headings should either have no font-family (inherit) or use the variable
      expect(hasDirectFontOverride).toBe(false);
    });
  });

  describe('Monospace Font for Code (Test Case 5)', () => {
    test('should define a monospace font family variable', () => {
      // Test Case 5: Code examples use monospace font family
      expect(cssContent).toMatch(/--font-family-mono\s*:/);
    });

    test('should have monospace font defined with fallbacks', () => {
      const monoMatch = cssContent.match(/--font-family-mono\s*:\s*([^;]+);/);
      expect(monoMatch).toBeTruthy();
      const monoValue = monoMatch[1].toLowerCase();
      expect(monoValue).toMatch(/monospace/);
    });

    test('should apply monospace font to code elements', () => {
      // Code elements should use the monospace font variable
      expect(cssContent).toMatch(/code[\s\S]*?font-family\s*:\s*var\(--font-family-mono/);
    });

    test('should apply monospace font to pre elements', () => {
      // Pre elements should also use monospace for code blocks
      expect(cssContent).toMatch(/pre[\s\S]*code[\s\S]*font-family\s*:\s*var\(--font-family-mono/);
    });
  });

  describe('Color Scheme Consistency', () => {
    test('should define primary color variable', () => {
      expect(cssContent).toMatch(/--color-primary\s*:/);
    });

    test('should define secondary color variable', () => {
      expect(cssContent).toMatch(/--color-secondary\s*:/);
    });

    test('should define text color variable', () => {
      expect(cssContent).toMatch(/--color-text\s*:/);
    });

    test('should define background color variable', () => {
      expect(cssContent).toMatch(/--color-background\s*:/);
    });

    test('should use CSS custom properties throughout for consistent theming', () => {
      // Count usage of var(--color- to verify consistent variable usage
      const colorVarUsage = cssContent.match(/var\(--color-/g);
      expect(colorVarUsage).toBeTruthy();
      // Should be used multiple times for consistency
      expect(colorVarUsage.length).toBeGreaterThan(10);
    });
  });

  describe('Professional Design Elements', () => {
    test('should define spacing variables for consistent whitespace', () => {
      expect(cssContent).toMatch(/--spacing-/);
    });

    test('should define multiple spacing scale values', () => {
      // Professional design uses a spacing scale
      const spacingVars = cssContent.match(/--spacing-[a-z0-9]+\s*:/gi);
      expect(spacingVars).toBeTruthy();
      expect(spacingVars.length).toBeGreaterThanOrEqual(3);
    });

    test('should define border radius for consistent corner styling', () => {
      expect(cssContent).toMatch(/--border-radius\s*:/);
    });

    test('should define box shadows for depth', () => {
      expect(cssContent).toMatch(/--shadow-/);
    });

    test('should define max-width for content containment', () => {
      expect(cssContent).toMatch(/--max-width\s*:/);
    });
  });

  describe('Typography Readability', () => {
    test('should set base font-size on body', () => {
      expect(cssContent).toMatch(/body\s*\{[\s\S]*?font-size\s*:/);
    });

    test('should set line-height for readability', () => {
      expect(cssContent).toMatch(/body\s*\{[\s\S]*?line-height\s*:/);
    });

    test('should have line-height of at least 1.5 for body text', () => {
      const bodyMatch = cssContent.match(/body\s*\{[\s\S]*?line-height\s*:\s*([\d.]+)/);
      expect(bodyMatch).toBeTruthy();
      const lineHeight = parseFloat(bodyMatch[1]);
      expect(lineHeight).toBeGreaterThanOrEqual(1.5);
    });

    test('should define heading sizes with appropriate hierarchy', () => {
      // Check that h1, h2, h3 have defined font sizes
      expect(cssContent).toMatch(/h1\s*\{[\s\S]*?font-size\s*:/);
      expect(cssContent).toMatch(/h2\s*\{[\s\S]*?font-size\s*:/);
      expect(cssContent).toMatch(/h3\s*\{[\s\S]*?font-size\s*:/);
    });
  });
});
