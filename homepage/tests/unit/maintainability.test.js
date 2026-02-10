/**
 * Content Maintainability Unit Tests
 * Owner: Scenario 10 - Content Maintainability
 *
 * Unit tests for validating that the MirDB homepage content is easy to maintain,
 * well-structured, and can be updated without affecting page layout.
 *
 * Requirements traced:
 * - NFR-4: Homepage shall be easy to maintain
 * - USR-6: Stakeholder wants to update homepage content
 */

const fs = require('fs');
const path = require('path');

describe('Content Maintainability', () => {
  let htmlContent;
  let cssContent;
  let responsiveCssContent;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../index.html');
    const cssPath = path.join(__dirname, '../../css/styles.css');
    const responsiveCssPath = path.join(__dirname, '../../css/responsive.css');

    htmlContent = fs.readFileSync(htmlPath, 'utf8');
    cssContent = fs.readFileSync(cssPath, 'utf8');
    responsiveCssContent = fs.readFileSync(responsiveCssPath, 'utf8');
  });

  // Test Case 1: Check for HTML comments marking sections
  describe('HTML Section Comments', () => {
    test('should have comments identifying major sections', () => {
      // Check for BEGIN/END comment markers
      const beginComments = htmlContent.match(/<!--\s*BEGIN:/g) || [];
      const endComments = htmlContent.match(/<!--\s*END:/g) || [];

      expect(beginComments.length).toBeGreaterThan(0);
      expect(endComments.length).toBeGreaterThan(0);
      expect(beginComments.length).toBe(endComments.length);
    });

    test('should have comment for Header section', () => {
      expect(htmlContent).toMatch(/<!--\s*BEGIN:\s*Header/i);
      expect(htmlContent).toMatch(/<!--\s*END:\s*Header/i);
    });

    test('should have comment for Hero section', () => {
      expect(htmlContent).toMatch(/<!--\s*BEGIN:\s*Hero/i);
      expect(htmlContent).toMatch(/<!--\s*END:\s*Hero/i);
    });

    test('should have comment for Features section', () => {
      expect(htmlContent).toMatch(/<!--\s*BEGIN:\s*Features/i);
      expect(htmlContent).toMatch(/<!--\s*END:\s*Features/i);
    });

    test('should have comment for Quick Start section', () => {
      expect(htmlContent).toMatch(/<!--\s*BEGIN:\s*Quick\s*Start/i);
      expect(htmlContent).toMatch(/<!--\s*END:\s*Quick\s*Start/i);
    });

    test('should have comment for Resources section', () => {
      expect(htmlContent).toMatch(/<!--\s*BEGIN:\s*Resources/i);
      expect(htmlContent).toMatch(/<!--\s*END:\s*Resources/i);
    });

    test('should have comment for Footer section', () => {
      expect(htmlContent).toMatch(/<!--\s*BEGIN:\s*Footer/i);
      expect(htmlContent).toMatch(/<!--\s*END:\s*Footer/i);
    });
  });

  // Test Case 2: Check for inline styles
  describe('Inline Styles', () => {
    test('should have minimal inline style attributes', () => {
      // Count inline style attributes in HTML
      const inlineStyles = htmlContent.match(/style\s*=\s*["'][^"']+["']/gi) || [];

      // Allow a small number of inline styles for dynamic purposes (e.g., JavaScript-added)
      // but should be minimal - less than 5 is acceptable
      expect(inlineStyles.length).toBeLessThan(5);
    });

    test('should prefer CSS classes over inline styles for major sections', () => {
      // Major sections should use class attributes, not inline styles
      const sectionTags = htmlContent.match(/<section[^>]*>/gi) || [];

      sectionTags.forEach(section => {
        // Check that sections have class attributes
        expect(section).toMatch(/class\s*=/i);
        // Sections should not have inline styles
        expect(section).not.toMatch(/style\s*=/i);
      });
    });

    test('should use CSS classes for layout and styling', () => {
      // Check that divs use classes instead of inline styles
      const divTags = htmlContent.match(/<div[^>]*>/gi) || [];
      const divsWithInlineStyle = divTags.filter(div => /style\s*=/i.test(div));

      // Very few divs should have inline styles
      expect(divsWithInlineStyle.length).toBeLessThan(3);
    });
  });

  // Test Case 3: Verify CSS uses classes not IDs for styling
  describe('CSS Selector Strategy', () => {
    test('should primarily use class selectors for reusability', () => {
      // Count class selectors (starting with .)
      const classSelectors = cssContent.match(/\.[a-zA-Z][a-zA-Z0-9_-]*/g) || [];
      // Count ID selectors (starting with #)
      const idSelectors = cssContent.match(/#[a-zA-Z][a-zA-Z0-9_-]*/g) || [];

      // Class selectors should significantly outnumber ID selectors
      expect(classSelectors.length).toBeGreaterThan(idSelectors.length * 3);
    });

    test('should use class selectors for common UI components', () => {
      // Check that common components use classes
      expect(cssContent).toMatch(/\.container\s*\{/);
      expect(cssContent).toMatch(/\.btn/);
      expect(cssContent).toMatch(/\.feature-card/);
      expect(cssContent).toMatch(/\.hero/);
    });

    test('should avoid ID selectors for styling (IDs should be for JS/anchors)', () => {
      // Extract all ID selectors from CSS
      const idSelectors = cssContent.match(/#[a-zA-Z][a-zA-Z0-9_-]*\s*\{/g) || [];

      // Should have very few or no ID selectors for styling
      expect(idSelectors.length).toBeLessThan(3);
    });

    test('should use reusable class patterns', () => {
      // Check for utility/reusable classes
      const hasUtilityPatterns = (
        cssContent.includes('.btn-primary') ||
        cssContent.includes('.btn-secondary') ||
        cssContent.includes('.container')
      );
      expect(hasUtilityPatterns).toBe(true);
    });
  });

  // Test Case 4: Check CSS variable usage
  describe('CSS Custom Properties (Variables)', () => {
    test('should define CSS custom properties in :root', () => {
      expect(cssContent).toMatch(/:root\s*\{[\s\S]*--[a-zA-Z]/);
    });

    test('should use custom properties for colors', () => {
      // Check for color variable definitions
      expect(cssContent).toMatch(/--color-[a-zA-Z-]+\s*:/);

      // Check for usage of color variables
      expect(cssContent).toMatch(/var\(\s*--color-/);
    });

    test('should use custom properties for spacing', () => {
      // Check for spacing variable definitions
      expect(cssContent).toMatch(/--spacing-[a-zA-Z-]+\s*:/);

      // Check for usage of spacing variables
      expect(cssContent).toMatch(/var\(\s*--spacing-/);
    });

    test('should define primary brand colors as variables', () => {
      expect(cssContent).toMatch(/--color-primary\s*:/);
      expect(cssContent).toMatch(/--color-text\s*:/);
      expect(cssContent).toMatch(/--color-background\s*:/);
    });

    test('should define font families as variables', () => {
      expect(cssContent).toMatch(/--font-family[a-zA-Z-]*\s*:/);
    });

    test('should define border radius as variable', () => {
      expect(cssContent).toMatch(/--border-radius\s*:/);
    });

    test('should have at least 5 color variables defined', () => {
      const colorVariables = cssContent.match(/--color-[a-zA-Z-]+\s*:/g) || [];
      expect(colorVariables.length).toBeGreaterThanOrEqual(5);
    });

    test('should have spacing scale variables', () => {
      const spacingVariables = cssContent.match(/--spacing-[a-zA-Z0-9-]+\s*:/g) || [];
      expect(spacingVariables.length).toBeGreaterThanOrEqual(4);
    });
  });

  // Additional maintainability tests
  describe('Code Organization', () => {
    test('should have CSS file header comment explaining purpose', () => {
      // Check for header comment in CSS
      expect(cssContent).toMatch(/\/\*[\s\S]*MirDB[\s\S]*\*\//);
    });

    test('should have descriptive CSS section comments', () => {
      // Check for section comments in CSS
      const sectionComments = cssContent.match(/\/\*[\s\S]*?\*\//g) || [];
      expect(sectionComments.length).toBeGreaterThan(3);
    });

    test('should separate styles into logical sections', () => {
      // Check that CSS has organized sections
      const hasLogicalSections = (
        cssContent.includes('/* Reset') ||
        cssContent.includes('/* Base') ||
        cssContent.includes('/* Typography') ||
        cssContent.includes('/* Header') ||
        cssContent.includes('/* Hero') ||
        cssContent.includes('/* Feature') ||
        cssContent.includes('/* Footer')
      );
      expect(hasLogicalSections).toBe(true);
    });

    test('should have consistent naming convention for CSS classes', () => {
      // Check for kebab-case naming (e.g., .feature-card, .cta-buttons)
      const classSelectors = cssContent.match(/\.[a-z][a-z0-9-]*/g) || [];

      // Most class names should follow kebab-case convention
      const kebabCaseClasses = classSelectors.filter(cls => /^\.[a-z][a-z0-9-]*$/.test(cls));
      expect(kebabCaseClasses.length / classSelectors.length).toBeGreaterThan(0.9);
    });
  });

  describe('Content Separation', () => {
    test('should have content in semantic HTML elements', () => {
      // Check for proper semantic structure
      expect(htmlContent).toMatch(/<header[^>]*>/i);
      expect(htmlContent).toMatch(/<main[^>]*>/i);
      expect(htmlContent).toMatch(/<footer[^>]*>/i);
      expect(htmlContent).toMatch(/<nav[^>]*>/i);
      expect(htmlContent).toMatch(/<section[^>]*>/i);
    });

    test('should have headings organized hierarchically', () => {
      // Check for proper heading hierarchy
      expect(htmlContent).toMatch(/<h1[^>]*>/i);
      expect(htmlContent).toMatch(/<h2[^>]*>/i);
      expect(htmlContent).toMatch(/<h3[^>]*>/i);
    });

    test('should load external CSS files (not embedded styles)', () => {
      // Check for external stylesheet links
      expect(htmlContent).toMatch(/<link[^>]*rel\s*=\s*["']stylesheet["'][^>]*href\s*=\s*["'][^"']*\.css["']/i);

      // Should not have large embedded style blocks
      const styleBlocks = htmlContent.match(/<style[^>]*>[\s\S]*?<\/style>/gi) || [];
      // Allow no or very minimal embedded styles
      expect(styleBlocks.length).toBeLessThanOrEqual(1);
    });

    test('should load external JavaScript files (not embedded scripts)', () => {
      // Check for external script links
      expect(htmlContent).toMatch(/<script[^>]*src\s*=\s*["'][^"']*\.js["'][^>]*>/i);
    });
  });
});
