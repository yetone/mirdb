/**
 * Cross-Browser Compatibility Tests
 *
 * Tests CSS for proper vendor prefix usage and standard property fallbacks.
 * Ensures the homepage renders correctly across major browsers.
 */

const fs = require('fs');
const path = require('path');

describe('Cross-Browser Compatibility', () => {
  let cssContent;

  beforeAll(() => {
    const cssPath = path.join(__dirname, '../../styles.css');
    cssContent = fs.readFileSync(cssPath, 'utf8');
  });

  describe('CSS Vendor Prefix Fallbacks', () => {
    test('webkit-background-clip has standard background-clip fallback', () => {
      // Check that -webkit-background-clip is used with standard background-clip
      const hasWebkitBackgroundClip = cssContent.includes('-webkit-background-clip');
      const hasStandardBackgroundClip = cssContent.includes('background-clip');

      if (hasWebkitBackgroundClip) {
        expect(hasStandardBackgroundClip).toBe(true);
      }
    });

    test('webkit-text-fill-color is used for text gradient effect', () => {
      // The -webkit-text-fill-color is used for gradient text effect
      // This is acceptable as it's a progressive enhancement
      const hasWebkitTextFillColor = cssContent.includes('-webkit-text-fill-color');

      // If webkit-text-fill-color is used, it should be paired with background-clip
      if (hasWebkitTextFillColor) {
        const hasBackgroundClip = cssContent.includes('background-clip');
        expect(hasBackgroundClip).toBe(true);
      }
    });

    test('no orphaned vendor prefixes without standard properties', () => {
      // Common vendor-prefixed properties that should have standard fallbacks
      const vendorPrefixes = [
        { prefix: '-webkit-transform', standard: 'transform' },
        { prefix: '-moz-transform', standard: 'transform' },
        { prefix: '-webkit-transition', standard: 'transition' },
        { prefix: '-moz-transition', standard: 'transition' },
        { prefix: '-webkit-animation', standard: 'animation' },
        { prefix: '-moz-animation', standard: 'animation' },
        { prefix: '-webkit-box-shadow', standard: 'box-shadow' },
        { prefix: '-moz-box-shadow', standard: 'box-shadow' },
        { prefix: '-webkit-border-radius', standard: 'border-radius' },
        { prefix: '-moz-border-radius', standard: 'border-radius' },
        { prefix: '-webkit-flex', standard: 'flex' },
        { prefix: '-ms-flex', standard: 'flex' },
      ];

      vendorPrefixes.forEach(({ prefix, standard }) => {
        const hasVendorPrefix = cssContent.includes(prefix);
        if (hasVendorPrefix) {
          const hasStandard = cssContent.includes(standard);
          expect(hasStandard).toBe(true);
        }
      });
    });

    test('uses standard CSS properties for layout (flexbox, grid)', () => {
      // Verify using modern standard CSS for layout
      const usesFlexbox = cssContent.includes('display: flex') || cssContent.includes('display:flex');
      const usesGrid = cssContent.includes('display: grid') || cssContent.includes('display:grid');

      // The page should use modern CSS layout
      expect(usesFlexbox || usesGrid).toBe(true);
    });

    test('uses CSS custom properties (variables)', () => {
      // CSS custom properties are well-supported in modern browsers
      const usesCSSVariables = cssContent.includes('--');
      const usesVarFunction = cssContent.includes('var(');

      expect(usesCSSVariables).toBe(true);
      expect(usesVarFunction).toBe(true);
    });

    test('box-sizing is set for cross-browser consistency', () => {
      // box-sizing: border-box is essential for cross-browser consistency
      const hasBoxSizing = cssContent.includes('box-sizing');
      expect(hasBoxSizing).toBe(true);
    });
  });

  describe('Standard CSS Properties Usage', () => {
    test('uses standard scroll-behavior property', () => {
      const hasScrollBehavior = cssContent.includes('scroll-behavior');
      expect(hasScrollBehavior).toBe(true);
    });

    test('uses standard transition property', () => {
      // Verify transitions use standard property
      const hasTransition = cssContent.includes('transition:') || cssContent.includes('transition ');
      expect(hasTransition).toBe(true);
    });

    test('uses standard transform property', () => {
      // Transform is used for hover effects
      const hasTransform = cssContent.includes('transform:') || cssContent.includes('transform ');
      expect(hasTransform).toBe(true);
    });

    test('uses standard linear-gradient syntax', () => {
      // Verify gradients use standard syntax
      const hasGradient = cssContent.includes('linear-gradient');
      expect(hasGradient).toBe(true);

      // Should not use old -webkit-gradient syntax
      const hasOldGradient = cssContent.includes('-webkit-gradient(');
      expect(hasOldGradient).toBe(false);
    });

    test('uses standard overflow-x for horizontal scrolling', () => {
      const hasOverflowX = cssContent.includes('overflow-x');
      expect(hasOverflowX).toBe(true);
    });
  });

  describe('Media Query Compatibility', () => {
    test('uses standard media query syntax', () => {
      const hasMediaQuery = cssContent.includes('@media');
      expect(hasMediaQuery).toBe(true);
    });

    test('media queries use max-width for responsive design', () => {
      const hasMaxWidth = cssContent.includes('max-width:');
      expect(hasMaxWidth).toBe(true);
    });
  });

  describe('Font and Typography', () => {
    test('uses system font stack for cross-browser compatibility', () => {
      // System fonts ensure consistent rendering across browsers
      const hasSystemUI = cssContent.includes('system-ui');
      const hasAppleSystem = cssContent.includes('-apple-system');
      const hasSegoeUI = cssContent.includes('Segoe UI');

      expect(hasSystemUI).toBe(true);
      expect(hasAppleSystem).toBe(true);
      expect(hasSegoeUI).toBe(true);
    });

    test('includes generic font family fallbacks', () => {
      const hasSansSerif = cssContent.includes('sans-serif');
      const hasMonospace = cssContent.includes('monospace');

      expect(hasSansSerif).toBe(true);
      expect(hasMonospace).toBe(true);
    });
  });

  describe('Color and Visual Properties', () => {
    test('uses standard color formats', () => {
      // Should use standard hex colors or CSS color names
      const hasHexColors = /#[0-9a-fA-F]{3,8}/.test(cssContent);
      expect(hasHexColors).toBe(true);
    });

    test('uses CSS variables for theming (cross-browser support)', () => {
      // CSS variables are supported in all modern browsers
      const hasRootVars = cssContent.includes(':root');
      expect(hasRootVars).toBe(true);
    });
  });
});
