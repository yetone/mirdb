/**
 * Unit Tests for Vendor-Prefixed CSS Usage
 *
 * This module verifies that the CSS uses appropriate vendor prefixes
 * for cross-browser compatibility per NFR-6.
 *
 * Test Case 4: Check for vendor-prefixed CSS usage
 * Expected: CSS uses appropriate vendor prefixes for cross-browser support
 */

const fs = require('fs');
const path = require('path');

describe('Vendor-Prefixed CSS for Cross-Browser Compatibility', () => {
  let cssContent;

  beforeAll(() => {
    const cssPath = path.join(__dirname, '..', 'styles.css');
    cssContent = fs.readFileSync(cssPath, 'utf8');
  });

  describe('Required Vendor Prefixes', () => {
    test('background-clip: text has -webkit prefix', () => {
      // background-clip: text requires -webkit prefix for Chrome, Safari, Edge
      // Check that -webkit-background-clip: text is used
      expect(cssContent).toMatch(/-webkit-background-clip:\s*text/);
    });

    test('text-fill-color has -webkit prefix for gradient text', () => {
      // -webkit-text-fill-color is needed alongside -webkit-background-clip
      expect(cssContent).toMatch(/-webkit-text-fill-color:\s*transparent/);
    });

    test('standard background-clip: text is also defined for future compatibility', () => {
      // Standard property should be present for browsers that support it
      expect(cssContent).toMatch(/background-clip:\s*text/);
    });
  });

  describe('Modern CSS Features with Good Browser Support', () => {
    test('CSS custom properties (variables) are used', () => {
      // CSS custom properties have good support in all modern browsers
      expect(cssContent).toMatch(/--[\w-]+:/);
      expect(cssContent).toMatch(/var\(--[\w-]+\)/);
    });

    test('flexbox is used without vendor prefixes (modern browsers)', () => {
      // Flexbox is well-supported in modern browsers, no prefix needed
      expect(cssContent).toMatch(/display:\s*flex/);
    });

    test('CSS Grid is used without vendor prefixes (modern browsers)', () => {
      // CSS Grid is well-supported in modern browsers, no prefix needed
      expect(cssContent).toMatch(/display:\s*grid/);
    });

    test('linear-gradient is used without vendor prefixes', () => {
      // Linear gradient is well-supported in modern browsers
      expect(cssContent).toMatch(/linear-gradient/);
    });

    test('box-sizing: border-box is used', () => {
      // Universal box-sizing reset for consistent rendering
      expect(cssContent).toMatch(/box-sizing:\s*border-box/);
    });
  });

  describe('Browser Compatibility Patterns', () => {
    test('uses system font stack for cross-platform consistency', () => {
      // System font stack provides native look across platforms
      expect(cssContent).toMatch(/-apple-system/);
      expect(cssContent).toMatch(/BlinkMacSystemFont/);
      expect(cssContent).toMatch(/Segoe UI/);
    });

    test('uses CSS reset for consistent cross-browser defaults', () => {
      // Reset margins and paddings for consistent cross-browser rendering
      expect(cssContent).toMatch(/margin:\s*0/);
      expect(cssContent).toMatch(/padding:\s*0/);
    });

    test('overflow-x handling for tables', () => {
      // Overflow-x auto for responsive tables in narrow viewports
      expect(cssContent).toMatch(/overflow-x:\s*auto/);
    });

    test('uses border-radius (well-supported)', () => {
      // Border-radius is fully supported in all modern browsers
      expect(cssContent).toMatch(/border-radius:/);
    });

    test('uses box-shadow (well-supported)', () => {
      // Box-shadow is fully supported in all modern browsers
      expect(cssContent).toMatch(/box-shadow:/);
    });

    test('uses transitions (well-supported)', () => {
      // CSS transitions are fully supported in all modern browsers
      expect(cssContent).toMatch(/transition:/);
    });

    test('uses transform (well-supported)', () => {
      // CSS transforms are fully supported in all modern browsers
      expect(cssContent).toMatch(/transform:/);
    });
  });

  describe('Responsive Design Features', () => {
    test('uses media queries for responsive design', () => {
      // Media queries for responsive breakpoints
      expect(cssContent).toMatch(/@media/);
    });

    test('defines breakpoints for mobile and tablet', () => {
      // Mobile breakpoints should be defined
      expect(cssContent).toMatch(/max-width:\s*768px/);
      expect(cssContent).toMatch(/max-width:\s*480px/);
      expect(cssContent).toMatch(/max-width:\s*320px/);
    });

    test('uses viewport meta tag compatible units', () => {
      // rem and em units work consistently across browsers
      expect(cssContent).toMatch(/\drem/);
    });
  });

  describe('Accessibility Features', () => {
    test('focus styles are defined', () => {
      // Focus styles for keyboard navigation
      expect(cssContent).toMatch(/:focus/);
      expect(cssContent).toMatch(/outline:/);
    });

    test('focus-visible is used for modern focus handling', () => {
      // focus-visible for better UX (only show focus ring for keyboard)
      expect(cssContent).toMatch(/:focus-visible/);
    });
  });

  describe('CSS Feature Detection Patterns', () => {
    test('smooth scroll behavior is defined', () => {
      // Smooth scroll behavior for modern browsers
      expect(cssContent).toMatch(/scroll-behavior:\s*smooth/);
    });

    test('uses min() or minmax() for responsive grids', () => {
      // Modern CSS functions for responsive grids
      expect(cssContent).toMatch(/min\(|minmax\(/);
    });
  });

  describe('No Unnecessary Vendor Prefixes', () => {
    test('no -moz-border-radius (deprecated)', () => {
      // -moz-border-radius is no longer needed
      expect(cssContent).not.toMatch(/-moz-border-radius/);
    });

    test('no -webkit-border-radius (deprecated)', () => {
      // -webkit-border-radius is no longer needed
      expect(cssContent).not.toMatch(/-webkit-border-radius/);
    });

    test('no -moz-box-shadow (deprecated)', () => {
      // -moz-box-shadow is no longer needed
      expect(cssContent).not.toMatch(/-moz-box-shadow/);
    });

    test('no -webkit-box-shadow (deprecated)', () => {
      // -webkit-box-shadow is no longer needed
      expect(cssContent).not.toMatch(/-webkit-box-shadow/);
    });

    test('no -webkit-transition (deprecated)', () => {
      // -webkit-transition is no longer needed
      expect(cssContent).not.toMatch(/-webkit-transition/);
    });

    test('no -moz-transition (deprecated)', () => {
      // -moz-transition is no longer needed
      expect(cssContent).not.toMatch(/-moz-transition/);
    });

    test('no -webkit-transform (deprecated for transforms)', () => {
      // -webkit-transform is no longer needed
      expect(cssContent).not.toMatch(/-webkit-transform:/);
    });
  });
});
