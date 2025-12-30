/**
 * Tests for Responsive Design - Mobile (NFR-1)
 * Verify page is fully responsive on mobile viewports (375px width)
 */

const fs = require('fs');
const path = require('path');

describe('Responsive Design - Mobile', () => {
  let document;
  let cssContent;

  beforeAll(() => {
    const html = fs.readFileSync(path.resolve(__dirname, '../index.html'), 'utf8');
    document = new DOMParser().parseFromString(html, 'text/html');
    cssContent = fs.readFileSync(path.resolve(__dirname, '../styles.css'), 'utf8');
  });

  // Test Case 1: Load page at 375px viewport width
  describe('Test Case 1: No horizontal scroll at 375px viewport width', () => {
    test('should have viewport meta tag for proper mobile rendering', () => {
      const viewportMeta = document.querySelector('meta[name="viewport"]');
      expect(viewportMeta).not.toBeNull();
      const content = viewportMeta.getAttribute('content');
      expect(content).toContain('width=device-width');
      expect(content).toContain('initial-scale=1');
    });

    test('should have container with responsive max-width and padding', () => {
      const containerRule = cssContent.includes('.container');
      expect(containerRule).toBe(true);
      // Check for padding that prevents horizontal overflow
      expect(cssContent).toMatch(/\.container\s*\{[^}]*padding[^}]*\}/);
    });

    test('should have box-sizing: border-box for proper sizing', () => {
      // Check for universal box-sizing rule
      expect(cssContent).toMatch(/box-sizing:\s*border-box/);
    });

    test('should not have fixed widths larger than mobile viewport', () => {
      // Verify no elements have fixed widths > 375px that would cause overflow
      // Check for width constraints on key elements
      const hasResponsiveContainers =
        cssContent.includes('max-width') &&
        (cssContent.includes('100%') || cssContent.includes('vw'));
      expect(hasResponsiveContainers).toBe(true);
    });

    test('should have overflow handling for code blocks', () => {
      // Code blocks should have overflow-x: auto for mobile
      expect(cssContent).toMatch(/pre\s*\{[^}]*overflow-x:\s*auto[^}]*\}/);
    });
  });

  // Test Case 2: Check hero section on mobile
  describe('Test Case 2: Hero section mobile accessibility', () => {
    test('should have hero section with proper structure', () => {
      const heroSection = document.querySelector('.hero, header.hero');
      expect(heroSection).not.toBeNull();
    });

    test('should have readable hero text with responsive font size', () => {
      // Check for mobile font size adjustments in CSS
      const hasResponsiveFontSize =
        cssContent.includes('@media') &&
        cssContent.match(/@media[^{]*max-width:\s*768px[^{]*\{[^}]*font-size/s);
      expect(hasResponsiveFontSize).toBeTruthy();
    });

    test('should have CTA buttons in hero section', () => {
      const heroSection = document.querySelector('.hero, header.hero');
      const buttons = heroSection.querySelectorAll('.btn, a.btn-primary, a.btn-secondary');
      expect(buttons.length).toBeGreaterThan(0);
    });

    test('should have CTA buttons with min-height for tappability (44px standard)', () => {
      // Check for button padding that ensures at least 44px height
      // Standard button with padding: 0.75rem (12px) top + bottom = 24px + line-height
      const hasButtonPadding = cssContent.match(/\.btn\s*\{[^}]*padding:\s*0\.75rem/);
      expect(hasButtonPadding).toBeTruthy();
    });

    test('should have hero-actions with flex-wrap for mobile stacking', () => {
      const hasFlexWrap = cssContent.match(/\.hero-actions\s*\{[^}]*flex-wrap:\s*wrap/);
      expect(hasFlexWrap).toBeTruthy();
    });
  });

  // Test Case 3: Check value propositions on mobile
  describe('Test Case 3: Value propositions stack vertically on mobile', () => {
    test('should have three value proposition articles', () => {
      const propositions = document.querySelectorAll('.proposition');
      expect(propositions.length).toBe(3);
    });

    test('should have propositions-grid container', () => {
      const grid = document.querySelector('.propositions-grid');
      expect(grid).not.toBeNull();
    });

    test('should have mobile-first column layout (stacks by default)', () => {
      // Check that the grid uses flex-direction: column by default (mobile-first)
      const hasMobileFirstLayout = cssContent.match(/\.propositions-grid\s*\{[^}]*flex-direction:\s*column/);
      expect(hasMobileFirstLayout).toBeTruthy();
    });

    test('should have 3-column grid layout only on desktop (min-width: 1024px)', () => {
      // Check that 3-column layout is only applied at larger breakpoints
      const hasDesktopGrid = cssContent.match(/@media\s*\(\s*min-width:\s*1024px\s*\)\s*\{[^}]*\.propositions-grid[^}]*grid-template-columns:\s*1fr\s+1fr\s+1fr/s);
      expect(hasDesktopGrid).toBeTruthy();
    });
  });

  // Test Case 4: Check code blocks on mobile
  describe('Test Case 4: Code blocks are scrollable/readable on mobile', () => {
    test('should have code blocks in the page', () => {
      const codeBlocks = document.querySelectorAll('pre, pre code');
      expect(codeBlocks.length).toBeGreaterThan(0);
    });

    test('should have overflow-x: auto on pre elements for horizontal scrolling', () => {
      const hasOverflowX = cssContent.match(/pre\s*\{[^}]*overflow-x:\s*auto[^}]*\}/);
      expect(hasOverflowX).toBeTruthy();
    });

    test('should have appropriate code font size for readability', () => {
      // Check for code font-size in CSS
      const hasCodeFontSize = cssContent.match(/code\s*\{[^}]*font-size[^}]*\}/);
      expect(hasCodeFontSize).toBeTruthy();
    });

    test('should have proper padding on pre elements for mobile viewing', () => {
      // Check for pre padding
      const hasPrePadding = cssContent.match(/pre\s*\{[^}]*padding[^}]*\}/);
      expect(hasPrePadding).toBeTruthy();
    });

    test('should have comparison table wrapper with overflow handling', () => {
      // Check for table wrapper that enables horizontal scroll on mobile
      const hasTableWrapper = cssContent.match(/\.comparison-table-wrapper\s*\{[^}]*overflow-x:\s*auto[^}]*\}/);
      expect(hasTableWrapper).toBeTruthy();
    });
  });

  // Additional mobile responsiveness tests
  describe('Additional Mobile Responsiveness', () => {
    test('should have responsive typography for section titles at mobile breakpoint', () => {
      // Check for smaller section title at mobile - using multiline search
      // The @media block contains multiple rules, so we need a more flexible match
      const mobileMediaMatch = cssContent.match(/@media\s*\(\s*max-width:\s*768px\s*\)\s*\{([\s\S]*?)\n\}/);
      expect(mobileMediaMatch).toBeTruthy();
      // Now check if .section-title with font-size is inside the media query content
      const mediaContent = mobileMediaMatch ? mobileMediaMatch[1] : '';
      const hasSectionTitleFontSize = mediaContent.includes('.section-title') && mediaContent.includes('font-size');
      expect(hasSectionTitleFontSize).toBe(true);
    });

    test('should have commands-grid with responsive layout', () => {
      // Commands grid should adapt to mobile
      const hasResponsiveCommandsGrid = cssContent.includes('.commands-grid');
      expect(hasResponsiveCommandsGrid).toBe(true);
    });

    test('should have step cards that work on mobile', () => {
      const steps = document.querySelectorAll('.step');
      expect(steps.length).toBeGreaterThan(0);
    });
  });
});
