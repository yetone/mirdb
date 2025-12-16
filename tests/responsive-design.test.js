/**
 * Mobile Responsive Design Tests
 * Scenario: Verify that the page is responsive and works on mobile devices 320px+ (NFR-1)
 */

const fs = require('fs');
const path = require('path');

describe('Mobile Responsive Design', () => {
  let htmlContent;
  let cssContent;
  let document;

  beforeAll(() => {
    // Load the index.html file
    const htmlPath = path.resolve(__dirname, '../index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf8');
    document = new DOMParser().parseFromString(htmlContent, 'text/html');

    // Load the styles.css file
    const cssPath = path.resolve(__dirname, '../styles.css');
    cssContent = fs.readFileSync(cssPath, 'utf8');
  });

  describe('Test Case 1: Viewport meta tag verification', () => {
    test('should have a viewport meta tag', () => {
      const viewportMeta = document.querySelector('meta[name="viewport"]');
      expect(viewportMeta).not.toBeNull();
    });

    test('viewport meta tag should contain width=device-width', () => {
      const viewportMeta = document.querySelector('meta[name="viewport"]');
      expect(viewportMeta).not.toBeNull();

      const content = viewportMeta.getAttribute('content');
      expect(content).toBeTruthy();
      expect(content).toContain('width=device-width');
    });

    test('viewport meta tag should contain initial-scale=1', () => {
      const viewportMeta = document.querySelector('meta[name="viewport"]');
      expect(viewportMeta).not.toBeNull();

      const content = viewportMeta.getAttribute('content');
      expect(content).toBeTruthy();
      expect(content).toMatch(/initial-scale\s*=\s*1(\.0)?/);
    });

    test('viewport meta tag should be in the head section', () => {
      const head = document.querySelector('head');
      expect(head).not.toBeNull();

      const viewportMeta = head.querySelector('meta[name="viewport"]');
      expect(viewportMeta).not.toBeNull();
    });
  });

  describe('Test Case 2: CSS media queries verification', () => {
    test('CSS file should exist and be non-empty', () => {
      expect(cssContent).toBeTruthy();
      expect(cssContent.length).toBeGreaterThan(0);
    });

    test('CSS should contain media queries', () => {
      // Check for @media rules
      expect(cssContent).toMatch(/@media\s*\(/);
    });

    test('CSS should have responsive breakpoint for tablets (768px)', () => {
      // Check for tablet breakpoint media query
      expect(cssContent).toMatch(/@media\s*\([^)]*max-width\s*:\s*768px[^)]*\)/);
    });

    test('CSS should have responsive breakpoint for small mobile (480px)', () => {
      // Check for small mobile breakpoint media query
      expect(cssContent).toMatch(/@media\s*\([^)]*max-width\s*:\s*480px[^)]*\)/);
    });

    test('CSS should use flexible units (%, em, rem, vw, vh)', () => {
      // Check for flexible CSS units
      const hasFlexibleUnits =
        cssContent.includes('%') ||
        cssContent.includes('em') ||
        cssContent.includes('rem') ||
        cssContent.includes('vw') ||
        cssContent.includes('vh');

      expect(hasFlexibleUnits).toBe(true);
    });

    test('CSS should use CSS Grid or Flexbox for layouts', () => {
      // Check for modern layout techniques
      const hasModernLayouts =
        cssContent.includes('display: flex') ||
        cssContent.includes('display:flex') ||
        cssContent.includes('display: grid') ||
        cssContent.includes('display:grid');

      expect(hasModernLayouts).toBe(true);
    });

    test('CSS should use box-sizing: border-box', () => {
      // Check for proper box model
      expect(cssContent).toMatch(/box-sizing\s*:\s*border-box/);
    });
  });

  describe('Test Case 3: Mobile layout at 320px width', () => {
    test('body should not have fixed pixel width', () => {
      // Check that body doesn't have a fixed width that would cause overflow
      expect(cssContent).not.toMatch(/body\s*\{[^}]*width\s*:\s*\d+px/);
    });

    test('containers should use max-width instead of fixed width', () => {
      // Check for max-width usage
      expect(cssContent).toMatch(/max-width\s*:/);
    });

    test('features grid should adapt to single column on mobile', () => {
      // Check for responsive grid that becomes single column
      // The media query should change grid columns to 1fr
      expect(cssContent).toMatch(/grid-template-columns\s*:\s*1fr\s*;?\s*\}/);
    });

    test('navigation should be responsive', () => {
      // Check for navbar responsive styles
      expect(cssContent).toMatch(/\.navbar\s*\{[^}]*flex/);
    });

    test('images should be responsive with max-width', () => {
      // Check that hero logo has responsive styling
      const hasResponsiveImage =
        cssContent.includes('max-width') &&
        (cssContent.includes('.hero-logo') || cssContent.includes('img'));

      expect(hasResponsiveImage).toBe(true);
    });
  });

  describe('Test Case 4: Tablet layout at 768px width', () => {
    test('should have styles for tablet breakpoint', () => {
      // Extract media query content for 768px
      const tabletMediaQuery = cssContent.match(/@media\s*\([^)]*max-width\s*:\s*768px[^)]*\)\s*\{[\s\S]*?\n\}/g);
      expect(tabletMediaQuery).not.toBeNull();
    });

    test('navigation should adapt for tablet', () => {
      // Check that navbar has responsive behavior
      const mediaQuerySection = cssContent.match(/@media\s*\([^)]*768px[^)]*\)[\s\S]*?(?=@media|$)/);
      expect(mediaQuerySection).not.toBeNull();
      expect(mediaQuerySection[0]).toMatch(/nav|navbar/i);
    });

    test('features grid should adapt for tablet', () => {
      // Check for grid adaptation in media query
      const mediaQuerySection = cssContent.match(/@media\s*\([^)]*768px[^)]*\)[\s\S]*?(?=@media|$)/);
      expect(mediaQuerySection).not.toBeNull();
      expect(mediaQuerySection[0]).toMatch(/features-grid|grid-template/i);
    });

    test('hero section should adapt for tablet', () => {
      // Check for hero responsive styles
      const mediaQuerySection = cssContent.match(/@media\s*\([^)]*768px[^)]*\)[\s\S]*?(?=@media|$)/);
      expect(mediaQuerySection).not.toBeNull();
      expect(mediaQuerySection[0]).toMatch(/hero/i);
    });
  });

  describe('Test Case 5: Desktop layout at 1024px+ width', () => {
    test('features section should have multi-column layout by default', () => {
      // Check for 3-column grid in features section (desktop default)
      expect(cssContent).toMatch(/\.features-grid[\s\S]*?grid-template-columns\s*:\s*repeat\s*\(\s*3/);
    });

    test('container should have max-width constraint', () => {
      // Check for max-width on container
      expect(cssContent).toMatch(/\.container[\s\S]*?max-width\s*:/);
    });

    test('content should be centered with auto margins', () => {
      // Check for centered layout
      expect(cssContent).toMatch(/margin\s*:\s*0\s+auto|margin:\s*0 auto/);
    });

    test('hero section should have comfortable padding at desktop', () => {
      // Check for hero padding
      expect(cssContent).toMatch(/\.hero[\s\S]*?padding\s*:/);
    });

    test('navigation should display horizontally at desktop', () => {
      // Check for horizontal nav layout
      expect(cssContent).toMatch(/\.nav-links[\s\S]*?display\s*:\s*flex/);
    });
  });

  describe('Additional responsive design best practices', () => {
    test('should use relative font sizes', () => {
      // Check for rem or em font sizes
      const hasRelativeFonts =
        cssContent.includes('rem') ||
        cssContent.includes('em');

      expect(hasRelativeFonts).toBe(true);
    });

    test('should have accessible focus styles', () => {
      // Check for focus styles
      expect(cssContent).toMatch(/focus/i);
    });

    test('pre/code blocks should handle overflow', () => {
      // Check for overflow handling in code blocks
      expect(cssContent).toMatch(/overflow(-x)?\s*:\s*auto|overflow(-x)?\s*:\s*scroll/);
    });

    test('buttons should have touch-friendly sizes', () => {
      // Check for button padding
      expect(cssContent).toMatch(/\.btn[\s\S]*?padding\s*:/);
    });

    test('line-height should be set for readability', () => {
      // Check for line-height
      expect(cssContent).toMatch(/line-height\s*:/);
    });
  });
});
