/**
 * Mobile Responsive Design Tests
 * Scenario: Verify that the page is responsive and works on mobile devices 320px+ (NFR-1)
 */

const fs = require('fs');
const path = require('path');

describe('Mobile Responsive Design', () => {
  let html;
  let css;
  let document;

  beforeAll(() => {
    // Load the HTML file
    const htmlPath = path.resolve(__dirname, '../index.html');
    html = fs.readFileSync(htmlPath, 'utf8');
    document = new DOMParser().parseFromString(html, 'text/html');

    // Load the CSS file
    const cssPath = path.resolve(__dirname, '../styles.css');
    css = fs.readFileSync(cssPath, 'utf8');
  });

  // Test Case 1: Viewport meta tag
  describe('Test Case 1: Viewport Meta Tag', () => {
    test('should have a meta tag with name="viewport"', () => {
      const viewportMeta = document.querySelector('meta[name="viewport"]');
      expect(viewportMeta).not.toBeNull();
    });

    test('viewport meta tag should contain "width=device-width"', () => {
      const viewportMeta = document.querySelector('meta[name="viewport"]');
      expect(viewportMeta).not.toBeNull();

      const content = viewportMeta.getAttribute('content');
      expect(content).toBeTruthy();
      expect(content).toContain('width=device-width');
    });

    test('viewport meta tag should contain "initial-scale=1"', () => {
      const viewportMeta = document.querySelector('meta[name="viewport"]');
      expect(viewportMeta).not.toBeNull();

      const content = viewportMeta.getAttribute('content');
      expect(content).toBeTruthy();
      expect(content).toMatch(/initial-scale\s*=\s*1/);
    });
  });

  // Test Case 2: CSS Media Queries
  describe('Test Case 2: CSS Media Queries', () => {
    test('CSS should contain media queries for responsive breakpoints', () => {
      // Check for any media query
      expect(css).toMatch(/@media/);
    });

    test('CSS should have media query for mobile/tablet breakpoint (max-width: 768px)', () => {
      // Check for 768px breakpoint (common tablet/mobile threshold)
      expect(css).toMatch(/@media\s*\(\s*max-width\s*:\s*768px\s*\)/);
    });

    test('CSS should have media query for small mobile breakpoint (max-width: 480px or similar)', () => {
      // Check for smaller mobile breakpoint
      expect(css).toMatch(/@media\s*\(\s*max-width\s*:\s*(480|320|375|360)px\s*\)/);
    });

    test('CSS media queries should contain responsive adjustments for navigation', () => {
      // Extract media query contents
      const mediaQueryMatches = css.match(/@media[^{]+\{([^{}]*\{[^{}]*\})*[^{}]*\}/g) || [];
      const mediaQueryContent = mediaQueryMatches.join(' ').toLowerCase();

      // Check for navbar responsive adjustments
      expect(mediaQueryContent).toMatch(/\.navbar|\.nav-links|nav/);
    });

    test('CSS media queries should contain responsive adjustments for features grid', () => {
      const mediaQueryMatches = css.match(/@media[^{]+\{([^{}]*\{[^{}]*\})*[^{}]*\}/g) || [];
      const mediaQueryContent = mediaQueryMatches.join(' ').toLowerCase();

      // Check for features grid responsive adjustments
      expect(mediaQueryContent).toMatch(/\.features-grid|\.feature-card|features/);
    });
  });

  // Test Case 3: Mobile Layout (320px)
  describe('Test Case 3: Mobile Layout at 320px', () => {
    test('CSS should support single-column layout for features at mobile widths', () => {
      // Check that features-grid is changed to single column at mobile breakpoints
      const mediaQueryMatches = css.match(/@media[^{]+\{([^{}]*\{[^{}]*\})*[^{}]*\}/g) || [];
      const mediaQueryContent = mediaQueryMatches.join(' ');

      // Look for grid-template-columns: 1fr pattern in media queries
      expect(mediaQueryContent).toMatch(/grid-template-columns\s*:\s*1fr/);
    });

    test('body should not have fixed width that would cause overflow', () => {
      // Check that body or main container doesn't have fixed widths
      const bodyStyle = css.match(/body\s*\{[^}]*\}/);
      if (bodyStyle) {
        // Should not have fixed width
        expect(bodyStyle[0]).not.toMatch(/width\s*:\s*\d{4,}px/);
      }
    });

    test('container should use max-width not fixed width', () => {
      // Check for .container styles
      const containerMatch = css.match(/\.container\s*\{[^}]*\}/g);
      if (containerMatch) {
        const containerStyles = containerMatch.join(' ');
        // Should have max-width, not fixed width
        expect(containerStyles).toMatch(/max-width/);
      }
    });

    test('images should have max-width to prevent overflow', () => {
      // Check hero-logo has flexible sizing
      const heroLogoMatch = css.match(/\.hero-logo\s*\{[^}]*\}/);
      if (heroLogoMatch) {
        // Logo should have width constraints
        expect(heroLogoMatch[0]).toMatch(/width/);
      }
    });

    test('CSS should use box-sizing: border-box', () => {
      // Check for box-sizing reset
      expect(css).toMatch(/box-sizing\s*:\s*border-box/);
    });
  });

  // Test Case 4: Tablet Layout (768px)
  describe('Test Case 4: Tablet Layout at 768px', () => {
    test('navigation should adapt for tablet size (flex-wrap or column)', () => {
      const mediaQueryMatches = css.match(/@media[^{]+\{([^{}]*\{[^{}]*\})*[^{}]*\}/g) || [];
      const mediaQueryContent = mediaQueryMatches.join(' ');

      // Should have adjustments for navbar layout
      expect(mediaQueryContent).toMatch(/\.navbar\s*\{[^}]*(flex-direction|flex-wrap)[^}]*\}/);
    });

    test('hero section should have reduced padding at tablet size', () => {
      const mediaQueryMatches = css.match(/@media[^{]+\{([^{}]*\{[^{}]*\})*[^{}]*\}/g) || [];
      const mediaQueryContent = mediaQueryMatches.join(' ');

      // Should have hero padding adjustments
      expect(mediaQueryContent).toMatch(/\.hero\s*\{[^}]*padding[^}]*\}/);
    });

    test('font sizes should scale down at tablet breakpoints', () => {
      const mediaQueryMatches = css.match(/@media[^{]+\{([^{}]*\{[^{}]*\})*[^{}]*\}/g) || [];
      const mediaQueryContent = mediaQueryMatches.join(' ');

      // Should have font-size adjustments
      expect(mediaQueryContent).toMatch(/font-size/);
    });

    test('commands grid should be responsive', () => {
      // Check that commands-grid uses auto-fill or responsive columns
      expect(css).toMatch(/\.commands-grid\s*\{[^}]*auto-fill|\.commands-grid\s*\{[^}]*auto-fit/);
    });
  });

  // Test Case 5: Desktop Layout (1024px+)
  describe('Test Case 5: Desktop Layout at 1024px+', () => {
    test('features grid should have 3-column layout at desktop', () => {
      // Check base features-grid (outside media queries) has 3 columns
      // First, get the base CSS without media queries
      const cssWithoutMediaQueries = css.replace(/@media[^{]+\{([^{}]*\{[^{}]*\})*[^{}]*\}/g, '');

      // Should have 3-column grid
      expect(cssWithoutMediaQueries).toMatch(/\.features-grid\s*\{[^}]*grid-template-columns\s*:\s*repeat\s*\(\s*3/);
    });

    test('container should have max-width for desktop', () => {
      // Check container has max-width constraint
      expect(css).toMatch(/\.container\s*\{[^}]*max-width\s*:\s*1200px/);
    });

    test('navigation should be horizontal at desktop', () => {
      // Check nav-links display flex with row direction (or no column direction in base)
      const cssWithoutMediaQueries = css.replace(/@media[^{]+\{([^{}]*\{[^{}]*\})*[^{}]*\}/g, '');
      const navLinksMatch = cssWithoutMediaQueries.match(/\.nav-links\s*\{[^}]*\}/);

      if (navLinksMatch) {
        // Should be flex display for horizontal layout
        expect(navLinksMatch[0]).toMatch(/display\s*:\s*flex/);
      }
    });
  });

  // Additional responsive checks
  describe('Additional Responsive Checks', () => {
    test('overflow-x should be handled to prevent horizontal scrolling', () => {
      // Check for overflow-x auto on code blocks
      expect(css).toMatch(/overflow-x\s*:\s*auto/);
    });

    test('pre/code elements should handle overflow', () => {
      const preMatch = css.match(/pre\s*\{[^}]*\}/);
      if (preMatch) {
        expect(preMatch[0]).toMatch(/overflow/);
      }
    });

    test('buttons should be responsive with flex-wrap', () => {
      const heroCtaMatch = css.match(/\.hero-cta\s*\{[^}]*\}/);
      if (heroCtaMatch) {
        expect(heroCtaMatch[0]).toMatch(/flex|wrap/);
      }
    });

    test('tables should be contained within viewport', () => {
      // Response table should have max-width or be inside a container
      const tableMatch = css.match(/\.response-table\s*\{[^}]*\}/);
      if (tableMatch) {
        expect(tableMatch[0]).toMatch(/width|max-width/);
      }
    });
  });
});
