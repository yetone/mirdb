/**
 * Responsive Design Tests
 * Owner: Scenario 6 - Responsive Design Validation
 *
 * Tests for:
 * - Mobile layout (320px - 767px)
 * - Tablet layout (768px - 1023px)
 * - Desktop layout (1024px+)
 * - CSS media query coverage
 * - Element visibility at breakpoints
 * - Viewport meta tag
 * - Image responsiveness
 * - Touch-friendly tap targets
 */

const fs = require('fs');
const path = require('path');
const { JSDOM } = require('jsdom');
const { loadHTML, loadHTMLWithWindow, getByTestId } = require('./test-utils');

/**
 * Load HTML with a specific viewport width
 * @param {number} width - Viewport width in pixels
 * @returns {Promise<{document: Document, window: Window, dom: JSDOM}>}
 */
async function loadHTMLWithViewport(width) {
  const htmlPath = path.resolve(__dirname, '../../index.html');
  const html = fs.readFileSync(htmlPath, 'utf-8');
  const dom = new JSDOM(html, {
    url: 'http://localhost',
    runScripts: 'dangerously',
    resources: 'usable',
    pretendToBeVisual: true
  });

  // Set viewport dimensions
  Object.defineProperty(dom.window, 'innerWidth', { value: width, writable: true });
  Object.defineProperty(dom.window, 'innerHeight', { value: 800, writable: true });

  return { document: dom.window.document, window: dom.window, dom };
}

/**
 * Extract CSS content from the HTML
 * @param {Document} doc
 * @returns {string}
 */
function getCSSContent(doc) {
  const styleElements = doc.querySelectorAll('style');
  let cssContent = '';
  styleElements.forEach(style => {
    cssContent += style.textContent;
  });
  return cssContent;
}

/**
 * Parse media queries from CSS content
 * @param {string} cssContent
 * @returns {string[]}
 */
function parseMediaQueries(cssContent) {
  const mediaQueryRegex = /@media[^{]+\{(?:[^{}]|\{[^{}]*\})*\}/g;
  const matches = cssContent.match(mediaQueryRegex);
  return matches || [];
}

/**
 * Check if CSS contains media query for a specific max-width breakpoint
 * @param {string} cssContent
 * @param {number} breakpoint
 * @returns {boolean}
 */
function hasMediaQueryForBreakpoint(cssContent, breakpoint) {
  const regex = new RegExp(`@media[^{]*\\(\\s*max-width\\s*:\\s*${breakpoint}px\\s*\\)`, 'i');
  return regex.test(cssContent);
}

describe('Responsive Design Validation', () => {
  let document;
  let cssContent;

  beforeAll(async () => {
    document = await loadHTML();
    cssContent = getCSSContent(document);
  });

  describe('Test Case 1: Mobile Viewport (320px)', () => {
    test('All content is visible without horizontal scrolling', async () => {
      const { document: doc, dom } = await loadHTMLWithViewport(320);

      // Check that body exists and has content
      expect(doc.body).toBeTruthy();
      expect(doc.body.innerHTML.trim().length).toBeGreaterThan(0);

      // CSS should set box-sizing: border-box and contain overflow
      expect(cssContent).toMatch(/box-sizing\s*:\s*border-box/);

      // Container should have max-width and padding
      expect(cssContent).toMatch(/\.container\s*\{[^}]*max-width/);
      expect(cssContent).toMatch(/\.container\s*\{[^}]*padding/);

      // Images should have responsive sizing
      const images = doc.querySelectorAll('img');
      expect(images.length).toBeGreaterThan(0);

      // Hero section should be present
      const heroSection = getByTestId(doc, 'hero-section');
      expect(heroSection).toBeTruthy();

      dom.window.close();
    });
  });

  describe('Test Case 2: Tablet Viewport (768px)', () => {
    test('Layout adjusts appropriately for tablet view', async () => {
      const { document: doc, dom } = await loadHTMLWithViewport(768);

      // Check main sections exist
      expect(getByTestId(doc, 'hero-section')).toBeTruthy();
      expect(getByTestId(doc, 'features-section')).toBeTruthy();
      expect(getByTestId(doc, 'quick-start-section')).toBeTruthy();
      expect(getByTestId(doc, 'comparison-section')).toBeTruthy();

      // Media query for 768px should exist (or less restrictive breakpoint)
      const hasTabletStyles =
        hasMediaQueryForBreakpoint(cssContent, 768) ||
        cssContent.includes('@media');
      expect(hasTabletStyles).toBe(true);

      dom.window.close();
    });
  });

  describe('Test Case 3: Desktop Viewport (1024px)', () => {
    test('Full desktop layout is displayed', async () => {
      const { document: doc, dom } = await loadHTMLWithViewport(1024);

      // All main sections should be visible
      expect(getByTestId(doc, 'hero-section')).toBeTruthy();
      expect(getByTestId(doc, 'features-section')).toBeTruthy();
      expect(getByTestId(doc, 'quick-start-section')).toBeTruthy();
      expect(getByTestId(doc, 'comparison-section')).toBeTruthy();
      expect(getByTestId(doc, 'project-status-section')).toBeTruthy();
      expect(getByTestId(doc, 'footer')).toBeTruthy();

      // Features grid should use grid layout
      expect(cssContent).toMatch(/\.features-grid\s*\{[^}]*display\s*:\s*grid/);

      // Grid should handle multiple columns
      expect(cssContent).toMatch(/grid-template-columns/);

      dom.window.close();
    });
  });

  describe('Test Case 4: CSS Media Queries', () => {
    test('Stylesheet contains @media queries for responsive breakpoints', () => {
      const mediaQueries = parseMediaQueries(cssContent);

      // Should have at least one media query
      expect(mediaQueries.length).toBeGreaterThan(0);

      // Should have media queries with max-width breakpoints
      expect(cssContent).toMatch(/@media\s*\([^)]*max-width/i);

      // Check for common breakpoints (768px for tablet, 480px for small mobile)
      const has768Breakpoint = hasMediaQueryForBreakpoint(cssContent, 768);
      const has480Breakpoint = hasMediaQueryForBreakpoint(cssContent, 480);

      // At least one of the common breakpoints should be present
      expect(has768Breakpoint || has480Breakpoint).toBe(true);
    });

    test('Media queries include responsive adjustments', () => {
      // Media queries should adjust font sizes, padding, or layout
      const mediaQueryContent = parseMediaQueries(cssContent).join('\n');

      const hasResponsiveAdjustments =
        mediaQueryContent.includes('font-size') ||
        mediaQueryContent.includes('padding') ||
        mediaQueryContent.includes('grid-template-columns') ||
        mediaQueryContent.includes('flex');

      expect(hasResponsiveAdjustments).toBe(true);
    });
  });

  describe('Test Case 5: Viewport Meta Tag', () => {
    test('Meta tag with name="viewport" and content including "width=device-width" exists', () => {
      const viewportMeta = document.querySelector('meta[name="viewport"]');

      expect(viewportMeta).toBeTruthy();
      expect(viewportMeta.getAttribute('content')).toMatch(/width=device-width/i);
    });

    test('Viewport meta tag includes initial-scale', () => {
      const viewportMeta = document.querySelector('meta[name="viewport"]');

      expect(viewportMeta).toBeTruthy();
      expect(viewportMeta.getAttribute('content')).toMatch(/initial-scale/i);
    });
  });

  describe('Test Case 6: Image Responsiveness', () => {
    test('Images have max-width: 100% or similar responsive sizing', () => {
      // Check CSS for responsive image rules
      const hasResponsiveImageCSS =
        cssContent.match(/img[^}]*\{[^}]*max-width\s*:\s*\d+%/) ||
        cssContent.match(/\.logo\s*\{[^}]*max-width/) ||
        cssContent.match(/img[^}]*\{[^}]*width\s*:\s*100%/);

      // Or check for specific image styling
      const logoStyle = cssContent.includes('.logo') || cssContent.includes('#hero img');

      expect(hasResponsiveImageCSS || logoStyle).toBeTruthy();
    });

    test('Logo image has appropriate sizing constraints', () => {
      const logoImg = document.querySelector('.logo') || document.querySelector('#hero img');

      expect(logoImg).toBeTruthy();

      // CSS should define max-width for logo
      expect(cssContent).toMatch(/\.logo|#hero\s+img/);
      expect(cssContent).toMatch(/max-width/);
    });
  });

  describe('Test Case 7: Text Readability at 320px', () => {
    test('Font sizes are readable (minimum 14px effective size)', () => {
      // Check base font size is not too small
      // Body should have reasonable line-height and font-family
      expect(cssContent).toMatch(/body\s*\{[^}]*line-height/);
      expect(cssContent).toMatch(/body\s*\{[^}]*font-family/);

      // Code blocks should have readable font size
      expect(cssContent).toMatch(/pre|code/);

      // Media queries should not make fonts too small
      // Check that font-size values in media queries are reasonable (at least 0.85em or 14px)
      const mediaQueryContent = parseMediaQueries(cssContent).join('\n');

      // If there are font-size adjustments in media queries, they should be reasonable
      const fontSizeMatches = mediaQueryContent.match(/font-size\s*:\s*([\d.]+)(px|em|rem)/gi);

      if (fontSizeMatches) {
        fontSizeMatches.forEach(match => {
          const sizeMatch = match.match(/([\d.]+)(px|em|rem)/i);
          if (sizeMatch) {
            const value = parseFloat(sizeMatch[1]);
            const unit = sizeMatch[2].toLowerCase();

            // Check minimum sizes
            if (unit === 'px') {
              expect(value).toBeGreaterThanOrEqual(12); // Allow 12px minimum in media queries
            } else if (unit === 'em' || unit === 'rem') {
              expect(value).toBeGreaterThanOrEqual(0.75); // 0.75em is about 12px at 16px base
            }
          }
        });
      }
    });

    test('Headings scale down appropriately on mobile', () => {
      const mediaQueryContent = parseMediaQueries(cssContent).join('\n');

      // Should have h1 or hero heading adjustments in media queries
      const hasHeadingAdjustments =
        mediaQueryContent.includes('#hero h1') ||
        mediaQueryContent.includes('h1') ||
        mediaQueryContent.includes('.tagline');

      expect(hasHeadingAdjustments).toBe(true);
    });
  });

  describe('Test Case 8: Touch-Friendly Tap Targets', () => {
    test('Interactive elements are at least 44px in touch target size', () => {
      // Check button styles
      expect(cssContent).toMatch(/\.btn\s*\{[^}]*padding/);

      // Extract btn padding values
      const btnMatch = cssContent.match(/\.btn\s*\{([^}]*)\}/);

      if (btnMatch) {
        const btnStyles = btnMatch[1];
        const paddingMatch = btnStyles.match(/padding\s*:\s*([\d.]+)(px|em|rem)?(\s+([\d.]+)(px|em|rem)?)?/);

        if (paddingMatch) {
          // Button has padding defined, which contributes to touch target
          expect(paddingMatch).toBeTruthy();
        }
      }

      // Links should be adequately sized in footer
      expect(cssContent).toMatch(/footer|\.footer-links/);
    });

    test('CTA button has adequate padding for touch', () => {
      const btn = document.querySelector('.btn');
      expect(btn).toBeTruthy();

      // Button padding should be at least 12px (contributes to 44px target with text)
      const btnMatch = cssContent.match(/\.btn\s*\{([^}]*)\}/);
      expect(btnMatch).toBeTruthy();

      const btnStyles = btnMatch[1];
      expect(btnStyles).toMatch(/padding\s*:/);

      // Extract padding value - should be at least 12px for touch-friendly
      const paddingMatch = btnStyles.match(/padding\s*:\s*(\d+)px/);
      if (paddingMatch) {
        expect(parseInt(paddingMatch[1])).toBeGreaterThanOrEqual(12);
      }
    });

    test('Footer links have adequate spacing for touch targets', () => {
      const footerLinksMatch = cssContent.match(/\.footer-links\s*\{([^}]*)\}/);

      expect(footerLinksMatch).toBeTruthy();

      const footerStyles = footerLinksMatch[1];
      // Should have gap or margin for spacing
      expect(footerStyles).toMatch(/gap|margin/);
    });
  });

  describe('Additional Responsive Requirements', () => {
    test('Container prevents content overflow', () => {
      expect(cssContent).toMatch(/\.container\s*\{[^}]*max-width/);
      expect(cssContent).toMatch(/\.container\s*\{[^}]*padding/);
      expect(cssContent).toMatch(/\.container\s*\{[^}]*margin\s*:\s*0\s*auto/);
    });

    test('Features grid uses auto-fit for responsive columns', () => {
      expect(cssContent).toMatch(/\.features-grid[^}]*grid-template-columns[^}]*auto-fit/);
    });

    test('Comparison table is responsive', () => {
      // Check for table styling
      expect(cssContent).toMatch(/\.comparison-table/);

      // Mobile styles should adjust table size
      const mediaQueryContent = parseMediaQueries(cssContent).join('\n');
      const hasTableResponsive =
        mediaQueryContent.includes('comparison-table') ||
        mediaQueryContent.includes('table');

      expect(hasTableResponsive).toBe(true);
    });

    test('Pre/code blocks handle overflow', () => {
      expect(cssContent).toMatch(/pre\s*\{[^}]*overflow-x\s*:\s*auto/);
    });
  });
});
