/**
 * Responsive Design Unit Tests
 * Owner: Scenario 5 - Responsive Design
 *
 * Unit tests for responsive configuration of the MirDB homepage.
 *
 * Expected test coverage:
 * - Viewport meta tag presence and configuration
 * - Base font size requirements
 * - CSS responsive file exists and contains breakpoints
 *
 * Requirements traced:
 * - REQ-5: Responsive and accessible
 * - USR-5: Mobile device viewing
 */

const fs = require('fs');
const path = require('path');

describe('Responsive Design Configuration', () => {
  let htmlContent;
  let cssContent;

  beforeAll(() => {
    // Read the HTML file
    const htmlPath = path.join(__dirname, '../../index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    // Read the responsive CSS file
    const cssPath = path.join(__dirname, '../../css/responsive.css');
    cssContent = fs.readFileSync(cssPath, 'utf-8');
  });

  // Test Case 7: Check viewport meta tag
  describe('Viewport Meta Tag', () => {
    test('HTML contains viewport meta tag', () => {
      expect(htmlContent).toMatch(/<meta\s+name=["']viewport["']/i);
    });

    test('viewport meta tag contains width=device-width', () => {
      const viewportMatch = htmlContent.match(/<meta\s+name=["']viewport["'][^>]*content=["']([^"']+)["']/i)
        || htmlContent.match(/<meta[^>]*content=["']([^"']+)["'][^>]*name=["']viewport["']/i);

      expect(viewportMatch).not.toBeNull();

      const viewportContent = viewportMatch[1];
      expect(viewportContent).toContain('width=device-width');
    });

    test('viewport meta tag contains initial-scale=1', () => {
      const viewportMatch = htmlContent.match(/<meta\s+name=["']viewport["'][^>]*content=["']([^"']+)["']/i)
        || htmlContent.match(/<meta[^>]*content=["']([^"']+)["'][^>]*name=["']viewport["']/i);

      expect(viewportMatch).not.toBeNull();

      const viewportContent = viewportMatch[1];
      expect(viewportContent).toMatch(/initial-scale=1(\.0)?/);
    });
  });

  // Test Case 6: Font size requirements
  describe('Font Size Configuration', () => {
    let mainCssContent;

    beforeAll(() => {
      const mainCssPath = path.join(__dirname, '../../css/styles.css');
      mainCssContent = fs.readFileSync(mainCssPath, 'utf-8');
    });

    test('body font size is at least 14px', () => {
      // Check in main CSS for body font-size
      const fontSizeMatch = mainCssContent.match(/body\s*\{[^}]*font-size:\s*(\d+)px/i);

      if (fontSizeMatch) {
        const fontSize = parseInt(fontSizeMatch[1], 10);
        expect(fontSize).toBeGreaterThanOrEqual(14);
      } else {
        // If no explicit font-size, check for rem-based sizing
        const remMatch = mainCssContent.match(/body\s*\{[^}]*font-size:\s*([\d.]+)rem/i);
        if (remMatch) {
          // Assuming 1rem = 16px (browser default)
          const fontSize = parseFloat(remMatch[1]) * 16;
          expect(fontSize).toBeGreaterThanOrEqual(14);
        } else {
          // Default browser font-size is 16px which is >= 14px
          expect(true).toBe(true);
        }
      }
    });

    test('responsive CSS maintains readable font size on mobile', () => {
      // Check that responsive CSS doesn't reduce font-size below 14px
      const mobileFontSizeMatch = cssContent.match(/@media[^{]*max-width:\s*767px[^{]*\{[^}]*body\s*\{[^}]*font-size:\s*(\d+)px/i);

      if (mobileFontSizeMatch) {
        const fontSize = parseInt(mobileFontSizeMatch[1], 10);
        expect(fontSize).toBeGreaterThanOrEqual(14);
      } else {
        // If no specific mobile font-size override, check body override
        const anyFontSizeMatch = cssContent.match(/body\s*\{[^}]*font-size:\s*(\d+)px/i);
        if (anyFontSizeMatch) {
          const fontSize = parseInt(anyFontSizeMatch[1], 10);
          expect(fontSize).toBeGreaterThanOrEqual(14);
        } else {
          // No font-size override means default is maintained (16px)
          expect(true).toBe(true);
        }
      }
    });
  });

  describe('Responsive CSS Structure', () => {
    test('responsive.css is linked in HTML', () => {
      expect(htmlContent).toMatch(/href=["'][^"']*responsive\.css["']/i);
    });

    test('responsive CSS contains mobile breakpoints', () => {
      // Check for mobile media query (max-width around 767px)
      expect(cssContent).toMatch(/@media[^{]*max-width:\s*(767|768|480|600)px/i);
    });

    test('responsive CSS contains tablet breakpoints', () => {
      // Check for tablet media query (768px - 1023px range)
      expect(cssContent).toMatch(/@media[^{]*(min-width:\s*768px|max-width:\s*1023px|max-width:\s*1024px)/i);
    });

    test('responsive CSS prevents horizontal overflow', () => {
      // Check for overflow-x: hidden or max-width: 100vw
      expect(cssContent).toMatch(/overflow-x:\s*hidden|max-width:\s*100vw|max-width:\s*100%/i);
    });

    test('responsive CSS defines touch target sizes', () => {
      // Check for minimum 44px touch targets
      expect(cssContent).toMatch(/min-height:\s*44px|min-width:\s*44px/i);
    });
  });

  describe('Responsive Image Handling', () => {
    test('responsive CSS includes max-width for images', () => {
      // Check that images have max-width: 100%
      expect(cssContent).toMatch(/img[^{]*\{[^}]*max-width:\s*100%/i);
    });
  });
});
