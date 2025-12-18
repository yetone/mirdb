/**
 * Responsive Design - Mobile Tests
 * Tests for NFR-2: Page must be fully responsive across desktop, tablet, and mobile devices
 *
 * These tests verify the CSS responsive design implementation by parsing CSS rules
 * and validating that proper mobile breakpoints and responsive styles are in place.
 */

const fs = require('fs');
const path = require('path');

describe('Responsive Design - Mobile', () => {
  let htmlContent;
  let cssContent;
  let document;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../src/index.html');
    const cssPath = path.join(__dirname, '../src/styles.css');
    htmlContent = fs.readFileSync(htmlPath, 'utf8');
    cssContent = fs.readFileSync(cssPath, 'utf8');
    document = new DOMParser().parseFromString(htmlContent, 'text/html');
  });

  // Test Case 1: No horizontal scrollbar appears at 375px viewport width
  describe('TC-1: No horizontal scrollbar at 375px viewport width', () => {
    test('should have viewport meta tag for responsive design', () => {
      const viewportMeta = document.querySelector('meta[name="viewport"]');
      expect(viewportMeta).not.toBeNull();
      const content = viewportMeta.getAttribute('content');
      expect(content).toContain('width=device-width');
      expect(content).toContain('initial-scale=1');
    });

    test('should have max-width constraints on container', () => {
      // Check that CSS has proper container constraints
      expect(cssContent).toMatch(/\.container\s*\{[^}]*max-width:/);
    });

    test('should use box-sizing border-box for all elements', () => {
      // This prevents elements from causing overflow due to padding/borders
      expect(cssContent).toMatch(/\*\s*\{[^}]*box-sizing:\s*border-box/);
    });

    test('should have mobile-responsive media queries', () => {
      // Check for mobile media query (max-width: 768px or similar)
      expect(cssContent).toMatch(/@media\s*\(\s*max-width:\s*768px\s*\)/);
    });
  });

  // Test Case 2: Hero section text is fully visible and readable at 375px
  describe('TC-2: Hero section text is fully visible and readable', () => {
    test('should have hero section with proper responsive font sizing', () => {
      const heroSection = document.querySelector('.hero-section');
      expect(heroSection).not.toBeNull();

      // Check for responsive font size in media query
      expect(cssContent).toMatch(/@media[^{]*max-width:\s*768px[^{]*\{[^@]*\.product-name\s*\{[^}]*font-size:/);
    });

    test('should have reduced product name font size in mobile breakpoint', () => {
      // Extract mobile media query content
      const mobileMediaMatch = cssContent.match(/@media\s*\(\s*max-width:\s*768px\s*\)\s*\{([\s\S]*?)\n\}/);
      expect(mobileMediaMatch).not.toBeNull();

      const mobileStyles = mobileMediaMatch[1];
      // Product name should have reduced font size for mobile
      expect(mobileStyles).toMatch(/\.product-name\s*\{[^}]*font-size:\s*2\.5rem/);
    });

    test('should have responsive tagline font size', () => {
      const mobileMediaMatch = cssContent.match(/@media\s*\(\s*max-width:\s*768px\s*\)\s*\{([\s\S]*?)\n\}/);
      expect(mobileMediaMatch).not.toBeNull();

      const mobileStyles = mobileMediaMatch[1];
      // Tagline should have reduced font size for mobile
      expect(mobileStyles).toMatch(/\.tagline|\.hero-tagline/);
    });
  });

  // Test Case 3: Feature cards stack vertically at 375px
  describe('TC-3: Feature cards stack vertically', () => {
    test('should have features grid with responsive layout', () => {
      const featuresSection = document.querySelector('.features-section');
      expect(featuresSection).not.toBeNull();

      const featuresGrid = document.querySelector('.features-grid');
      expect(featuresGrid).not.toBeNull();
    });

    test('should use CSS grid or flexbox for features layout', () => {
      // Check for grid or flexbox layout on features grid
      expect(cssContent).toMatch(/\.features-grid\s*\{[^}]*(display:\s*(grid|flex)|grid-template-columns)/);
    });

    test('should have auto-fit or auto-fill for responsive grid columns', () => {
      // Check for responsive grid with auto-fit/auto-fill and minmax
      expect(cssContent).toMatch(/\.features-grid\s*\{[^}]*grid-template-columns:[^}]*repeat\s*\(\s*auto-fit/);
    });

    test('should have minmax with reasonable minimum for single column on mobile', () => {
      // minmax(300px, 1fr) or similar will stack at 375px viewport
      const gridMatch = cssContent.match(/\.features-grid\s*\{[^}]*grid-template-columns:[^}]*minmax\s*\(\s*(\d+)px/);
      expect(gridMatch).not.toBeNull();
      const minWidth = parseInt(gridMatch[1], 10);
      // Min width should be large enough to force stacking on 375px viewport
      expect(minWidth).toBeGreaterThanOrEqual(300);
    });
  });

  // Test Case 4: Code snippet section is horizontally scrollable or wrapped appropriately
  describe('TC-4: Code snippet section horizontal scroll handling', () => {
    test('should have code block section', () => {
      const codeBlock = document.querySelector('.code-block');
      expect(codeBlock).not.toBeNull();
    });

    test('should have overflow-x auto on code blocks', () => {
      // Code blocks should allow horizontal scrolling
      expect(cssContent).toMatch(/\.code-block\s*\{[^}]*overflow-x:\s*auto/);
    });

    test('should have pre element inside code block', () => {
      const codeBlock = document.querySelector('.code-block');
      const preElement = codeBlock.querySelector('pre');
      expect(preElement).not.toBeNull();
    });
  });

  // Test Case 5: CTA buttons are full-width or appropriately sized for mobile
  describe('TC-5: CTA buttons are appropriately sized for mobile', () => {
    test('should have CTA buttons in hero section', () => {
      const ctaButtons = document.querySelector('.cta-buttons');
      expect(ctaButtons).not.toBeNull();

      const buttons = ctaButtons.querySelectorAll('a');
      expect(buttons.length).toBeGreaterThanOrEqual(2);
    });

    test('should have flex-wrap on CTA buttons container', () => {
      // CTA buttons container should wrap on mobile
      expect(cssContent).toMatch(/\.cta-buttons\s*\{[^}]*flex-wrap:\s*wrap/);
    });

    test('should have flex-direction column on mobile for CTA buttons', () => {
      const mobileMediaMatch = cssContent.match(/@media\s*\(\s*max-width:\s*768px\s*\)\s*\{([\s\S]*?)\n\}/);
      expect(mobileMediaMatch).not.toBeNull();

      const mobileStyles = mobileMediaMatch[1];
      // CTA buttons should stack vertically on mobile
      expect(mobileStyles).toMatch(/\.cta-buttons\s*\{[^}]*flex-direction:\s*column/);
    });

    test('should have full width or max-width for CTA buttons on mobile', () => {
      const mobileMediaMatch = cssContent.match(/@media\s*\(\s*max-width:\s*768px\s*\)\s*\{([\s\S]*?)\n\}/);
      expect(mobileMediaMatch).not.toBeNull();

      const mobileStyles = mobileMediaMatch[1];
      // CTA buttons should have width: 100% or max-width on mobile
      expect(mobileStyles).toMatch(/\.(cta-primary|cta-secondary|btn-primary)\s*\{[^}]*(width:\s*100%|max-width)/);
    });
  });

  // Test Case 6: All touch targets are at least 44x44 pixels
  describe('TC-6: Touch target sizing', () => {
    test('should have CTA buttons with adequate padding for touch targets', () => {
      // CTA buttons should have padding that ensures minimum 44x44px touch target
      // With 14px padding on each side (28px total) plus text, this meets minimum
      expect(cssContent).toMatch(/\.(cta-primary|btn-primary)\s*\{[^}]*padding:\s*14px\s+32px/);
    });

    test('should have secondary CTA button with adequate padding', () => {
      expect(cssContent).toMatch(/\.cta-secondary\s*\{[^}]*padding:\s*14px\s+32px/);
    });

    test('should have footer links with adequate spacing', () => {
      // Footer links should have gap for touch target separation
      expect(cssContent).toMatch(/\.footer-links\s*\{[^}]*gap:/);
    });

    test('should have command items with adequate touch target size', () => {
      // Command items should have padding that ensures touch target size
      expect(cssContent).toMatch(/\.command-item|\.command-card\s*\{[^}]*padding:\s*1\.5rem/);
    });
  });

  // Test Case 7: Page layout adapts appropriately for tablet size (768px)
  describe('TC-7: Tablet viewport adaptation', () => {
    test('should have tablet breakpoint in media queries', () => {
      // 768px is a common tablet breakpoint
      expect(cssContent).toMatch(/@media\s*\(\s*max-width:\s*768px\s*\)/);
    });

    test('should have commands grid with 2 columns on tablet', () => {
      const mobileMediaMatch = cssContent.match(/@media\s*\(\s*max-width:\s*768px\s*\)\s*\{([\s\S]*?)\n\}/);
      expect(mobileMediaMatch).not.toBeNull();

      const mobileStyles = mobileMediaMatch[1];
      // Commands grid should have 2 columns at tablet size
      expect(mobileStyles).toMatch(/\.commands-grid\s*\{[^}]*grid-template-columns:\s*repeat\s*\(\s*2/);
    });

    test('should have reduced section heading font sizes on tablet', () => {
      const mobileMediaMatch = cssContent.match(/@media\s*\(\s*max-width:\s*768px\s*\)\s*\{([\s\S]*?)\n\}/);
      expect(mobileMediaMatch).not.toBeNull();

      const mobileStyles = mobileMediaMatch[1];
      // Section headings should have reduced font size
      expect(mobileStyles).toMatch(/\.(features-section|quickstart-section|commands-section)\s+h2\s*\{[^}]*font-size:\s*2rem/);
    });
  });

  // Additional responsive design verification
  describe('Additional Responsive Design Checks', () => {
    test('should have mobile-first or responsive container padding', () => {
      expect(cssContent).toMatch(/\.container\s*\{[^}]*padding:\s*0\s+20px/);
    });

    test('should have hero section with responsive padding', () => {
      expect(cssContent).toMatch(/\.hero-section\s*\{[^}]*padding:\s*\d+px\s+20px/);
    });

    test('should not have fixed widths that would cause overflow', () => {
      // Check that containers use max-width (fluid) rather than fixed width
      // max-width allows elements to shrink on smaller screens
      expect(cssContent).toMatch(/\.container\s*\{[^}]*max-width:/);
      expect(cssContent).toMatch(/\.code-block\s*\{[^}]*max-width:/);

      // Verify containers use max-width (not fixed width)
      // The container class should have max-width, not a fixed width property
      const containerBlock = cssContent.match(/\.container\s*\{[^}]*\}/);
      expect(containerBlock).not.toBeNull();
      // Should use max-width instead of fixed width
      expect(containerBlock[0]).toMatch(/max-width:/);
      // Should not have a standalone "width:" property (not "max-width:")
      expect(containerBlock[0]).not.toMatch(/(?<!max-)width:\s*\d{3,}px/);
    });

    test('should have value proposition text with constrained width', () => {
      expect(cssContent).toMatch(/\.(value-proposition|hero-description)\s*\{[^}]*max-width:/);
    });
  });
});
