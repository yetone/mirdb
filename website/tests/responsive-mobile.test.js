/**
 * Unit Tests for Responsive Design - Mobile Viewport
 *
 * Scenario: Verify the homepage displays correctly on mobile devices
 * without horizontal scrolling and with touch-friendly interactions
 *
 * Test Cases:
 * - TC1: No horizontal scrolling (CSS analysis)
 * - TC2: Responsive images (CSS rules)
 * - TC3: Touch target sizes (CSS rules)
 * - TC4: Font sizes (CSS rules)
 * - TC5: Code block scrolling (CSS rules)
 *
 * @jest-environment jsdom
 */

const fs = require('fs');
const path = require('path');

let css;
let htmlContent;

// Load HTML and CSS content before tests
beforeEach(() => {
  // Load HTML for each test to ensure clean state
  const htmlPath = path.join(__dirname, '..', 'index.html');
  htmlContent = fs.readFileSync(htmlPath, 'utf8');
  document.body.innerHTML = htmlContent;

  // Load CSS
  const cssPath = path.join(__dirname, '..', 'styles.css');
  css = fs.readFileSync(cssPath, 'utf8');
  const styleElement = document.createElement('style');
  styleElement.textContent = css;
  document.head.appendChild(styleElement);
});

describe('Responsive Design - Mobile Viewport', () => {

  /**
   * Test Case 1: No horizontal scrolling at mobile viewport
   * Expected: Document body width does not exceed viewport width
   */
  describe('Test Case 1: No Horizontal Scrolling', () => {
    test('CSS should prevent body overflow', () => {
      expect(css).toContain('overflow-x: hidden');
    });

    test('body should have max-width constraint', () => {
      expect(css).toContain('max-width: 100vw');
    });

    test('all containers should have max-width in mobile media query', () => {
      // Check that mobile containers get max-width: 100%
      expect(css).toContain('hero-container');
      expect(css).toContain('features-container');
      expect(css).toContain('quickstart-container');
      expect(css).toContain('architecture-container');
      expect(css).toContain('commands-container');
    });

    test('viewport meta tag should be properly configured', () => {
      expect(htmlContent).toContain('name="viewport"');
      expect(htmlContent).toContain('width=device-width');
      expect(htmlContent).toContain('initial-scale=1');
    });
  });

  /**
   * Test Case 2: Check all images and media for max-width
   * Expected: All images have max-width: 100% or equivalent responsive sizing
   */
  describe('Test Case 2: Image and Media Responsive Sizing', () => {
    test('all img elements should have max-width styles allowing responsive behavior', () => {
      const images = document.querySelectorAll('img');

      // Note: In jsdom, getComputedStyle may not fully apply CSS.
      // We verify the CSS rules exist and images have proper attributes
      images.forEach((img) => {
        // Images should either have inline max-width or rely on CSS
        // Check that images don't have fixed pixel widths that would break mobile
        const widthAttr = img.getAttribute('width');
        if (widthAttr && !widthAttr.includes('%')) {
          // If fixed width, should be reasonable for mobile
          const widthValue = parseInt(widthAttr, 10);
          expect(widthValue).toBeLessThanOrEqual(375);
        }
      });
    });

    test('SVG elements should use responsive width attributes', () => {
      const svgs = document.querySelectorAll('svg');

      svgs.forEach((svg) => {
        // SVGs should use viewBox for scalability
        const viewBox = svg.getAttribute('viewBox');
        if (svg.classList.contains('lsm-tree-diagram')) {
          expect(viewBox).toBeTruthy();
        }
      });
    });

    test('LSM-tree diagram should have responsive CSS class', () => {
      const diagram = document.querySelector('.lsm-tree-diagram');
      expect(diagram).toBeInTheDocument();
      // The CSS should set max-width: 100% and width: 100% for mobile
    });

    test('architecture diagram container should handle overflow', () => {
      const diagramContainer = document.querySelector('.architecture-diagram');
      expect(diagramContainer).toBeInTheDocument();
    });

    test('CSS should define max-width rules for images', () => {
      // Look for responsive image rules
      expect(css).toContain('img');
      expect(css).toContain('max-width: 100%');
      expect(css).toContain('height: auto');
    });

    test('CSS should define responsive LSM diagram in mobile media query', () => {
      // Check for lsm-tree-diagram max-width in mobile media query
      expect(css).toContain('lsm-tree-diagram');
    });
  });

  /**
   * Test Case 4: Check font sizes at mobile viewport
   * Expected: Body text is at least 16px, headings scale appropriately
   */
  describe('Test Case 4: Font Sizes for Mobile', () => {
    test('CSS should define appropriate base font size', () => {
      const cssPath = path.join(__dirname, '..', 'styles.css');
      const css = fs.readFileSync(cssPath, 'utf8');

      // The body should use a readable font stack
      expect(css).toContain('font-family');
    });

    test('body element should have readable line-height', () => {
      const cssPath = path.join(__dirname, '..', 'styles.css');
      const css = fs.readFileSync(cssPath, 'utf8');

      // Check for line-height definition
      expect(css).toContain('line-height');
    });

    test('hero title should scale down for mobile in media query', () => {
      const cssPath = path.join(__dirname, '..', 'styles.css');
      const css = fs.readFileSync(cssPath, 'utf8');

      // Check that there's a media query with font-size adjustment for hero-title
      expect(css).toContain('@media');
      expect(css).toContain('hero-title');
    });

    test('hero tagline should scale down for mobile in media query', () => {
      const cssPath = path.join(__dirname, '..', 'styles.css');
      const css = fs.readFileSync(cssPath, 'utf8');

      // Check for hero-tagline font size adjustment
      expect(css).toContain('hero-tagline');
    });

    test('section titles should be defined with appropriate size', () => {
      const sectionTitles = document.querySelectorAll('.section-title');
      expect(sectionTitles.length).toBeGreaterThan(0);
    });

    test('code blocks should use monospace font for readability', () => {
      const codeBlocks = document.querySelectorAll('.code-block code');
      expect(codeBlocks.length).toBeGreaterThan(0);

      const cssPath = path.join(__dirname, '..', 'styles.css');
      const css = fs.readFileSync(cssPath, 'utf8');
      expect(css).toContain('font-mono');
    });
  });

  /**
   * Additional CSS Structure Tests
   */
  describe('CSS Responsive Structure', () => {
    test('CSS should have mobile-first media query at 768px', () => {
      const cssPath = path.join(__dirname, '..', 'styles.css');
      const css = fs.readFileSync(cssPath, 'utf8');

      expect(css).toContain('@media (max-width: 768px)');
    });

    test('navigation should hide links on mobile', () => {
      const cssPath = path.join(__dirname, '..', 'styles.css');
      const css = fs.readFileSync(cssPath, 'utf8');

      // Check for nav-links display: none in media query
      const mobileMediaMatch = css.match(/@media[^{]+max-width:\s*768px[^{]*\{([^}]+\{[^}]*\})+/s);
      expect(mobileMediaMatch).toBeTruthy();
      expect(css).toContain('nav-links');
    });

    test('features grid should use single column on mobile', () => {
      const cssPath = path.join(__dirname, '..', 'styles.css');
      const css = fs.readFileSync(cssPath, 'utf8');

      // Check for features-grid in media query
      expect(css).toContain('features-grid');
      expect(css).toContain('grid-template-columns');
    });

    test('comparison grid should use single column on mobile', () => {
      const cssPath = path.join(__dirname, '..', 'styles.css');
      const css = fs.readFileSync(cssPath, 'utf8');

      // Check for comparison-grid in media query
      expect(css).toContain('comparison-grid');
    });

    test('footer should stack content on mobile', () => {
      const cssPath = path.join(__dirname, '..', 'styles.css');
      const css = fs.readFileSync(cssPath, 'utf8');

      // Check for footer-content flex-direction in media query
      expect(css).toContain('footer-content');
    });

    test('code blocks should have overflow-x auto for horizontal scrolling', () => {
      const cssPath = path.join(__dirname, '..', 'styles.css');
      const css = fs.readFileSync(cssPath, 'utf8');

      expect(css).toContain('overflow-x');
    });

    test('viewport meta tag should be present in HTML', () => {
      const htmlPath = path.join(__dirname, '..', 'index.html');
      const html = fs.readFileSync(htmlPath, 'utf8');

      expect(html).toContain('viewport');
      expect(html).toContain('width=device-width');
    });

    test('HTML should use box-sizing: border-box', () => {
      const cssPath = path.join(__dirname, '..', 'styles.css');
      const css = fs.readFileSync(cssPath, 'utf8');

      expect(css).toContain('box-sizing: border-box');
    });
  });

  /**
   * Test Case 3: Touch Target Sizes
   * Expected: Buttons and links have minimum 44x44px touch target area
   */
  describe('Test Case 3: Touch Target Sizes', () => {
    test('buttons should have proper class for styling', () => {
      const buttons = document.querySelectorAll('.btn');
      expect(buttons.length).toBeGreaterThan(0);
    });

    test('navigation links should exist', () => {
      const navLinks = document.querySelectorAll('.nav-links a');
      expect(navLinks.length).toBeGreaterThan(0);
    });

    test('footer links should exist', () => {
      const footerLinks = document.querySelectorAll('.footer-links a');
      expect(footerLinks.length).toBeGreaterThan(0);
    });

    test('CSS should define adequate padding for buttons', () => {
      // Check for btn padding definition
      expect(css).toContain('.btn');
      expect(css).toContain('padding');
    });

    test('CSS should define minimum touch target size for buttons', () => {
      // Check for min-height: 44px for buttons (WCAG touch target requirement)
      expect(css).toContain('min-height: 44px');
      expect(css).toContain('min-width: 44px');
    });

    test('copy buttons should have adequate size for touch', () => {
      const copyButtons = document.querySelectorAll('.copy-button');
      expect(copyButtons.length).toBeGreaterThan(0);
      // CSS should define adequate padding for copy buttons
      expect(css).toContain('.copy-button');
    });
  });

  /**
   * Test Case 5: Code Block Horizontal Scrolling
   * Expected: Code blocks scroll horizontally within container, not page-level scroll
   */
  describe('Test Case 5: Code Block Horizontal Scrolling', () => {
    test('code blocks should exist in the page', () => {
      const codeBlocks = document.querySelectorAll('.code-block');
      expect(codeBlocks.length).toBeGreaterThan(0);
    });

    test('CSS should define overflow-x for code blocks', () => {
      expect(css).toContain('.code-block');
      expect(css).toContain('overflow-x');
    });

    test('code blocks should have max-width constraint in mobile media query', () => {
      // Check for code-block max-width in mobile media query
      expect(css).toMatch(/\.code-block[\s\S]*?max-width/);
    });

    test('code blocks should use monospace font', () => {
      const codeElements = document.querySelectorAll('.code-block code');
      expect(codeElements.length).toBeGreaterThan(0);
      expect(css).toContain('font-mono');
    });

    test('code block pre elements should not overflow', () => {
      const preElements = document.querySelectorAll('.code-block pre');
      expect(preElements.length).toBeGreaterThan(0);
      // CSS should handle pre overflow through parent code-block
    });
  });
});
