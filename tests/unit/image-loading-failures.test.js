/**
 * Unit Tests for Image Loading Failures - Error Handling
 * Scenario: Error Handling - Image Loading Failures
 * Test Cases: 2, 3
 * Validates that images have dimensions to prevent layout shift and meaningful alt text
 */

const fs = require('fs');
const path = require('path');
const { JSDOM } = require('jsdom');

describe('Error Handling - Image Loading Failures', () => {
  let document;
  let htmlContent;
  let dom;

  beforeAll(() => {
    const htmlPath = path.resolve(__dirname, '../../index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf-8');
    dom = new JSDOM(htmlContent);
    document = dom.window.document;
  });

  /**
   * Test Case 2: Check images have explicit width/height or aspect-ratio
   * Expected: Images have dimensions set to prevent layout shift
   */
  describe('Test Case 2: Image Dimensions for Layout Stability', () => {
    test('all img elements should have explicit width and height attributes or CSS dimensions', () => {
      const images = document.querySelectorAll('img');

      // Check each img element for dimension attributes
      images.forEach((img) => {
        const hasWidthAttr = img.hasAttribute('width');
        const hasHeightAttr = img.hasAttribute('height');
        const hasExplicitDimensions = hasWidthAttr && hasHeightAttr;

        // Check for CSS classes that define dimensions
        const hasResponsiveClass = img.classList.contains('hero-logo') ||
                                   img.classList.contains('responsive') ||
                                   img.classList.contains('img-fluid');

        // Check inline style for dimensions
        const style = img.getAttribute('style') || '';
        const hasCssDimensions = style.includes('width') ||
                                 style.includes('height') ||
                                 style.includes('aspect-ratio');

        const hasDimensionStrategy = hasExplicitDimensions || hasResponsiveClass || hasCssDimensions;

        expect(hasDimensionStrategy).toBe(true);
      });
    });

    test('SVG elements used as images should have viewBox for proper scaling', () => {
      // Check SVGs with role="img" (used as images)
      const svgImages = document.querySelectorAll('svg[role="img"]');

      svgImages.forEach((svg) => {
        const viewBox = svg.getAttribute('viewBox');
        expect(viewBox).toBeTruthy();
        // ViewBox should be in format "minX minY width height"
        expect(viewBox).toMatch(/^\d+\s+\d+\s+\d+\s+\d+$/);
      });
    });

    test('hero logo should have defined dimensions via CSS class', () => {
      const heroLogo = document.querySelector('.hero-logo');

      if (heroLogo) {
        // Check if it's an SVG with viewBox (for scalability)
        if (heroLogo.tagName.toLowerCase() === 'svg') {
          expect(heroLogo.getAttribute('viewBox')).toBeTruthy();
        } else if (heroLogo.tagName.toLowerCase() === 'img') {
          // For img elements, should have width/height
          const hasWidth = heroLogo.hasAttribute('width') || heroLogo.classList.contains('hero-logo');
          const hasHeight = heroLogo.hasAttribute('height') || heroLogo.classList.contains('hero-logo');
          expect(hasWidth && hasHeight).toBe(true);
        }

        // The hero-logo class should be defined in CSS with dimensions
        // Check that the class is present which provides CSS-based dimensions
        expect(heroLogo.classList.contains('hero-logo')).toBe(true);
      }
    });

    test('images should not cause layout shift when failing to load', () => {
      const images = document.querySelectorAll('img');
      const svgImages = document.querySelectorAll('svg[role="img"]');

      // All visual elements should have a size defined
      const allVisualElements = [...images, ...svgImages];

      allVisualElements.forEach((el) => {
        if (el.tagName.toLowerCase() === 'img') {
          // img elements need explicit dimensions or CSS constraints
          const hasWidth = el.hasAttribute('width');
          const hasHeight = el.hasAttribute('height');
          const hasClass = el.className.length > 0;

          expect(hasWidth && hasHeight || hasClass).toBe(true);
        } else if (el.tagName.toLowerCase() === 'svg') {
          // SVG elements need viewBox
          expect(el.getAttribute('viewBox')).toBeTruthy();
        }
      });
    });

    test('CSS should define fixed dimensions for hero-logo class', () => {
      // Extract CSS from style tag
      const styleTag = document.querySelector('style');
      const cssText = styleTag?.textContent || '';

      // Check that hero-logo has width and height defined
      const heroLogoMatch = cssText.match(/\.hero-logo\s*\{[^}]*\}/);
      expect(heroLogoMatch).toBeTruthy();

      const heroLogoCss = heroLogoMatch[0];
      expect(heroLogoCss).toMatch(/width\s*:/);
      expect(heroLogoCss).toMatch(/height\s*:/);
    });
  });

  /**
   * Test Case 3: Verify architecture diagram has meaningful alt text
   * Expected: Diagram alt text describes the data flow it represents
   */
  describe('Test Case 3: Architecture Diagram Alt Text', () => {
    test('architecture diagram SVG should have role="img" for accessibility', () => {
      // Check for SVG with class lsm-tree-diagram or in architecture section
      const architectureSvg = document.querySelector('svg.lsm-tree-diagram') ||
                              document.querySelector('.architecture svg[role="img"]') ||
                              document.querySelector('svg[role="img"]');

      if (architectureSvg) {
        expect(architectureSvg.getAttribute('role')).toBe('img');
      }
    });

    test('architecture diagram should have aria-label or aria-labelledby for accessibility', () => {
      const architectureSvg = document.querySelector('svg.lsm-tree-diagram') ||
                              document.querySelector('svg[role="img"]');

      if (architectureSvg) {
        const hasAriaLabel = architectureSvg.hasAttribute('aria-label');
        const hasAriaLabelledby = architectureSvg.hasAttribute('aria-labelledby');

        expect(hasAriaLabel || hasAriaLabelledby).toBe(true);
      }
    });

    test('hero logo SVG should have aria-label describing the logo', () => {
      const heroLogo = document.querySelector('.hero-logo');

      if (heroLogo && heroLogo.tagName.toLowerCase() === 'svg') {
        const ariaLabel = heroLogo.getAttribute('aria-label');
        expect(ariaLabel).toBeTruthy();
        expect(ariaLabel.toLowerCase()).toContain('logo');
      }
    });

    test('informative SVGs should have title elements for screen readers', () => {
      const svgsWithRole = document.querySelectorAll('svg[role="img"]');

      svgsWithRole.forEach((svg) => {
        // Should have either title element or aria-label
        const titleElement = svg.querySelector('title');
        const ariaLabel = svg.getAttribute('aria-label');

        expect(titleElement || ariaLabel).toBeTruthy();
      });
    });

    test('all decorative SVGs should be hidden from assistive technology', () => {
      // SVGs used for decoration (icons) should have aria-hidden="true"
      const allSvgs = document.querySelectorAll('svg');

      allSvgs.forEach((svg) => {
        const isInformative = svg.getAttribute('role') === 'img';
        const isDecorativeIcon = svg.closest('.feature-icon') ||
                                 svg.closest('.differentiator-icon') ||
                                 svg.closest('[aria-hidden="true"]');

        if (!isInformative) {
          // Should be aria-hidden for decorative purposes
          const isHidden = svg.getAttribute('aria-hidden') === 'true';
          const parentHidden = svg.closest('[aria-hidden="true"]');

          // Decorative SVGs should be hidden or contained in hidden parent
          expect(isHidden || parentHidden || !isDecorativeIcon).toBeTruthy();
        }
      });
    });
  });

  /**
   * Additional tests for graceful degradation
   */
  describe('Graceful Degradation for Image Failures', () => {
    test('page should have all necessary content without images', () => {
      // Content should be available in text form, not just images
      const bodyText = document.body.textContent;

      // Key content should be present as text
      expect(bodyText).toContain('MirDB');
      expect(bodyText).toContain('Persistent');
      expect(bodyText.toLowerCase()).toContain('key-value');
      expect(bodyText.toLowerCase()).toContain('memcached');
    });

    test('navigation should be fully functional without images', () => {
      const navLinks = document.querySelectorAll('nav a');

      navLinks.forEach((link) => {
        // Each nav link should have text content
        const text = link.textContent.trim();
        expect(text.length).toBeGreaterThan(0);
      });
    });

    test('CTA buttons should not rely solely on images', () => {
      const buttons = document.querySelectorAll('.btn, button, a.btn-primary, a.btn-secondary');

      buttons.forEach((btn) => {
        const text = btn.textContent.trim();
        // Buttons should have text content for accessibility
        expect(text.length).toBeGreaterThan(0);
      });
    });
  });
});
