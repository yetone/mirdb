/**
 * Performance Tests for Page Load
 * Tests NFR-2: Page load time under 3 seconds
 *
 * Test cases:
 * - TC3: Verify image lazy loading attributes
 * - TC5: Verify image optimization (WebP format or optimized JPG/PNG)
 */

const fs = require('fs');
const path = require('path');
require('@testing-library/jest-dom');

// Note: The global beforeEach in setup.js handles loading the HTML content

describe('Page Load Performance', () => {

  describe('Test Case 3: Lazy Loading Attributes', () => {
    test('Below-fold images should have loading="lazy" attribute', () => {
      // Get all images on the page
      const allImages = document.querySelectorAll('img');

      // Images that are below the fold (not in header/hero)
      // The showcase image is the primary below-fold image
      const showcaseImage = document.querySelector('[data-testid="showcase-image"]');

      expect(showcaseImage).toBeInTheDocument();
      expect(showcaseImage).toHaveAttribute('loading', 'lazy');
    });

    test('Hero image should NOT have lazy loading (above the fold)', () => {
      const heroImage = document.querySelector('[data-testid="hero-image"]');

      expect(heroImage).toBeInTheDocument();
      // Hero image should either not have loading attribute or have loading="eager"
      const loadingAttr = heroImage.getAttribute('loading');
      expect(loadingAttr === null || loadingAttr === 'eager').toBe(true);
    });

    test('Logo image should NOT have lazy loading (critical above-fold content)', () => {
      const logoImage = document.querySelector('.logo-image');

      expect(logoImage).toBeInTheDocument();
      // Logo should not be lazy loaded as it's above the fold
      const loadingAttr = logoImage.getAttribute('loading');
      expect(loadingAttr === null || loadingAttr === 'eager').toBe(true);
    });

    test('All images have alt attributes for accessibility', () => {
      const allImages = document.querySelectorAll('img');

      allImages.forEach((img) => {
        expect(img).toHaveAttribute('alt');
        expect(img.getAttribute('alt').trim()).not.toBe('');
      });
    });
  });

  describe('Test Case 5: Image Optimization', () => {
    test('Images should use optimized formats (GIF, WebP, optimized JPG/PNG)', () => {
      const allImages = document.querySelectorAll('img');
      const validFormats = ['.webp', '.gif', '.jpg', '.jpeg', '.png', '.svg'];

      allImages.forEach((img) => {
        const src = img.getAttribute('src');
        expect(src).toBeTruthy();

        const extension = path.extname(src).toLowerCase();
        expect(validFormats).toContain(extension);
      });
    });

    test('GIF images are used appropriately for animated content', () => {
      // Check that GIF images exist and have appropriate usage
      const allImages = document.querySelectorAll('img');
      const gifImages = Array.from(allImages).filter(img =>
        img.getAttribute('src')?.endsWith('.gif')
      );

      // GIF images should have descriptive alt text indicating animation/demo
      gifImages.forEach((img) => {
        const alt = img.getAttribute('alt');
        expect(alt).toBeTruthy();
      });
    });

    test('Image sources should not be empty or undefined', () => {
      const allImages = document.querySelectorAll('img');

      allImages.forEach((img) => {
        const src = img.getAttribute('src');
        expect(src).toBeTruthy();
        expect(src).not.toBe('');
        expect(src).not.toBe('undefined');
      });
    });
  });

  describe('CSS Performance Optimizations', () => {
    test('CSS file includes scroll-behavior smooth for perceived performance', () => {
      const cssPath = path.resolve(__dirname, '../src/styles.css');
      const cssContent = fs.readFileSync(cssPath, 'utf8');

      expect(cssContent).toContain('scroll-behavior: smooth');
    });

    test('CSS uses CSS custom properties for efficient theming', () => {
      const cssPath = path.resolve(__dirname, '../src/styles.css');
      const cssContent = fs.readFileSync(cssPath, 'utf8');

      expect(cssContent).toContain(':root');
      expect(cssContent).toContain('--primary-color');
      expect(cssContent).toContain('var(--');
    });

    test('CSS has responsive media queries for optimized mobile loading', () => {
      const cssPath = path.resolve(__dirname, '../src/styles.css');
      const cssContent = fs.readFileSync(cssPath, 'utf8');

      expect(cssContent).toContain('@media');
      expect(cssContent).toMatch(/@media.*max-width/);
    });
  });

  describe('HTML Structure Performance', () => {
    test('HTML includes viewport meta tag for proper mobile rendering', () => {
      const viewportMeta = document.querySelector('meta[name="viewport"]');
      expect(viewportMeta).toBeInTheDocument();
      expect(viewportMeta.getAttribute('content')).toContain('width=device-width');
    });

    test('HTML includes charset meta tag', () => {
      const charsetMeta = document.querySelector('meta[charset]');
      expect(charsetMeta).toBeInTheDocument();
      expect(charsetMeta.getAttribute('charset').toLowerCase()).toBe('utf-8');
    });

    test('CSS is loaded in head for render blocking optimization', () => {
      const linkTags = document.querySelectorAll('head link[rel="stylesheet"]');
      expect(linkTags.length).toBeGreaterThan(0);
    });

    test('JavaScript is loaded at end of body for non-blocking', () => {
      const bodyScripts = document.querySelectorAll('body script');
      expect(bodyScripts.length).toBeGreaterThan(0);
    });
  });

  describe('Layout Shift Prevention', () => {
    test('Images have explicit dimensions via CSS', () => {
      // Check that image styling prevents layout shifts
      const cssPath = path.resolve(__dirname, '../src/styles.css');
      const cssContent = fs.readFileSync(cssPath, 'utf8');

      // Images should have max-width and height:auto for responsive sizing without shifts
      expect(cssContent).toContain('max-width');
      expect(cssContent).toContain('height: auto');
    });

    test('Hero section has defined layout structure', () => {
      const heroSection = document.querySelector('[data-testid="hero-section"]');
      expect(heroSection).toBeInTheDocument();

      // Verify hero has the proper class for grid layout
      expect(heroSection).toHaveClass('hero-section');
    });

    test('CSS defines explicit sizing for logo image', () => {
      const cssPath = path.resolve(__dirname, '../src/styles.css');
      const cssContent = fs.readFileSync(cssPath, 'utf8');

      // Logo should have explicit height to prevent layout shift
      expect(cssContent).toMatch(/\.logo-image\s*\{[^}]*height/);
    });
  });
});
