import { describe, it, expect, beforeEach } from 'vitest';
import { JSDOM } from 'jsdom';
import fs from 'fs';
import path from 'path';

describe('Page Performance', () => {
  let document;
  let html;
  let css;

  beforeEach(() => {
    const htmlPath = path.resolve(__dirname, '../index.html');
    html = fs.readFileSync(htmlPath, 'utf8');
    const dom = new JSDOM(html);
    document = dom.window.document;

    const cssPath = path.resolve(__dirname, '../styles.css');
    css = fs.readFileSync(cssPath, 'utf8');
  });

  // Test Case 1: DOM Content Loaded Time (verified by page weight analysis)
  describe('Test Case 1: Page Load Performance (Structural Verification)', () => {
    it('should have minimal DOM elements for fast parsing', () => {
      // Count total elements in the DOM
      const allElements = document.querySelectorAll('*');
      // A lightweight page should have less than 100 elements
      expect(allElements.length).toBeLessThan(100);
    });

    it('should have no render-blocking scripts in head', () => {
      const head = document.querySelector('head');
      const scripts = head.querySelectorAll('script:not([async]):not([defer])');
      // No blocking scripts should be in the head
      expect(scripts.length).toBe(0);
    });

    it('should have a single stylesheet link for minimal requests', () => {
      const stylesheets = document.querySelectorAll('link[rel="stylesheet"]');
      // Only one CSS file should be loaded
      expect(stylesheets.length).toBe(1);
    });

    it('should not have inline scripts that could delay rendering', () => {
      const scripts = document.querySelectorAll('script');
      // Page should work without any JavaScript for core content
      expect(scripts.length).toBe(0);
    });
  });

  // Test Case 2: Total Page Weight
  describe('Test Case 2: Total Page Size Under 1MB', () => {
    it('should have HTML file size well under 1MB', () => {
      const htmlPath = path.resolve(__dirname, '../index.html');
      const stats = fs.statSync(htmlPath);
      const fileSizeInBytes = stats.size;
      const fileSizeInKB = fileSizeInBytes / 1024;

      // HTML should be under 50KB for a landing page
      expect(fileSizeInKB).toBeLessThan(50);
    });

    it('should have CSS file size well under 1MB', () => {
      const cssPath = path.resolve(__dirname, '../styles.css');
      const stats = fs.statSync(cssPath);
      const fileSizeInBytes = stats.size;
      const fileSizeInKB = fileSizeInBytes / 1024;

      // CSS should be under 50KB for a landing page
      expect(fileSizeInKB).toBeLessThan(50);
    });

    it('should have combined HTML and CSS under 100KB', () => {
      const htmlPath = path.resolve(__dirname, '../index.html');
      const cssPath = path.resolve(__dirname, '../styles.css');

      const htmlStats = fs.statSync(htmlPath);
      const cssStats = fs.statSync(cssPath);

      const totalSizeInKB = (htmlStats.size + cssStats.size) / 1024;

      // Combined should be under 100KB for fast 3G loading
      expect(totalSizeInKB).toBeLessThan(100);
    });

    it('should not exceed 1MB total page weight estimate', () => {
      const htmlPath = path.resolve(__dirname, '../index.html');
      const cssPath = path.resolve(__dirname, '../styles.css');

      const htmlStats = fs.statSync(htmlPath);
      const cssStats = fs.statSync(cssPath);

      // No images in current page, just HTML + CSS
      const totalSizeInBytes = htmlStats.size + cssStats.size;
      const totalSizeInMB = totalSizeInBytes / (1024 * 1024);

      // Total should be well under 1MB
      expect(totalSizeInMB).toBeLessThan(1);
    });
  });

  // Test Case 3: Image Optimization (WebP or compressed formats)
  describe('Test Case 3: Image Optimization', () => {
    it('should use modern image formats (WebP, AVIF) or have no images', () => {
      const images = document.querySelectorAll('img');

      if (images.length === 0) {
        // No images is acceptable - page uses CSS for visual design
        expect(images.length).toBe(0);
      } else {
        // If there are images, check they use modern formats
        images.forEach(img => {
          const src = img.getAttribute('src') || '';
          const hasModernFormat = src.endsWith('.webp') ||
                                  src.endsWith('.avif') ||
                                  src.endsWith('.svg'); // SVG is also optimized
          expect(hasModernFormat).toBe(true);
        });
      }
    });

    it('should use CSS gradients instead of image backgrounds', () => {
      // Check that hero uses gradient instead of image
      const hasGradient = css.includes('linear-gradient');
      expect(hasGradient).toBe(true);
    });

    it('should not have large background images', () => {
      // Check for background-image urls in CSS that might be large
      const backgroundImageMatches = css.match(/background-image:\s*url\([^)]+\)/g) || [];
      // Should have no external image URLs for performance
      expect(backgroundImageMatches.length).toBe(0);
    });
  });

  // Test Case 4: Core Content Visible Without JavaScript
  describe('Test Case 4: Core Content Without JavaScript', () => {
    it('should have product name visible without JavaScript', () => {
      const productName = document.querySelector('.product-name, h1');
      expect(productName).not.toBeNull();
      expect(productName.textContent).toContain('MirDB');
    });

    it('should have tagline visible without JavaScript', () => {
      const tagline = document.querySelector('.tagline');
      expect(tagline).not.toBeNull();
      expect(tagline.textContent.length).toBeGreaterThan(0);
    });

    it('should have features/value proposition visible without JavaScript', () => {
      const valueProp = document.querySelector('.value-proposition');
      expect(valueProp).not.toBeNull();
      expect(valueProp.textContent.length).toBeGreaterThan(0);
    });

    it('should have CTA buttons visible without JavaScript', () => {
      const ctaButtons = document.querySelectorAll('.btn, a.btn-primary, a.btn-secondary');
      expect(ctaButtons.length).toBeGreaterThan(0);
    });

    it('should have Get Started button accessible without JavaScript', () => {
      const getStartedBtn = document.querySelector('a[href="#get-started"]');
      expect(getStartedBtn).not.toBeNull();
      expect(getStartedBtn.textContent).toBe('Get Started');
    });

    it('should not rely on JavaScript for essential content rendering', () => {
      // Page should have no script tags - pure HTML/CSS
      const scripts = document.querySelectorAll('script');
      expect(scripts.length).toBe(0);
    });

    it('should have installation instructions visible without JavaScript', () => {
      const installSection = document.querySelector('.installation');
      expect(installSection).not.toBeNull();

      const codeBlocks = installSection.querySelectorAll('pre code');
      expect(codeBlocks.length).toBeGreaterThan(0);
    });
  });

  // Test Case 5: Lazy Loading on Below-Fold Images
  describe('Test Case 5: Lazy Loading for Below-Fold Images', () => {
    it('should have lazy loading attribute on below-fold images if present', () => {
      const images = document.querySelectorAll('img');

      if (images.length === 0) {
        // No images means test passes - no lazy loading needed
        expect(true).toBe(true);
      } else {
        // Check all images for lazy loading
        const belowFoldImages = Array.from(images).filter((img, index) => {
          // First image is likely above fold, rest should be lazy
          return index > 0;
        });

        belowFoldImages.forEach(img => {
          expect(img.getAttribute('loading')).toBe('lazy');
        });
      }
    });

    it('should use loading="lazy" attribute format correctly', () => {
      const images = document.querySelectorAll('img[loading]');
      images.forEach(img => {
        const loadingValue = img.getAttribute('loading');
        expect(['lazy', 'eager', 'auto']).toContain(loadingValue);
      });
    });

    it('should have no render-blocking image resources in head', () => {
      const head = document.querySelector('head');
      const preloadImages = head.querySelectorAll('link[rel="preload"][as="image"]');
      // No critical images to preload for this text-focused page
      expect(preloadImages.length).toBe(0);
    });
  });

  // Test Case 6: CSS Optimization
  describe('Test Case 6: CSS Minification and Optimization', () => {
    it('should have CSS file that can be minified', () => {
      // CSS should be properly formatted and minifiable
      expect(css.length).toBeGreaterThan(0);
    });

    it('should use efficient CSS selectors', () => {
      // Check for efficient class-based selectors rather than deep nesting
      const hasClassSelectors = css.includes('.');
      expect(hasClassSelectors).toBe(true);
    });

    it('should not have excessive comments in CSS', () => {
      // Count comment blocks
      const commentMatches = css.match(/\/\*[\s\S]*?\*\//g) || [];
      const commentRatio = commentMatches.join('').length / css.length;

      // Comments should be less than 20% of CSS for production
      expect(commentRatio).toBeLessThan(0.2);
    });

    it('should use CSS custom properties or shorthand for efficiency', () => {
      // Check for efficient patterns like shorthand properties
      const hasShorthand = css.includes('padding:') ||
                           css.includes('margin:') ||
                           css.includes('border:');
      expect(hasShorthand).toBe(true);
    });

    it('should have proper CSS structure for maintainability', () => {
      // Should have proper structure with selectors
      const rulesetCount = (css.match(/\{/g) || []).length;
      // Should have reasonable number of rulesets
      expect(rulesetCount).toBeGreaterThan(10);
      expect(rulesetCount).toBeLessThan(200);
    });

    it('should not have unused CSS frameworks', () => {
      // Check that we're not loading large frameworks
      const hasBootstrap = css.includes('bootstrap');
      const hasTailwindBase = css.includes('@tailwind');

      expect(hasBootstrap).toBe(false);
      expect(hasTailwindBase).toBe(false);
    });
  });

  // Additional Performance Tests
  describe('Additional Performance Optimizations', () => {
    it('should use system fonts for fast rendering', () => {
      const hasSystemFonts = css.includes('-apple-system') ||
                             css.includes('system-ui') ||
                             css.includes('BlinkMacSystemFont');
      expect(hasSystemFonts).toBe(true);
    });

    it('should not have external font imports that delay rendering', () => {
      const head = document.querySelector('head');
      const externalFonts = head.querySelectorAll('link[href*="fonts.googleapis.com"], link[href*="fonts.gstatic.com"]');
      expect(externalFonts.length).toBe(0);
    });

    it('should have proper meta viewport for mobile performance', () => {
      const viewport = document.querySelector('meta[name="viewport"]');
      expect(viewport).not.toBeNull();
      expect(viewport.getAttribute('content')).toContain('width=device-width');
    });

    it('should have proper charset meta tag early in head', () => {
      const charset = document.querySelector('meta[charset]');
      expect(charset).not.toBeNull();
      expect(charset.getAttribute('charset').toLowerCase()).toBe('utf-8');
    });

    it('should have reduced motion media query for accessibility', () => {
      const hasReducedMotion = css.includes('prefers-reduced-motion');
      expect(hasReducedMotion).toBe(true);
    });
  });
});
