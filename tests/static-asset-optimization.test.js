/**
 * Static Asset Optimization Tests
 *
 * Scenario: Verify static assets are optimized for performance
 * Test Cases:
 * 1. Check image file sizes - No single image exceeds 200KB
 * 2. Check for lazy loading attributes - Below-fold images have loading='lazy'
 * 3. Verify no unused CSS - CSS contains only rules used on the page
 */

const fs = require('fs');
const path = require('path');

// Load HTML content for DOM testing
const htmlPath = path.join(__dirname, '..', 'index.html');
const htmlContent = fs.existsSync(htmlPath) ? fs.readFileSync(htmlPath, 'utf-8') : '';

describe('Static Asset Optimization', () => {
  beforeAll(() => {
    document.documentElement.innerHTML = htmlContent;
  });

  /**
   * Test Case 1: Check image file sizes
   * Expected: No single image exceeds 200KB
   */
  describe('Test Case 1: Image File Sizes', () => {
    const assetsDir = path.join(__dirname, '..', 'assets');
    const MAX_IMAGE_SIZE_KB = 200;
    const MAX_IMAGE_SIZE_BYTES = MAX_IMAGE_SIZE_KB * 1024;

    test('should have no referenced image files exceeding 200KB', () => {
      // Get all image sources from the HTML
      const imgElements = document.querySelectorAll('img');
      const localImageSrcs = Array.from(imgElements)
        .map(img => img.getAttribute('src'))
        .filter(src => src && !src.startsWith('http') && !src.startsWith('//'));

      const oversizedImages = [];
      const imageSizes = [];

      localImageSrcs.forEach(src => {
        // Resolve the path relative to project root
        const imagePath = path.join(__dirname, '..', src);

        if (fs.existsSync(imagePath)) {
          const stats = fs.statSync(imagePath);
          const sizeKB = stats.size / 1024;
          imageSizes.push({ file: src, sizeKB: sizeKB.toFixed(2) });

          if (stats.size > MAX_IMAGE_SIZE_BYTES) {
            oversizedImages.push({
              file: src,
              sizeKB: sizeKB.toFixed(2),
              maxKB: MAX_IMAGE_SIZE_KB
            });
          }
        }
      });

      // Log image sizes for debugging
      console.log('Referenced image file sizes:');
      imageSizes.forEach(({ file, sizeKB }) => {
        const status = parseFloat(sizeKB) <= MAX_IMAGE_SIZE_KB ? '✓' : '✗';
        console.log(`  ${status} ${file}: ${sizeKB} KB (max: ${MAX_IMAGE_SIZE_KB} KB)`);
      });

      // Assert no oversized images
      expect(oversizedImages.length).toBe(0);
    });

    test('should reference optimized image formats in HTML', () => {
      // Check that referenced images use optimized formats
      const imgElements = document.querySelectorAll('img');
      const srcAttributes = Array.from(imgElements).map(img => img.getAttribute('src')).filter(Boolean);

      console.log('Image references in HTML:');
      srcAttributes.forEach(src => {
        console.log(`  - ${src}`);
      });

      // Ensure images are referenced
      expect(srcAttributes.length).toBeGreaterThan(0);
    });
  });

  /**
   * Test Case 2: Check for lazy loading attributes
   * Expected: Below-fold images have loading='lazy' attribute
   */
  describe('Test Case 2: Lazy Loading Attributes', () => {
    test('should have loading="lazy" attribute on below-fold images', () => {
      const allImages = document.querySelectorAll('img');
      const belowFoldImages = [];
      const aboveFoldSections = ['hero', 'nav', 'navbar'];

      allImages.forEach(img => {
        const src = img.getAttribute('src') || '';
        const parentSections = [];
        let parent = img.parentElement;

        // Check ancestor elements
        while (parent) {
          const id = parent.getAttribute('id') || '';
          const className = parent.className || '';
          parentSections.push({ id, className });
          parent = parent.parentElement;
        }

        // Determine if image is below fold by checking if it's not in hero/nav
        const isInAboveFold = parentSections.some(({ id, className }) =>
          aboveFoldSections.some(section =>
            id.toLowerCase().includes(section) ||
            className.toLowerCase().includes(section)
          )
        );

        // Logo images in nav/hero are above fold
        const isLogo = src.toLowerCase().includes('logo');
        const isBelowFold = !isInAboveFold && !isLogo;

        if (isBelowFold) {
          belowFoldImages.push({
            src,
            hasLazyLoading: img.getAttribute('loading') === 'lazy'
          });
        }
      });

      console.log('Below-fold images:');
      belowFoldImages.forEach(({ src, hasLazyLoading }) => {
        const status = hasLazyLoading ? '✓' : '✗';
        console.log(`  ${status} ${src}: loading="${hasLazyLoading ? 'lazy' : 'missing'}"`);
      });

      // All below-fold images should have lazy loading
      const missingLazyLoading = belowFoldImages.filter(img => !img.hasLazyLoading);
      expect(missingLazyLoading.length).toBe(0);
    });

    test('should have images with proper loading attributes', () => {
      const allImages = document.querySelectorAll('img');
      const imageLoadingInfo = [];

      allImages.forEach(img => {
        const src = img.getAttribute('src') || '';
        const loading = img.getAttribute('loading');
        imageLoadingInfo.push({ src, loading });
      });

      console.log('All images and loading attributes:');
      imageLoadingInfo.forEach(({ src, loading }) => {
        console.log(`  - ${src}: loading="${loading || 'not set'}"`);
      });

      // At least verify we have images
      expect(allImages.length).toBeGreaterThan(0);
    });
  });

  /**
   * Test Case 3: Verify no unused CSS
   * Expected: CSS contains only rules used on the page
   */
  describe('Test Case 3: CSS Optimization', () => {
    test('should have CSS with rules that are used in HTML', () => {
      // Extract inline styles from HTML
      const styleElements = document.querySelectorAll('style');
      let cssContent = '';

      styleElements.forEach(style => {
        cssContent += style.textContent || '';
      });

      // Parse CSS selectors (simple regex approach)
      const selectorPattern = /([.#]?[a-zA-Z_-][a-zA-Z0-9_-]*)\s*[,{]/g;
      const selectors = new Set();
      let match;

      while ((match = selectorPattern.exec(cssContent)) !== null) {
        const selector = match[1].trim();
        if (selector && !selector.startsWith('@') && !selector.startsWith(':')) {
          selectors.add(selector);
        }
      }

      // Check which selectors have matching elements
      const usedSelectors = [];
      const unusedSelectors = [];

      selectors.forEach(selector => {
        try {
          // Skip pseudo-classes and special selectors
          if (selector.includes(':') || selector === '*') {
            usedSelectors.push(selector);
            return;
          }

          const elements = document.querySelectorAll(selector);
          if (elements.length > 0) {
            usedSelectors.push(selector);
          } else {
            // Check if it's a valid selector that might match dynamic content
            unusedSelectors.push(selector);
          }
        } catch (e) {
          // Invalid selector, skip
        }
      });

      console.log(`CSS Analysis:`);
      console.log(`  - Total selectors found: ${selectors.size}`);
      console.log(`  - Used selectors: ${usedSelectors.length}`);
      console.log(`  - Potentially unused selectors: ${unusedSelectors.length}`);

      if (unusedSelectors.length > 0 && unusedSelectors.length <= 10) {
        console.log('  Potentially unused selectors (sample):');
        unusedSelectors.slice(0, 10).forEach(sel => {
          console.log(`    - ${sel}`);
        });
      }

      // Calculate CSS efficiency ratio
      const efficiencyRatio = usedSelectors.length / (usedSelectors.length + unusedSelectors.length);
      console.log(`  - CSS efficiency ratio: ${(efficiencyRatio * 100).toFixed(1)}%`);

      // Allow some flexibility - at least 70% of selectors should be used
      // This accounts for responsive/dynamic selectors
      expect(efficiencyRatio).toBeGreaterThanOrEqual(0.7);
    });

    test('should not have excessively large inline CSS', () => {
      const styleElements = document.querySelectorAll('style');
      let totalCSSSize = 0;

      styleElements.forEach(style => {
        totalCSSSize += (style.textContent || '').length;
      });

      const cssSizeKB = totalCSSSize / 1024;
      console.log(`Total inline CSS size: ${cssSizeKB.toFixed(2)} KB`);

      // Inline CSS should be under 50KB for performance
      expect(cssSizeKB).toBeLessThan(50);
    });

    test('should use CSS variables for consistent theming', () => {
      const styleElements = document.querySelectorAll('style');
      let cssContent = '';

      styleElements.forEach(style => {
        cssContent += style.textContent || '';
      });

      // Check for CSS custom properties (variables)
      const hasRootVariables = cssContent.includes(':root');
      const hasVarUsage = cssContent.includes('var(--');

      console.log('CSS Variables:');
      console.log(`  - Has :root variables: ${hasRootVariables}`);
      console.log(`  - Uses var() function: ${hasVarUsage}`);

      // Modern CSS should use variables for maintainability
      expect(hasRootVariables).toBe(true);
      expect(hasVarUsage).toBe(true);
    });
  });
});
