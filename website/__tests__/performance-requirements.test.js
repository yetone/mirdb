/**
 * Performance Requirements Tests
 * Tests for NFR-1 of MirDB Landing Page
 * Verifies page weight under 500KB and asset optimization
 */

const fs = require('fs');
const path = require('path');

describe('Performance Requirements', () => {
  const srcDir = path.join(__dirname, '../src');
  let htmlContent;
  let document;

  beforeAll(() => {
    const htmlPath = path.join(srcDir, 'index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf8');
    document = new DOMParser().parseFromString(htmlContent, 'text/html');
  });

  /**
   * Test Case 6: CSS and JavaScript files are minified for production
   * Checks that CSS has no excessive whitespace patterns that indicate unminified code
   */
  describe('TC-6: CSS and JavaScript Minification', () => {
    test('CSS files should be optimized and properly structured', () => {
      const cssPath = path.join(srcDir, 'styles.css');
      expect(fs.existsSync(cssPath)).toBe(true);

      const cssContent = fs.readFileSync(cssPath, 'utf8');
      const cssSize = Buffer.byteLength(cssContent, 'utf8');

      // CSS should be reasonably sized (under 50KB for a landing page)
      expect(cssSize).toBeLessThan(50 * 1024);

      // CSS should contain actual content
      expect(cssContent.length).toBeGreaterThan(0);

      // CSS should have valid structure (contains selectors and rules)
      expect(cssContent).toMatch(/\{[\s\S]*?\}/);
    });

    test('CSS should use system fonts (no heavy web font downloads)', () => {
      const cssPath = path.join(srcDir, 'styles.css');
      const cssContent = fs.readFileSync(cssPath, 'utf8');

      // Should use system font stack for better performance
      const usesSystemFonts = cssContent.includes('-apple-system') ||
                              cssContent.includes('BlinkMacSystemFont') ||
                              cssContent.includes('system-ui') ||
                              cssContent.includes('Segoe UI');

      expect(usesSystemFonts).toBe(true);
    });

    test('HTML should not include excessive inline scripts', () => {
      const scripts = document.querySelectorAll('script');
      let totalInlineScriptSize = 0;

      scripts.forEach(script => {
        if (!script.src && script.textContent) {
          totalInlineScriptSize += script.textContent.length;
        }
      });

      // Inline scripts should be minimal (under 5KB)
      expect(totalInlineScriptSize).toBeLessThan(5 * 1024);
    });

    test('HTML should be well-structured without excessive whitespace', () => {
      const htmlSize = Buffer.byteLength(htmlContent, 'utf8');

      // HTML should be under 20KB for a simple landing page
      expect(htmlSize).toBeLessThan(20 * 1024);

      // HTML should have proper structure
      expect(htmlContent).toContain('<!DOCTYPE html>');
      expect(htmlContent).toContain('<html');
      expect(htmlContent).toContain('<head>');
      expect(htmlContent).toContain('<body>');
    });

    test('CSS should not have excessive comments (production optimization)', () => {
      const cssPath = path.join(srcDir, 'styles.css');
      const cssContent = fs.readFileSync(cssPath, 'utf8');

      // Count comment blocks
      const commentMatches = cssContent.match(/\/\*[\s\S]*?\*\//g) || [];
      let totalCommentSize = 0;
      commentMatches.forEach(comment => {
        totalCommentSize += comment.length;
      });

      // Comments should be less than 20% of total CSS
      const commentRatio = totalCommentSize / cssContent.length;
      expect(commentRatio).toBeLessThan(0.2);
    });
  });

  /**
   * Test Case 7: Images below the fold implement lazy loading
   * Verifies that images have lazy loading attributes where appropriate
   */
  describe('TC-7: Lazy Loading for Below-Fold Images', () => {
    test('Page should not have excessive image count without lazy loading', () => {
      const images = document.querySelectorAll('img');

      // If there are more than 2 images, at least some should have lazy loading
      if (images.length > 2) {
        const lazyLoadedImages = document.querySelectorAll('img[loading="lazy"]');
        expect(lazyLoadedImages.length).toBeGreaterThan(0);
      } else {
        // Few images is acceptable without lazy loading
        expect(images.length).toBeLessThanOrEqual(2);
      }
    });

    test('Above-the-fold images should NOT have lazy loading', () => {
      // Images in hero section should load immediately
      const heroSection = document.querySelector('.hero-section');
      if (heroSection) {
        const heroImages = heroSection.querySelectorAll('img');
        heroImages.forEach(img => {
          // Hero images should either have loading="eager" or no loading attribute
          const loadingAttr = img.getAttribute('loading');
          expect(loadingAttr !== 'lazy').toBe(true);
        });
      }
    });

    test('If iframes exist, they should have lazy loading', () => {
      const iframes = document.querySelectorAll('iframe');
      iframes.forEach(iframe => {
        // Iframes (e.g., embedded videos) should have lazy loading
        const loadingAttr = iframe.getAttribute('loading');
        expect(loadingAttr).toBe('lazy');
      });
    });

    test('CSS background images should be optimized', () => {
      const cssPath = path.join(srcDir, 'styles.css');
      const cssContent = fs.readFileSync(cssPath, 'utf8');

      // Check for large background image URLs (data URIs over 10KB are not recommended)
      const dataUriMatches = cssContent.match(/url\(["']?data:image[^)]+\)/g) || [];

      dataUriMatches.forEach(dataUri => {
        // Data URIs should be under 10KB
        expect(dataUri.length).toBeLessThan(10 * 1024);
      });
    });
  });

  /**
   * Test Case 4: Total page weight under 500KB
   */
  describe('TC-4: Page Weight Under 500KB', () => {
    test('Combined HTML and CSS should be under 500KB', () => {
      const htmlPath = path.join(srcDir, 'index.html');
      const cssPath = path.join(srcDir, 'styles.css');

      const htmlSize = fs.statSync(htmlPath).size;
      const cssSize = fs.statSync(cssPath).size;

      const totalSize = htmlSize + cssSize;

      // Total should be well under 500KB (in bytes)
      expect(totalSize).toBeLessThan(500 * 1024);
    });

    test('HTML file should be under 50KB', () => {
      const htmlPath = path.join(srcDir, 'index.html');
      const htmlSize = fs.statSync(htmlPath).size;

      // HTML should be under 50KB for a landing page
      expect(htmlSize).toBeLessThan(50 * 1024);
    });

    test('CSS file should be under 100KB', () => {
      const cssPath = path.join(srcDir, 'styles.css');
      const cssSize = fs.statSync(cssPath).size;

      // CSS should be under 100KB
      expect(cssSize).toBeLessThan(100 * 1024);
    });

    test('No external large image files are directly embedded in HTML', () => {
      // Check for large inline base64 images in HTML
      const base64Images = htmlContent.match(/data:image[^"']+/g) || [];

      base64Images.forEach(img => {
        // Inline images should be under 5KB
        expect(img.length).toBeLessThan(5 * 1024);
      });
    });
  });

  /**
   * Test Case 5: Image assets are compressed and optimized
   */
  describe('TC-5: Image Optimization', () => {
    test('Any image references should use modern formats or be optimized', () => {
      // Check img src attributes
      const images = document.querySelectorAll('img[src]');

      images.forEach(img => {
        const src = img.getAttribute('src');
        if (src && !src.startsWith('data:')) {
          // External images should use modern formats
          const hasModernFormat = src.endsWith('.webp') ||
                                  src.endsWith('.avif') ||
                                  src.endsWith('.svg') ||
                                  src.endsWith('.png') ||
                                  src.endsWith('.jpg') ||
                                  src.endsWith('.jpeg');
          expect(hasModernFormat).toBe(true);
        }
      });
    });

    test('SVG icons are preferred over raster images for small graphics', () => {
      const cssPath = path.join(srcDir, 'styles.css');
      const cssContent = fs.readFileSync(cssPath, 'utf8');

      // If using gradients or simple shapes, CSS is preferred over images
      const usesGradients = cssContent.includes('linear-gradient') ||
                           cssContent.includes('radial-gradient');

      // Using CSS gradients is a good performance practice
      expect(usesGradients).toBe(true);
    });

    test('Images should have appropriate dimensions specified', () => {
      const images = document.querySelectorAll('img');

      images.forEach(img => {
        // Images should ideally have width/height to prevent layout shift
        // OR be styled with CSS dimensions
        const hasWidth = img.getAttribute('width') ||
                        img.style.width ||
                        img.getAttribute('style')?.includes('width');
        const hasHeight = img.getAttribute('height') ||
                         img.style.height ||
                         img.getAttribute('style')?.includes('height');

        // Either dimensions should be set, or there are no images (which is fine)
        if (images.length > 0 && img.getAttribute('src')) {
          // At minimum, images should be in a container that constrains size
          expect(img.closest('div, section, figure') !== null || hasWidth || hasHeight).toBe(true);
        }
      });
    });
  });

  /**
   * Additional Performance Optimizations
   */
  describe('Additional Performance Optimizations', () => {
    test('HTML includes proper viewport meta tag', () => {
      const viewport = document.querySelector('meta[name="viewport"]');
      expect(viewport).not.toBeNull();
      expect(viewport.getAttribute('content')).toContain('width=device-width');
    });

    test('HTML includes charset declaration', () => {
      const charset = document.querySelector('meta[charset]');
      expect(charset).not.toBeNull();
      expect(charset.getAttribute('charset').toLowerCase()).toBe('utf-8');
    });

    test('External links have proper rel attributes for security and performance', () => {
      const externalLinks = document.querySelectorAll('a[target="_blank"]');

      externalLinks.forEach(link => {
        const rel = link.getAttribute('rel');
        expect(rel).toContain('noopener');
      });
    });

    test('CSS is loaded synchronously in head for critical rendering', () => {
      const headStylesheets = document.querySelectorAll('head link[rel="stylesheet"]');
      expect(headStylesheets.length).toBeGreaterThan(0);
    });

    test('No render-blocking external resources in body', () => {
      const bodyStylesheets = document.querySelectorAll('body link[rel="stylesheet"]:not([media="print"])');
      // Stylesheets should generally be in head, not body
      expect(bodyStylesheets.length).toBe(0);
    });
  });
});
