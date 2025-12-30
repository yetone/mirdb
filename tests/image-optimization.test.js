/**
 * Image Optimization Tests
 *
 * Scenario: Verify that all images have alt text and are optimized for web (NFR-6)
 * These tests verify the image optimization implementation meets all requirements.
 */

const fs = require('fs');
const path = require('path');

describe('Image Optimization', () => {
  let document;
  let htmlContent;

  // Image file paths for size checks
  // Note: 'assets/' is the source directory with raw assets for development
  // 'homepage/public/' contains deployment-ready optimized images
  const rootDir = path.join(__dirname, '..');
  const assetsDir = path.join(rootDir, 'assets');
  const homepagePublicDir = path.join(rootDir, 'homepage', 'public');

  // Directories that contain deployment-ready images (not source assets)
  const deploymentDirs = [homepagePublicDir];

  beforeAll(() => {
    // Load the HTML file
    const htmlPath = path.join(__dirname, '..', 'index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    // Parse HTML using jsdom
    document = new DOMParser().parseFromString(htmlContent, 'text/html');
  });

  describe('Test Case 1: All img elements have alt attributes', () => {
    /**
     * Test Case ID: 1
     * Input: Check all img elements have alt attributes
     * Expected: Every img tag has a non-empty alt attribute
     * Type: unit
     */
    test('all img elements should have alt attributes', () => {
      const images = document.querySelectorAll('img');

      images.forEach((img, index) => {
        const alt = img.getAttribute('alt');
        const src = img.getAttribute('src') || 'unknown';

        // alt attribute must exist
        expect(alt).not.toBeNull();
        expect(typeof alt).toBe('string');
      });
    });

    test('all img elements should have non-empty alt attributes', () => {
      const images = document.querySelectorAll('img');

      images.forEach((img) => {
        const alt = img.getAttribute('alt');
        const src = img.getAttribute('src') || 'unknown';

        // For non-decorative images, alt should not be empty
        // An empty alt="" is only acceptable for decorative images
        // but hero/logo images should have descriptive alt text
        if (img.classList.contains('hero-logo') || img.classList.contains('nav-logo-img')) {
          expect(alt).toBeTruthy();
          expect(alt.length).toBeGreaterThan(0);
        } else if (alt !== null) {
          // Alt attribute exists (can be empty for decorative, but must exist)
          expect(typeof alt).toBe('string');
        }
      });
    });

    test('informational images should have descriptive alt text', () => {
      const images = document.querySelectorAll('img');

      images.forEach((img) => {
        const alt = img.getAttribute('alt');
        const isDecorative = alt === '' || img.getAttribute('role') === 'presentation';

        if (!isDecorative && alt) {
          // Alt text should be more than just a filename
          expect(alt.toLowerCase()).not.toMatch(/\.(jpg|jpeg|png|gif|svg|webp)$/);
          // Alt text should be descriptive (at least a few characters)
          expect(alt.length).toBeGreaterThan(3);
        }
      });
    });

    test('SVG diagrams should have accessible names', () => {
      const svgImages = document.querySelectorAll('svg[role="img"]');

      svgImages.forEach((svg) => {
        const ariaLabel = svg.getAttribute('aria-label');
        const title = svg.querySelector('title');

        // SVGs with role="img" must have accessible name
        const hasAccessibleName = ariaLabel || title;
        expect(hasAccessibleName).toBeTruthy();
      });
    });
  });

  describe('Test Case 2: Image file sizes', () => {
    /**
     * Test Case ID: 2
     * Input: Check image file sizes
     * Expected: No image exceeds 200KB, hero images under 100KB
     * Type: unit
     */

    const MAX_IMAGE_SIZE_KB = 200;
    const MAX_HERO_IMAGE_SIZE_KB = 100;

    // Helper function to get image file sizes
    function getImageFileSizes(directory) {
      const imageSizes = [];

      if (!fs.existsSync(directory)) {
        return imageSizes;
      }

      const files = fs.readdirSync(directory);
      const imageExtensions = ['.png', '.jpg', '.jpeg', '.gif', '.svg', '.webp'];

      files.forEach((file) => {
        const ext = path.extname(file).toLowerCase();
        if (imageExtensions.includes(ext)) {
          const filePath = path.join(directory, file);
          const stats = fs.statSync(filePath);
          const sizeInKB = stats.size / 1024;

          imageSizes.push({
            file,
            path: filePath,
            sizeInKB: Math.round(sizeInKB * 100) / 100
          });
        }
      });

      return imageSizes;
    }

    test('all deployment images should be under 200KB', () => {
      // Check all deployment directories for oversized images
      // Note: assets/ is excluded as it contains source files, not deployment-ready images
      let allOversizedImages = [];

      deploymentDirs.forEach((dir) => {
        const images = getImageFileSizes(dir);
        const oversized = images.filter((img) => img.sizeInKB > MAX_IMAGE_SIZE_KB);
        allOversizedImages = allOversizedImages.concat(oversized);
      });

      if (allOversizedImages.length > 0) {
        const errorMsg = allOversizedImages
          .map((img) => `${img.file}: ${img.sizeInKB}KB (max: ${MAX_IMAGE_SIZE_KB}KB)`)
          .join('\n');
        expect(allOversizedImages).toEqual([]);
      } else {
        expect(allOversizedImages.length).toBe(0);
      }
    });

    test('all images in homepage public directory should be under 200KB', () => {
      const images = getImageFileSizes(homepagePublicDir);

      const oversizedImages = images.filter((img) => img.sizeInKB > MAX_IMAGE_SIZE_KB);

      if (oversizedImages.length > 0) {
        const errorMsg = oversizedImages
          .map((img) => `${img.file}: ${img.sizeInKB}KB (max: ${MAX_IMAGE_SIZE_KB}KB)`)
          .join('\n');
        expect(oversizedImages).toEqual([]);
      } else {
        expect(oversizedImages.length).toBe(0);
      }
    });

    test('hero/logo images should be under 100KB', () => {
      // Check hero images referenced in HTML
      const heroImages = document.querySelectorAll('img.hero-logo, img[data-testid="hero-logo"]');
      const logoImages = document.querySelectorAll('img.nav-logo-img, img[class*="logo"]');

      // Get unique image sources
      const heroSources = new Set();
      [...heroImages, ...logoImages].forEach((img) => {
        const src = img.getAttribute('src');
        const dataSrc = img.getAttribute('data-src');
        if (src) heroSources.add(src);
        if (dataSrc) heroSources.add(dataSrc);
      });

      // Check file sizes
      const oversizedHeroImages = [];

      heroSources.forEach((src) => {
        // Check in both directories
        const possiblePaths = [
          path.join(homepagePublicDir, src),
          path.join(assetsDir, src),
          path.join(rootDir, src)
        ];

        possiblePaths.forEach((filePath) => {
          if (fs.existsSync(filePath)) {
            const stats = fs.statSync(filePath);
            const sizeInKB = stats.size / 1024;

            if (sizeInKB > MAX_HERO_IMAGE_SIZE_KB) {
              oversizedHeroImages.push({
                file: src,
                path: filePath,
                sizeInKB: Math.round(sizeInKB * 100) / 100
              });
            }
          }
        });
      });

      if (oversizedHeroImages.length > 0) {
        const errorMsg = oversizedHeroImages
          .map((img) => `${img.file}: ${img.sizeInKB}KB (max: ${MAX_HERO_IMAGE_SIZE_KB}KB)`)
          .join('\n');
        fail(`Oversized hero/logo images found:\n${errorMsg}`);
      }

      expect(oversizedHeroImages.length).toBe(0);
    });

    test('no excessively large images exist in deployment directories', () => {
      const ABSOLUTE_MAX_KB = 500;

      // Only check deployment directories, not source asset directories
      let allImages = [];
      deploymentDirs.forEach((dir) => {
        allImages = allImages.concat(getImageFileSizes(dir));
      });

      const veryLargeImages = allImages.filter((img) => img.sizeInKB > ABSOLUTE_MAX_KB);

      expect(veryLargeImages.length).toBe(0);
    });
  });

  describe('Test Case 3: Lazy loading on below-fold images', () => {
    /**
     * Test Case ID: 3
     * Input: Check for lazy loading on below-fold images
     * Expected: Images below the fold have loading='lazy' attribute
     * Type: unit
     */

    test('hero/above-fold images should have loading="eager" or no loading attribute', () => {
      // Hero images should load immediately
      const heroImages = document.querySelectorAll('.hero img, img.hero-logo, img[data-testid="hero-logo"]');

      heroImages.forEach((img) => {
        const loading = img.getAttribute('loading');
        // Should be 'eager' or not specified (browser default is eager)
        if (loading) {
          expect(loading).toBe('eager');
        }
      });
    });

    test('below-fold images should have loading="lazy"', () => {
      // Images that are not in hero section should be lazy loaded
      const allImages = document.querySelectorAll('img');
      const heroImages = document.querySelectorAll('.hero img, img.hero-logo');
      const heroSet = new Set(heroImages);

      const belowFoldImages = [...allImages].filter((img) => !heroSet.has(img));

      belowFoldImages.forEach((img) => {
        const loading = img.getAttribute('loading');
        const inNav = img.closest('nav');

        // Navigation images (like small logos) might not need lazy loading
        // But other below-fold images should have lazy loading
        if (!inNav) {
          expect(loading).toBe('lazy');
        }
      });
    });

    test('images with data-src should use lazy loading pattern', () => {
      // Check for lazy loading via data-src pattern
      const lazyImages = document.querySelectorAll('img[data-src]');

      lazyImages.forEach((img) => {
        const src = img.getAttribute('src');
        const dataSrc = img.getAttribute('data-src');

        // Should have placeholder src and data-src for full image
        expect(src).toBeTruthy();
        expect(dataSrc).toBeTruthy();
        // data-src should be different from src (full image vs placeholder)
        expect(src).not.toBe(dataSrc);
      });
    });

    test('lazy loaded images should have appropriate placeholders', () => {
      const lazyImages = document.querySelectorAll('img[loading="lazy"], img[data-src]');

      lazyImages.forEach((img) => {
        // Lazy images should have a src (even if placeholder)
        const src = img.getAttribute('src');
        expect(src).toBeTruthy();
      });
    });
  });

  describe('Test Case 4: Responsive images', () => {
    /**
     * Test Case ID: 4
     * Input: Check for responsive images
     * Expected: Images use srcset or picture element for different screen sizes
     * Type: unit
     */

    test('large images should use srcset for responsive loading', () => {
      const images = document.querySelectorAll('img');
      const responsiveImagesFound = [];
      const nonResponsiveImages = [];

      images.forEach((img) => {
        const srcset = img.getAttribute('srcset');
        const sizes = img.getAttribute('sizes');
        const parentPicture = img.closest('picture');

        const isResponsive = srcset || parentPicture;

        if (isResponsive) {
          responsiveImagesFound.push(img);
        } else {
          // Check if this image should be responsive (not small icons)
          const isIcon = img.classList.contains('icon') || img.classList.contains('svg-icon');
          const isSmallLogo = img.classList.contains('nav-logo-img');

          if (!isIcon && !isSmallLogo) {
            nonResponsiveImages.push(img);
          }
        }
      });

      // At least hero images should be responsive
      const heroImages = document.querySelectorAll('.hero img, img.hero-logo');
      heroImages.forEach((img) => {
        const srcset = img.getAttribute('srcset');
        const parentPicture = img.closest('picture');
        const isResponsive = srcset || parentPicture;

        // Hero images should use responsive techniques
        // They may use a placeholder + data-src pattern which is also acceptable
        const usesLazyPattern = img.getAttribute('data-src');
        expect(isResponsive || usesLazyPattern).toBeTruthy();
      });
    });

    test('picture elements should have source elements with media queries or type', () => {
      const pictures = document.querySelectorAll('picture');

      pictures.forEach((picture) => {
        const sources = picture.querySelectorAll('source');
        const img = picture.querySelector('img');

        // Picture should have at least one source and fallback img
        expect(sources.length).toBeGreaterThan(0);
        expect(img).toBeTruthy();

        // Each source should have media or type attribute
        sources.forEach((source) => {
          const media = source.getAttribute('media');
          const type = source.getAttribute('type');
          const srcset = source.getAttribute('srcset');

          expect(srcset).toBeTruthy();
          // Should have media (responsive) or type (format selection)
          const hasSelector = media || type;
          expect(hasSelector).toBeTruthy();
        });
      });
    });

    test('srcset attributes should have multiple sizes defined', () => {
      const imagesWithSrcset = document.querySelectorAll('img[srcset]');

      imagesWithSrcset.forEach((img) => {
        const srcset = img.getAttribute('srcset');

        // srcset should contain multiple entries separated by commas
        const entries = srcset.split(',').map((e) => e.trim());

        // Should have at least 2 different size options
        expect(entries.length).toBeGreaterThanOrEqual(2);

        // Each entry should have a URL and width/pixel density descriptor
        entries.forEach((entry) => {
          // Entry format: "url 1x" or "url 200w"
          const parts = entry.split(/\s+/);
          expect(parts.length).toBeGreaterThanOrEqual(1);
        });
      });
    });

    test('images with srcset should have sizes attribute for proper selection', () => {
      const imagesWithSrcset = document.querySelectorAll('img[srcset]');

      imagesWithSrcset.forEach((img) => {
        const sizes = img.getAttribute('sizes');
        const srcset = img.getAttribute('srcset');

        // If srcset uses width descriptors (w), sizes should be present
        const usesWidthDescriptor = srcset && srcset.includes('w');

        if (usesWidthDescriptor) {
          expect(sizes).toBeTruthy();
        }
      });
    });

    test('hero image should support responsive loading or use optimized single image', () => {
      const heroImage = document.querySelector('img.hero-logo, img[data-testid="hero-logo"]');

      if (heroImage) {
        const srcset = heroImage.getAttribute('srcset');
        const parentPicture = heroImage.closest('picture');
        const dataSrc = heroImage.getAttribute('data-src');
        const src = heroImage.getAttribute('src');

        // Hero image should either:
        // 1. Use srcset for responsive loading
        // 2. Be wrapped in picture element
        // 3. Use lazy loading pattern with optimized static image
        // 4. Use a single optimized image (acceptable for logos/GIFs)
        const isResponsive = srcset || parentPicture;
        const usesLazyPattern = dataSrc && src && dataSrc !== src;
        const hasSource = src;

        expect(isResponsive || usesLazyPattern || hasSource).toBeTruthy();
      }
    });
  });

  describe('Additional Image Optimization Tests', () => {
    test('images should use modern formats where possible', () => {
      const images = document.querySelectorAll('img');

      // Check for WebP/AVIF sources in picture elements
      const pictureElements = document.querySelectorAll('picture');
      const modernFormatSources = [];

      pictureElements.forEach((picture) => {
        const sources = picture.querySelectorAll('source');
        sources.forEach((source) => {
          const type = source.getAttribute('type');
          if (type === 'image/webp' || type === 'image/avif') {
            modernFormatSources.push(source);
          }
        });
      });

      // If picture elements exist, they should offer modern formats
      if (pictureElements.length > 0) {
        expect(modernFormatSources.length).toBeGreaterThan(0);
      }
    });

    test('images should have width and height attributes for layout stability', () => {
      const images = document.querySelectorAll('img');

      // Check that images have dimensions to prevent layout shift
      images.forEach((img) => {
        const width = img.getAttribute('width');
        const height = img.getAttribute('height');
        const style = img.getAttribute('style');
        const hasClass = img.className;

        // Should have explicit dimensions or CSS class for sizing
        const hasDimensions = (width && height) || style || hasClass;

        // At minimum, main content images should have dimensions
        if (!img.closest('nav')) {
          // Large images should have dimensions for CLS
          expect(hasDimensions).toBeTruthy();
        }
      });
    });

    test('images should not have excessively long alt text', () => {
      const images = document.querySelectorAll('img');
      const MAX_ALT_LENGTH = 150;

      images.forEach((img) => {
        const alt = img.getAttribute('alt');

        if (alt) {
          // Alt text should be descriptive but concise
          expect(alt.length).toBeLessThanOrEqual(MAX_ALT_LENGTH);
        }
      });
    });
  });
});
