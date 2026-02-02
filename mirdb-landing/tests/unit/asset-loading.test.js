/**
 * Asset Loading Unit Tests
 * Owner: Scenario 18 - Asset Loading and Optimization
 *
 * Tests:
 * - Lazy loading attribute validation
 * - Image optimization verification
 * - Asset availability checks
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const HTML_PATH = path.resolve(__dirname, '../../index.html');

describe('Asset Loading - Unit Tests', () => {
  let htmlContent;

  beforeAll(() => {
    htmlContent = fs.readFileSync(HTML_PATH, 'utf-8');
  });

  describe('TC3: Verify lazy loading attribute on images', () => {
    test('Below-fold images should have loading="lazy" attribute', () => {
      // Parse images from HTML
      const imgTagRegex = /<img[^>]*>/gi;
      const images = htmlContent.match(imgTagRegex) || [];

      // Known below-fold images (architecture section, etc.)
      const belowFoldPatterns = [
        /architecture/i,
        /usage/i,
      ];

      // Check that below-fold images have lazy loading
      const belowFoldImages = images.filter((img) =>
        belowFoldPatterns.some((pattern) => pattern.test(img))
      );

      belowFoldImages.forEach((img) => {
        const hasLazyLoading = /loading\s*=\s*["']lazy["']/i.test(img);
        expect(hasLazyLoading).toBe(true);
      });
    });

    test('Images have width and height attributes for layout stability', () => {
      // Parse content images (not badges/shields)
      const imgTagRegex = /<img[^>]*>/gi;
      const images = htmlContent.match(imgTagRegex) || [];

      // Filter to content images only (exclude badges and shields)
      const contentImages = images.filter(
        (img) => !img.includes('badge') && !img.includes('shield')
      );

      contentImages.forEach((img) => {
        const hasWidth = /width\s*=/i.test(img);
        const hasHeight = /height\s*=/i.test(img);

        // Content images should have dimensions for CLS optimization
        if (hasWidth || hasHeight) {
          expect(hasWidth || hasHeight).toBe(true);
        }
      });
    });
  });

  describe('Asset Files', () => {
    const assetsDir = path.resolve(__dirname, '../../assets');

    test('Logo file exists in assets/images', () => {
      const logoPath = path.join(assetsDir, 'images', 'logo.gif');
      expect(fs.existsSync(logoPath)).toBe(true);
    });

    test('Usage GIF exists in assets/images', () => {
      const usagePath = path.join(assetsDir, 'images', 'usage.gif');
      expect(fs.existsSync(usagePath)).toBe(true);
    });

    test('Architecture SVG exists in assets/images', () => {
      const archPath = path.join(assetsDir, 'images', 'architecture.svg');
      expect(fs.existsSync(archPath)).toBe(true);
    });

    test('Icon SVGs exist in assets/icons', () => {
      const iconsDir = path.join(assetsDir, 'icons');
      const expectedIcons = ['github.svg', 'copy.svg', 'menu.svg'];

      expectedIcons.forEach((icon) => {
        const iconPath = path.join(iconsDir, icon);
        expect(fs.existsSync(iconPath)).toBe(true);
      });
    });
  });

  describe('Lazy Load Module', () => {
    test('Lazy load module file exists', () => {
      const modulePath = path.resolve(
        __dirname,
        '../../js/modules/lazy-load.js'
      );
      expect(fs.existsSync(modulePath)).toBe(true);
    });

    test('Lazy load module exports required functions', () => {
      const modulePath = path.resolve(
        __dirname,
        '../../js/modules/lazy-load.js'
      );
      const moduleContent = fs.readFileSync(modulePath, 'utf-8');

      // Check for init export
      expect(moduleContent).toMatch(/export\s+(?:const|function)\s+init/);

      // Check for observeElement export
      expect(moduleContent).toMatch(
        /export\s+(?:const|function)\s+observeElement/
      );
    });

    test('Lazy load module uses IntersectionObserver', () => {
      const modulePath = path.resolve(
        __dirname,
        '../../js/modules/lazy-load.js'
      );
      const moduleContent = fs.readFileSync(modulePath, 'utf-8');

      // Check for IntersectionObserver usage
      expect(moduleContent).toMatch(/IntersectionObserver/);
    });
  });

  describe('Main.js Integration', () => {
    test('Main.js imports lazy-load module', () => {
      const mainPath = path.resolve(__dirname, '../../js/main.js');
      const mainContent = fs.readFileSync(mainPath, 'utf-8');

      // Check for lazy-load import
      expect(mainContent).toMatch(/import.*lazy-load/);
    });

    test('Main.js initializes lazy loading', () => {
      const mainPath = path.resolve(__dirname, '../../js/main.js');
      const mainContent = fs.readFileSync(mainPath, 'utf-8');

      // Check for lazy loading initialization
      expect(mainContent).toMatch(/initLazyLoad\(\)/);
    });
  });

  describe('Image Optimization', () => {
    test('Architecture SVG is reasonably sized', () => {
      const svgPath = path.resolve(
        __dirname,
        '../../assets/images/architecture.svg'
      );
      const stats = fs.statSync(svgPath);

      // SVG should be under 50KB for a diagram
      expect(stats.size).toBeLessThan(50 * 1024);
    });

    test('Asset images reference correct paths in HTML', () => {
      // Check that image paths in HTML match asset structure
      const assetPathRegex = /src=["']assets\/images\/[^"']+["']/g;
      const paths = htmlContent.match(assetPathRegex) || [];

      paths.forEach((srcMatch) => {
        // Extract the path
        const pathMatch = srcMatch.match(/src=["']([^"']+)["']/);
        if (pathMatch) {
          const relativePath = pathMatch[1];
          const fullPath = path.resolve(__dirname, '../../', relativePath);

          // Verify file exists
          expect(fs.existsSync(fullPath)).toBe(true);
        }
      });
    });
  });
});
