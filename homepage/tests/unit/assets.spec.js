/**
 * Asset Optimization Tests
 * Owner: Scenario 11 - Performance and Load Time
 *
 * Tests:
 * - Logo file size (<500KB)
 * - Total page weight
 * - Image optimization
 */

const { test, expect } = require('@playwright/test');
const fs = require('fs');
const path = require('path');

const IMAGES_DIR = path.join(__dirname, '../../images');
const CSS_DIR = path.join(__dirname, '../../css');
const JS_DIR = path.join(__dirname, '../../js');
const ROOT_DIR = path.join(__dirname, '../..');

test.describe('Asset Optimization', () => {
  test.describe('Logo File Size', () => {
    test('logo GIF is under 500KB', async () => {
      const logoPath = path.join(IMAGES_DIR, 'logo.gif');

      // Check file exists
      expect(fs.existsSync(logoPath)).toBe(true);

      // Get file size
      const stats = fs.statSync(logoPath);
      const fileSizeInBytes = stats.size;
      const fileSizeInKB = fileSizeInBytes / 1024;

      // Logo must be under 500KB
      expect(fileSizeInKB).toBeLessThan(500);
    });

    test('logo GIF file is valid', async () => {
      const logoPath = path.join(IMAGES_DIR, 'logo.gif');

      // Read first bytes to verify GIF format
      const buffer = Buffer.alloc(6);
      const fd = fs.openSync(logoPath, 'r');
      fs.readSync(fd, buffer, 0, 6, 0);
      fs.closeSync(fd);

      // GIF files start with GIF87a or GIF89a
      const header = buffer.toString('ascii');
      expect(header === 'GIF87a' || header === 'GIF89a').toBe(true);
    });
  });

  test.describe('Image Optimization', () => {
    test('all images are under reasonable size limits', async () => {
      const allFiles = fs.readdirSync(IMAGES_DIR);
      const validExtensions = ['.gif', '.svg', '.png', '.jpg', '.jpeg', '.ico', '.webp'];
      const imageFiles = allFiles.filter((f) =>
        validExtensions.includes(path.extname(f).toLowerCase())
      );

      const maxSizeKB = {
        '.gif': 500, // Animated logos can be larger
        '.svg': 50, // SVGs should be compact
        '.png': 200,
        '.jpg': 200,
        '.jpeg': 200,
        '.ico': 50,
        '.webp': 200,
      };

      for (const file of imageFiles) {
        const ext = path.extname(file).toLowerCase();
        const filePath = path.join(IMAGES_DIR, file);
        const stats = fs.statSync(filePath);
        const sizeKB = stats.size / 1024;
        const limit = maxSizeKB[ext] || 500;

        expect(sizeKB).toBeLessThan(limit);
      }
    });

    test('architecture SVG is optimized', async () => {
      const svgPath = path.join(IMAGES_DIR, 'architecture.svg');

      if (fs.existsSync(svgPath)) {
        const stats = fs.statSync(svgPath);
        const sizeKB = stats.size / 1024;

        // SVG should be under 50KB
        expect(sizeKB).toBeLessThan(50);
      }
    });
  });

  test.describe('Total Asset Size', () => {
    test('total CSS size is reasonable', async () => {
      let totalSize = 0;
      const cssFiles = fs.readdirSync(CSS_DIR).filter((f) => f.endsWith('.css'));

      for (const file of cssFiles) {
        const stats = fs.statSync(path.join(CSS_DIR, file));
        totalSize += stats.size;
      }

      // Total CSS should be under 100KB
      const totalKB = totalSize / 1024;
      expect(totalKB).toBeLessThan(100);
    });

    test('total JavaScript size is reasonable', async () => {
      let totalSize = 0;
      const jsFiles = fs.readdirSync(JS_DIR).filter((f) => f.endsWith('.js'));

      for (const file of jsFiles) {
        const stats = fs.statSync(path.join(JS_DIR, file));
        totalSize += stats.size;
      }

      // Total JS should be under 200KB (includes Prism.js for syntax highlighting)
      const totalKB = totalSize / 1024;
      expect(totalKB).toBeLessThan(200);
    });

    test('HTML file size is reasonable', async () => {
      const htmlPath = path.join(ROOT_DIR, 'index.html');
      const stats = fs.statSync(htmlPath);
      const sizeKB = stats.size / 1024;

      // HTML should be under 100KB
      expect(sizeKB).toBeLessThan(100);
    });

    test('total page assets are under 1MB', async () => {
      let totalSize = 0;

      // HTML
      const htmlPath = path.join(ROOT_DIR, 'index.html');
      totalSize += fs.statSync(htmlPath).size;

      // CSS
      const cssFiles = fs.readdirSync(CSS_DIR).filter((f) => f.endsWith('.css'));
      for (const file of cssFiles) {
        totalSize += fs.statSync(path.join(CSS_DIR, file)).size;
      }

      // JavaScript
      const jsFiles = fs.readdirSync(JS_DIR).filter((f) => f.endsWith('.js'));
      for (const file of jsFiles) {
        totalSize += fs.statSync(path.join(JS_DIR, file)).size;
      }

      // Images (only valid image extensions)
      const validExtensions = ['.gif', '.svg', '.png', '.jpg', '.jpeg', '.ico', '.webp'];
      const imageFiles = fs.readdirSync(IMAGES_DIR).filter((f) =>
        validExtensions.includes(path.extname(f).toLowerCase())
      );
      for (const file of imageFiles) {
        totalSize += fs.statSync(path.join(IMAGES_DIR, file)).size;
      }

      // Total should be under 1MB
      const totalMB = totalSize / (1024 * 1024);
      expect(totalMB).toBeLessThan(1);
    });
  });

  test.describe('File Structure', () => {
    test('required asset directories exist', async () => {
      expect(fs.existsSync(CSS_DIR)).toBe(true);
      expect(fs.existsSync(JS_DIR)).toBe(true);
      expect(fs.existsSync(IMAGES_DIR)).toBe(true);
    });

    test('required CSS files exist', async () => {
      const requiredFiles = ['main.css', 'responsive.css', 'dark-mode.css'];

      for (const file of requiredFiles) {
        expect(fs.existsSync(path.join(CSS_DIR, file))).toBe(true);
      }
    });

    test('required JavaScript files exist', async () => {
      const requiredFiles = ['main.js'];

      for (const file of requiredFiles) {
        expect(fs.existsSync(path.join(JS_DIR, file))).toBe(true);
      }
    });

    test('logo file exists', async () => {
      expect(fs.existsSync(path.join(IMAGES_DIR, 'logo.gif'))).toBe(true);
    });
  });
});
