/**
 * Performance Unit Tests
 * Owner: Scenario 11 - Performance and Loading
 *
 * Tests:
 * - CSS and JS bundle size check (gzipped < 200KB)
 * - Image optimization check (modern formats, appropriate sizes)
 */

const fs = require('fs');
const path = require('path');
const zlib = require('zlib');

const PROJECT_ROOT = path.resolve(__dirname, '..');

/**
 * Get the gzipped size of a file in bytes.
 */
function getGzippedSize(filePath) {
  const content = fs.readFileSync(filePath);
  return zlib.gzipSync(content).length;
}

/**
 * Get all image files in a directory recursively.
 */
function getImageFiles(dir, files = []) {
  const entries = fs.readdirSync(dir, { withFileTypes: true });
  for (const entry of entries) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      getImageFiles(fullPath, files);
    } else if (/\.(svg|png|jpg|jpeg|webp|gif|avif)$/i.test(entry.name)) {
      files.push(fullPath);
    }
  }
  return files;
}

describe('Bundle Size Checks', () => {
  test('total CSS and JS bundle size should be less than 200KB gzipped', () => {
    const cssPath = path.join(PROJECT_ROOT, 'web', 'static', 'css', 'main.css');
    const jsPath = path.join(PROJECT_ROOT, 'web', 'src', 'js', 'main.js');

    expect(fs.existsSync(cssPath)).toBe(true);
    expect(fs.existsSync(jsPath)).toBe(true);

    const cssGzippedSize = getGzippedSize(cssPath);
    const jsGzippedSize = getGzippedSize(jsPath);
    const totalSize = cssGzippedSize + jsGzippedSize;

    // Total CSS+JS should be < 200KB (204800 bytes)
    expect(totalSize).toBeLessThan(204800);

    // Individual checks for more granular reporting
    expect(cssGzippedSize).toBeLessThan(150000); // CSS < 150KB
    expect(jsGzippedSize).toBeLessThan(50000);   // JS < 50KB
  });
});

describe('Image Optimization Checks', () => {
  test('images should use modern or optimized formats (SVG for icons)', () => {
    const imagesDir = path.join(PROJECT_ROOT, 'web', 'assets', 'images');
    expect(fs.existsSync(imagesDir)).toBe(true);

    const imageFiles = getImageFiles(imagesDir);
    expect(imageFiles.length).toBeGreaterThan(0);

    for (const imagePath of imageFiles) {
      const ext = path.extname(imagePath).toLowerCase();
      // Icons should be SVG (vector, scalable, tiny file size)
      // Photos should be WebP or AVIF
      const isVector = ext === '.svg';
      const isModernRaster = ext === '.webp' || ext === '.avif';
      const isAcceptable = isVector || isModernRaster || ext === '.png' || ext === '.jpg' || ext === '.jpeg';

      expect(isAcceptable).toBe(true);
    }
  });

  test('icon images should be SVG format for optimal size and scalability', () => {
    const imagesDir = path.join(PROJECT_ROOT, 'web', 'assets', 'images');
    const imageFiles = getImageFiles(imagesDir);

    // Feature icons should be SVG
    const iconImages = imageFiles.filter(p =>
      /link|chart|dashboard|shield/i.test(path.basename(p))
    );

    for (const iconPath of iconImages) {
      const ext = path.extname(iconPath).toLowerCase();
      expect(ext).toBe('.svg');
    }
  });

  test('individual image files should be appropriately sized (< 50KB each)', () => {
    const imagesDir = path.join(PROJECT_ROOT, 'web', 'assets', 'images');
    const imageFiles = getImageFiles(imagesDir);

    for (const imagePath of imageFiles) {
      const stats = fs.statSync(imagePath);
      // Each image should be less than 50KB
      expect(stats.size).toBeLessThan(51200);
    }
  });
});
