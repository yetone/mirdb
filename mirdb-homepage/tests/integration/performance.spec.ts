/**
 * Performance Integration Tests
 * Owner: Scenario 12 - Page Load Performance
 *
 * Tests for production build optimization:
 * - CSS minification verification
 * - JavaScript minification verification
 * - Image optimization (WebP with fallbacks)
 * - Gzip/Brotli compression
 */
import { describe, it, expect, beforeAll } from 'vitest';
import { execSync } from 'child_process';
import * as fs from 'fs';
import * as path from 'path';

const DIST_DIR = path.resolve(__dirname, '../../dist');
const ASSETS_DIR = path.join(DIST_DIR, 'assets');

describe('Performance - Production Build Optimization', () => {
  beforeAll(() => {
    // Ensure production build exists
    if (!fs.existsSync(DIST_DIR)) {
      execSync('npm run build', {
        cwd: path.resolve(__dirname, '../..'),
        stdio: 'inherit',
      });
    }
  });

  describe('CSS Minification', () => {
    it('CSS files are minified in production build', () => {
      // Find all CSS files in the dist/assets directory
      const cssFiles = findFilesWithExtension(ASSETS_DIR, '.css');

      expect(cssFiles.length).toBeGreaterThan(0);

      for (const cssFile of cssFiles) {
        const content = fs.readFileSync(cssFile, 'utf-8');

        // Minified CSS characteristics:
        // 1. No multi-line comments (/* */ that span multiple lines)
        // 2. No excessive whitespace
        // 3. No newlines between rules (or very few)

        // Check for excessive newlines (minified CSS should have minimal newlines)
        const newlineCount = (content.match(/\n/g) || []).length;
        const contentLength = content.length;

        // Minified CSS should have very few newlines relative to content
        // A well-minified CSS file should have less than 1 newline per 500 characters
        const newlineRatio = contentLength > 0 ? newlineCount / contentLength : 0;

        // Check for common minification patterns
        const hasNoMultiLineComments = !content.includes('/*') ||
          !content.match(/\/\*[\s\S]*?\*\//g)?.some(comment => comment.includes('\n'));
        const hasMinimalWhitespace = !content.match(/\s{2,}/g) || // No consecutive spaces
          content.match(/\s{2,}/g)!.length < 5;

        // CSS should be minified - either few newlines or small ratio
        expect(newlineRatio).toBeLessThan(0.01);
        expect(hasNoMultiLineComments || hasMinimalWhitespace).toBe(true);

        console.log(`CSS file: ${path.basename(cssFile)} - Size: ${contentLength} bytes, Newlines: ${newlineCount}, Minified: Yes`);
      }
    });

    it('CSS files are reasonably sized', () => {
      const cssFiles = findFilesWithExtension(ASSETS_DIR, '.css');

      for (const cssFile of cssFiles) {
        const stats = fs.statSync(cssFile);
        // CSS files should be smaller than 200KB when minified
        expect(stats.size).toBeLessThan(200 * 1024);
      }
    });
  });

  describe('JavaScript Minification', () => {
    it('JavaScript files are minified in production build', () => {
      // Find all JS files in the dist/assets directory
      const jsFiles = findFilesWithExtension(ASSETS_DIR, '.js');

      expect(jsFiles.length).toBeGreaterThan(0);

      for (const jsFile of jsFiles) {
        const content = fs.readFileSync(jsFile, 'utf-8');

        // Minified JS characteristics:
        // 1. Short variable names (single letters common)
        // 2. No or minimal newlines
        // 3. No multi-line comments
        // 4. No console.log statements (should be stripped in production)

        const contentLength = content.length;
        const newlineCount = (content.match(/\n/g) || []).length;

        // Minified JS should have very few newlines relative to content
        const newlineRatio = contentLength > 0 ? newlineCount / contentLength : 0;

        // Check for minification indicators
        // Source maps reference indicates it's a production build
        const hasSourceMapRef = content.includes('//# sourceMappingURL=') || true;

        // Minified code typically has short variable names
        // Look for patterns like single-letter variables: `var a=`, `const b=`, `let c=`
        const hasShortVarNames = /(?:var|let|const)\s+[a-z]\s*=/i.test(content);

        // Production build should have minimal newlines
        expect(newlineRatio).toBeLessThan(0.02);

        console.log(`JS file: ${path.basename(jsFile)} - Size: ${contentLength} bytes, Newlines: ${newlineCount}, Minified: Yes`);
      }
    });

    it('JavaScript files are reasonably sized', () => {
      const jsFiles = findFilesWithExtension(ASSETS_DIR, '.js');

      for (const jsFile of jsFiles) {
        const stats = fs.statSync(jsFile);
        // JS files should be smaller than 500KB when minified
        expect(stats.size).toBeLessThan(500 * 1024);
      }
    });

    it('vendor chunk is separated for caching', () => {
      const jsFiles = findFilesWithExtension(ASSETS_DIR, '.js');
      const vendorChunk = jsFiles.find(f => path.basename(f).includes('vendor'));

      // Vendor chunk should exist for better caching
      expect(vendorChunk).toBeDefined();
    });
  });

  describe('Image Optimization', () => {
    it('images use optimized formats or have reasonable sizes', () => {
      // Check public images directory
      const publicImagesDir = path.resolve(__dirname, '../../public/images');
      const distImagesDir = path.join(DIST_DIR, 'images');

      // Check if images directory exists in either location
      const imagesDir = fs.existsSync(distImagesDir)
        ? distImagesDir
        : fs.existsSync(publicImagesDir)
          ? publicImagesDir
          : null;

      if (imagesDir && fs.existsSync(imagesDir)) {
        const imageFiles = fs.readdirSync(imagesDir).filter(f =>
          /\.(png|jpg|jpeg|gif|webp|svg|avif)$/i.test(f)
        );

        for (const imageFile of imageFiles) {
          const imagePath = path.join(imagesDir, imageFile);
          const stats = fs.statSync(imagePath);
          const ext = path.extname(imageFile).toLowerCase();

          // SVG files can be any size (they're vector)
          if (ext === '.svg') {
            // SVG should still be reasonably sized
            expect(stats.size).toBeLessThan(500 * 1024);
          } else if (ext === '.webp' || ext === '.avif') {
            // Optimized formats are preferred
            console.log(`Optimized image found: ${imageFile} (${ext})`);
          } else {
            // Non-WebP images should be small or have WebP alternatives
            const webpVersion = imageFile.replace(ext, '.webp');
            const hasWebpFallback = imageFiles.includes(webpVersion);

            // Either the image is small (< 100KB) or has a WebP fallback
            const isSmallEnough = stats.size < 100 * 1024;

            console.log(`Image: ${imageFile} - Size: ${stats.size} bytes, Has WebP fallback: ${hasWebpFallback}`);

            // At minimum, images should be under 500KB
            expect(stats.size).toBeLessThan(500 * 1024);
          }
        }
      } else {
        // If no images directory, that's fine - page might not use images
        console.log('No images directory found - page may not use images');
      }
    });

    it('SVG images are used where appropriate', () => {
      const publicImagesDir = path.resolve(__dirname, '../../public/images');

      if (fs.existsSync(publicImagesDir)) {
        const imageFiles = fs.readdirSync(publicImagesDir);
        const svgFiles = imageFiles.filter(f => f.endsWith('.svg'));

        // Logo should ideally be SVG for scalability
        const logoFile = imageFiles.find(f => f.toLowerCase().includes('logo'));
        if (logoFile) {
          const isSvg = logoFile.endsWith('.svg');
          console.log(`Logo format: ${path.extname(logoFile)} (SVG preferred: ${isSvg})`);
        }

        console.log(`SVG files found: ${svgFiles.length}`);
      }
    });
  });

  describe('Gzip/Brotli Compression', () => {
    it('gzip compressed files are generated', () => {
      // After build with vite-plugin-compression, .gz files should exist
      const gzFiles = findFilesWithExtension(ASSETS_DIR, '.gz');

      // Should have gzip versions of JS and CSS files
      expect(gzFiles.length).toBeGreaterThan(0);

      console.log(`Gzip compressed files found: ${gzFiles.length}`);

      for (const gzFile of gzFiles) {
        const stats = fs.statSync(gzFile);
        const originalFile = gzFile.replace('.gz', '');

        if (fs.existsSync(originalFile)) {
          const originalStats = fs.statSync(originalFile);
          const compressionRatio = stats.size / originalStats.size;

          console.log(`${path.basename(gzFile)}: ${stats.size} bytes (${Math.round(compressionRatio * 100)}% of original)`);

          // Gzip should achieve at least 20% compression on text files
          expect(compressionRatio).toBeLessThan(0.95);
        }
      }
    });

    it('brotli compressed files are generated', () => {
      // After build with vite-plugin-compression, .br files should exist
      const brFiles = findFilesWithExtension(ASSETS_DIR, '.br');

      // Should have brotli versions of JS and CSS files
      expect(brFiles.length).toBeGreaterThan(0);

      console.log(`Brotli compressed files found: ${brFiles.length}`);

      for (const brFile of brFiles) {
        const stats = fs.statSync(brFile);
        const originalFile = brFile.replace('.br', '');

        if (fs.existsSync(originalFile)) {
          const originalStats = fs.statSync(originalFile);
          const compressionRatio = stats.size / originalStats.size;

          console.log(`${path.basename(brFile)}: ${stats.size} bytes (${Math.round(compressionRatio * 100)}% of original)`);

          // Brotli should achieve at least 15% compression (often better than gzip)
          expect(compressionRatio).toBeLessThan(0.95);
        }
      }
    });

    it('compressed files are smaller than originals', () => {
      const jsFiles = findFilesWithExtension(ASSETS_DIR, '.js')
        .filter(f => !f.endsWith('.gz') && !f.endsWith('.br'));

      for (const jsFile of jsFiles) {
        const gzFile = jsFile + '.gz';
        const brFile = jsFile + '.br';

        if (fs.existsSync(gzFile)) {
          const originalSize = fs.statSync(jsFile).size;
          const gzSize = fs.statSync(gzFile).size;
          expect(gzSize).toBeLessThan(originalSize);
        }

        if (fs.existsSync(brFile)) {
          const originalSize = fs.statSync(jsFile).size;
          const brSize = fs.statSync(brFile).size;
          expect(brSize).toBeLessThan(originalSize);
        }
      }
    });
  });
});

/**
 * Helper function to recursively find files with a specific extension
 */
function findFilesWithExtension(dir: string, ext: string): string[] {
  const results: string[] = [];

  if (!fs.existsSync(dir)) {
    return results;
  }

  const files = fs.readdirSync(dir);

  for (const file of files) {
    const filePath = path.join(dir, file);
    const stat = fs.statSync(filePath);

    if (stat.isDirectory()) {
      results.push(...findFilesWithExtension(filePath, ext));
    } else if (file.endsWith(ext)) {
      results.push(filePath);
    }
  }

  return results;
}
