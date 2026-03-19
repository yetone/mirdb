/**
 * Integration tests for build output and bundle sizes.
 * Owner: Scenario 13 - Performance Optimization
 *
 * Tests:
 * - CSS bundle is gzipped and under 50KB
 * - JavaScript bundle is code-split and main bundle under 100KB gzipped
 *
 * Requirements: REQ-11, NFR-2
 */

import { execSync } from 'child_process';
import * as fs from 'fs';
import * as path from 'path';
import * as zlib from 'zlib';

const PROJECT_ROOT = path.resolve(__dirname, '../..');
const NEXT_DIR = path.join(PROJECT_ROOT, '.next');
const STATIC_DIR = path.join(NEXT_DIR, 'static');

// Helper to calculate gzipped size in bytes
function getGzippedSize(filePath: string): number {
  const content = fs.readFileSync(filePath);
  const gzipped = zlib.gzipSync(content);
  return gzipped.length;
}

// Helper to convert bytes to KB
function bytesToKB(bytes: number): number {
  return bytes / 1024;
}

// Helper to find files recursively matching a pattern
function findFiles(dir: string, pattern: RegExp): string[] {
  const results: string[] = [];

  if (!fs.existsSync(dir)) {
    return results;
  }

  const items = fs.readdirSync(dir, { withFileTypes: true });

  for (const item of items) {
    const fullPath = path.join(dir, item.name);
    if (item.isDirectory()) {
      results.push(...findFiles(fullPath, pattern));
    } else if (pattern.test(item.name)) {
      results.push(fullPath);
    }
  }

  return results;
}

describe('Build Output Tests', () => {
  beforeAll(() => {
    // Run build before tests
    console.log('Building Next.js project...');
    try {
      execSync('npm run build', {
        cwd: PROJECT_ROOT,
        stdio: 'pipe',
        timeout: 120000, // 2 minute timeout
      });
    } catch (error) {
      console.error('Build failed:', error);
      throw error;
    }
  }, 180000); // 3 minute timeout for beforeAll

  describe('CSS Bundle Size', () => {
    it('should have CSS bundle gzipped and under 50KB', () => {
      // Find all CSS files in the build output
      const cssFiles = findFiles(STATIC_DIR, /\.css$/);

      expect(cssFiles.length).toBeGreaterThan(0);

      let totalGzippedSize = 0;
      const cssFileSizes: { file: string; gzippedKB: number }[] = [];

      for (const cssFile of cssFiles) {
        const gzippedSize = getGzippedSize(cssFile);
        totalGzippedSize += gzippedSize;
        cssFileSizes.push({
          file: path.basename(cssFile),
          gzippedKB: bytesToKB(gzippedSize),
        });
      }

      const totalGzippedKB = bytesToKB(totalGzippedSize);

      console.log('CSS Bundle Analysis:');
      cssFileSizes.forEach(({ file, gzippedKB }) => {
        console.log(`  ${file}: ${gzippedKB.toFixed(2)} KB gzipped`);
      });
      console.log(`  Total CSS: ${totalGzippedKB.toFixed(2)} KB gzipped`);

      // CSS bundle should be under 50KB gzipped
      expect(totalGzippedKB).toBeLessThan(50);
    });
  });

  describe('JavaScript Bundle Size', () => {
    it('should have code-split JavaScript with main bundle under 100KB gzipped', () => {
      // Find all JS chunks in the build output
      const jsChunksDir = path.join(STATIC_DIR, 'chunks');
      const jsFiles = findFiles(jsChunksDir, /\.js$/);

      expect(jsFiles.length).toBeGreaterThan(0);

      // Code splitting verification: should have multiple chunks
      expect(jsFiles.length).toBeGreaterThan(1);

      const jsFileSizes: { file: string; gzippedKB: number }[] = [];
      let mainBundleSize = 0;

      for (const jsFile of jsFiles) {
        const gzippedSize = getGzippedSize(jsFile);
        const fileName = path.basename(jsFile);

        jsFileSizes.push({
          file: fileName,
          gzippedKB: bytesToKB(gzippedSize),
        });

        // Identify main bundle - typically the largest app chunk or main-*.js
        // Next.js uses patterns like main-*, app-*, webpack-*
        if (fileName.startsWith('main-') || fileName.includes('main.')) {
          mainBundleSize = Math.max(mainBundleSize, gzippedSize);
        }
      }

      // Sort by size descending
      jsFileSizes.sort((a, b) => b.gzippedKB - a.gzippedKB);

      console.log('JavaScript Bundle Analysis:');
      console.log(`  Total chunks: ${jsFiles.length} (code-splitting verified)`);
      jsFileSizes.slice(0, 10).forEach(({ file, gzippedKB }) => {
        console.log(`  ${file}: ${gzippedKB.toFixed(2)} KB gzipped`);
      });

      // If no explicit main bundle found, use the largest chunk
      if (mainBundleSize === 0 && jsFileSizes.length > 0) {
        mainBundleSize = jsFileSizes[0].gzippedKB * 1024;
      }

      const mainBundleKB = bytesToKB(mainBundleSize);
      console.log(`  Main bundle: ${mainBundleKB.toFixed(2)} KB gzipped`);

      // Main bundle should be under 100KB gzipped
      expect(mainBundleKB).toBeLessThan(100);
    });

    it('should verify JavaScript is minified', () => {
      const jsFiles = findFiles(STATIC_DIR, /\.js$/);

      expect(jsFiles.length).toBeGreaterThan(0);

      // Check a sample of JS files for minification characteristics
      for (const jsFile of jsFiles.slice(0, 5)) {
        const content = fs.readFileSync(jsFile, 'utf-8');

        // Minified JS typically has:
        // - Few or no newlines relative to content length
        // - Short variable names
        // - No multi-line comments (except source maps)
        const lines = content.split('\n').filter(line =>
          line.trim() && !line.trim().startsWith('//')
        );

        // Minified files should have high content density (characters per line)
        if (content.length > 1000) {
          const avgLineLength = content.length / lines.length;
          // Minified code typically has very long lines (>100 chars average)
          expect(avgLineLength).toBeGreaterThan(50);
        }
      }
    });
  });

  describe('CSS Optimization', () => {
    it('should verify CSS is minified', () => {
      const cssFiles = findFiles(STATIC_DIR, /\.css$/);

      expect(cssFiles.length).toBeGreaterThan(0);

      for (const cssFile of cssFiles) {
        const content = fs.readFileSync(cssFile, 'utf-8');

        // Minified CSS characteristics:
        // - Minimal whitespace
        // - No multi-line comments (except source maps)
        // - High character density per line
        if (content.length > 500) {
          const lines = content.split('\n').filter(line => line.trim());
          const avgLineLength = content.length / Math.max(lines.length, 1);

          // Minified CSS typically has long lines
          expect(avgLineLength).toBeGreaterThan(50);
        }
      }
    });
  });
});
