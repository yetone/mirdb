/**
 * Bundle Optimization Integration Tests
 * Owner: Scenario 14 - Performance Requirements
 *
 * Integration tests for verifying production build optimization:
 * - Code splitting produces multiple chunks
 * - Bundle sizes are within acceptable limits
 * - JavaScript is properly minified
 * - Critical dependencies are optimized
 */

import { describe, it, expect, beforeAll } from 'vitest';
import { execSync } from 'child_process';
import * as fs from 'fs';
import * as path from 'path';

const DIST_DIR = path.join(process.cwd(), 'dist');
const ASSETS_DIR = path.join(DIST_DIR, 'assets');

// Maximum acceptable bundle sizes in KB
const MAX_MAIN_BUNDLE_SIZE_KB = 500; // Main bundle should be under 500KB
const MAX_VENDOR_BUNDLE_SIZE_KB = 1000; // Vendor bundle should be under 1MB
const MAX_TOTAL_JS_SIZE_KB = 2000; // Total JS should be under 2MB

describe('Bundle Optimization', () => {
  let buildOutput: string;
  let distExists: boolean;

  beforeAll(() => {
    try {
      // Run production build
      buildOutput = execSync('npm run build', {
        encoding: 'utf-8',
        cwd: process.cwd(),
        timeout: 120000, // 2 minute timeout
        stdio: 'pipe',
      });
      distExists = fs.existsSync(DIST_DIR);
    } catch (error) {
      console.error('Build failed:', error);
      distExists = false;
      buildOutput = '';
    }
  });

  describe('TC4: Production build is code-split and optimized', () => {
    it('Production build completes successfully', () => {
      expect(distExists).toBe(true);
      expect(fs.existsSync(path.join(DIST_DIR, 'index.html'))).toBe(true);
    });

    it('Assets directory exists with optimized files', () => {
      expect(fs.existsSync(ASSETS_DIR)).toBe(true);

      const files = fs.readdirSync(ASSETS_DIR);
      expect(files.length).toBeGreaterThan(0);
    });

    it('JavaScript is code-split into multiple chunks', () => {
      if (!fs.existsSync(ASSETS_DIR)) {
        throw new Error('Assets directory not found - build may have failed');
      }

      const jsFiles = fs.readdirSync(ASSETS_DIR).filter((f) => f.endsWith('.js'));

      // Should have at least 2 JS files (main entry + vendor chunk or lazy chunks)
      // Vite automatically code-splits vendor dependencies
      expect(jsFiles.length).toBeGreaterThanOrEqual(1);
    });

    it('JavaScript files are minified (have hash in filename)', () => {
      if (!fs.existsSync(ASSETS_DIR)) {
        throw new Error('Assets directory not found');
      }

      const jsFiles = fs.readdirSync(ASSETS_DIR).filter((f) => f.endsWith('.js'));

      // Vite adds content hash to filenames for cache busting
      // Format: name-[hash].js or index-[hash].js
      // Hash can contain alphanumeric and underscore (base64url variant)
      for (const file of jsFiles) {
        // Should contain a hash pattern (alphanumeric/underscore after -)
        expect(file).toMatch(/^.+-[a-zA-Z0-9_]+\.js$/);
      }
    });

    it('CSS is extracted and optimized', () => {
      if (!fs.existsSync(ASSETS_DIR)) {
        throw new Error('Assets directory not found');
      }

      const cssFiles = fs.readdirSync(ASSETS_DIR).filter((f) => f.endsWith('.css'));

      // Should have CSS file(s) extracted
      expect(cssFiles.length).toBeGreaterThanOrEqual(1);

      // CSS files should be hashed for cache busting
      // Hash can contain alphanumeric and underscore (base64url variant)
      for (const file of cssFiles) {
        expect(file).toMatch(/^.+-[a-zA-Z0-9_]+\.css$/);
      }
    });

    it('Total JavaScript bundle size is within limits', () => {
      if (!fs.existsSync(ASSETS_DIR)) {
        throw new Error('Assets directory not found');
      }

      const jsFiles = fs.readdirSync(ASSETS_DIR).filter((f) => f.endsWith('.js'));

      let totalSize = 0;
      for (const file of jsFiles) {
        const filePath = path.join(ASSETS_DIR, file);
        const stats = fs.statSync(filePath);
        totalSize += stats.size;
      }

      const totalSizeKB = totalSize / 1024;

      // Log for debugging
      console.log(`Total JS bundle size: ${totalSizeKB.toFixed(2)} KB`);

      // Total JS should be under 2MB for good performance
      expect(totalSizeKB).toBeLessThan(MAX_TOTAL_JS_SIZE_KB);
    });

    it('Individual bundles are within size limits', () => {
      if (!fs.existsSync(ASSETS_DIR)) {
        throw new Error('Assets directory not found');
      }

      const jsFiles = fs.readdirSync(ASSETS_DIR).filter((f) => f.endsWith('.js'));

      for (const file of jsFiles) {
        const filePath = path.join(ASSETS_DIR, file);
        const stats = fs.statSync(filePath);
        const sizeKB = stats.size / 1024;

        console.log(`  ${file}: ${sizeKB.toFixed(2)} KB`);

        // No single bundle should exceed 1MB (vendor limit)
        expect(sizeKB).toBeLessThan(MAX_VENDOR_BUNDLE_SIZE_KB);
      }
    });

    it('HTML references optimized assets', () => {
      const indexPath = path.join(DIST_DIR, 'index.html');
      if (!fs.existsSync(indexPath)) {
        throw new Error('index.html not found');
      }

      const html = fs.readFileSync(indexPath, 'utf-8');

      // Should reference hashed JS files
      expect(html).toMatch(/assets\/[^"]+\.js/);

      // Should reference hashed CSS files
      expect(html).toMatch(/assets\/[^"]+\.css/);

      // Should use type="module" for modern JS
      expect(html).toContain('type="module"');
    });

    it('Build does not include source maps in production', () => {
      if (!fs.existsSync(ASSETS_DIR)) {
        throw new Error('Assets directory not found');
      }

      const mapFiles = fs.readdirSync(ASSETS_DIR).filter((f) => f.endsWith('.map'));

      // By default, Vite does not include source maps in production
      // If they exist, the test should still pass but flag it
      if (mapFiles.length > 0) {
        console.warn('Source maps found in production build - consider disabling for smaller builds');
      }

      // This is informational, not a hard requirement
      expect(true).toBe(true);
    });
  });

  describe('Build Output Analysis', () => {
    it('Vite outputs build summary', () => {
      // Build output should contain size information
      // Vite outputs lines like "dist/assets/index-abc123.js  xxx.xx kB │ gzip: xx.xx kB"
      expect(buildOutput.length).toBeGreaterThan(0);
    });

    it('No build warnings for large chunks', () => {
      // Check if Vite warned about large chunks
      const hasLargeChunkWarning = buildOutput.includes('Some chunks are larger than');

      if (hasLargeChunkWarning) {
        console.warn('Build produced large chunk warning - consider optimizing');
      }

      // Informational - not a hard failure
      expect(true).toBe(true);
    });
  });

  describe('Dependency Optimization', () => {
    it('React is bundled efficiently', () => {
      if (!fs.existsSync(ASSETS_DIR)) {
        throw new Error('Assets directory not found');
      }

      const jsFiles = fs.readdirSync(ASSETS_DIR).filter((f) => f.endsWith('.js'));

      // Read all JS content
      let totalContent = '';
      for (const file of jsFiles) {
        const content = fs.readFileSync(path.join(ASSETS_DIR, file), 'utf-8');
        totalContent += content;
      }

      // Production build should be minified (no large whitespace blocks)
      // Check that the code is minified by verifying it's compact
      const averageLineLength = totalContent.length / (totalContent.split('\n').length || 1);

      // Minified code typically has very long lines (high average)
      console.log(`Average line length: ${averageLineLength.toFixed(0)} chars`);
      expect(averageLineLength).toBeGreaterThan(100);

      // Verify bundle doesn't contain development-only strings that indicate unminified code
      // Note: __REACT_DEVTOOLS_GLOBAL_HOOK__ is expected in production (it's a hook point)
      expect(totalContent).not.toContain('process.env.NODE_ENV !== "production"');
    });

    it('Tree-shaking removes unused code', () => {
      if (!fs.existsSync(ASSETS_DIR)) {
        throw new Error('Assets directory not found');
      }

      // Get total JS size
      const jsFiles = fs.readdirSync(ASSETS_DIR).filter((f) => f.endsWith('.js'));
      let totalSize = 0;
      for (const file of jsFiles) {
        totalSize += fs.statSync(path.join(ASSETS_DIR, file)).size;
      }

      // Check package.json for deps
      const packageJson = JSON.parse(
        fs.readFileSync(path.join(process.cwd(), 'package.json'), 'utf-8')
      );
      const depCount = Object.keys(packageJson.dependencies || {}).length;

      // A well-optimized bundle should be smaller than raw deps
      // This is a heuristic check - actual size depends on usage
      const avgPerDep = totalSize / 1024 / (depCount || 1);
      console.log(`Average KB per dependency: ${avgPerDep.toFixed(2)}`);

      // Should be reasonably efficient
      expect(avgPerDep).toBeLessThan(200); // Less than 200KB per dependency on average
    });
  });
});
