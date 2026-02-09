/**
 * Static Build and Deployment Integration Tests
 *
 * Tests that verify the build output meets NFR-1 (static deployment)
 * and NFR-2 (page load time) requirements.
 */
import { describe, it, expect, beforeAll } from 'vitest';
import { execSync } from 'child_process';
import { existsSync, readFileSync, readdirSync, statSync } from 'fs';
import { join } from 'path';

const DIST_DIR = join(__dirname, '../../dist');
const ASSETS_DIR = join(DIST_DIR, 'assets');

describe('Static Build and Deployment', () => {
  beforeAll(() => {
    // Clean and rebuild before running tests
    execSync('npm run build', {
      cwd: join(__dirname, '../..'),
      stdio: 'pipe'
    });
  });

  describe('Test Case 1: Build completes successfully', () => {
    it('should complete build without errors', () => {
      // The beforeAll already ran the build - if we reach here, it succeeded
      expect(existsSync(DIST_DIR)).toBe(true);
    });
  });

  describe('Test Case 2: Build output contains required files', () => {
    it('should create dist/ directory', () => {
      expect(existsSync(DIST_DIR)).toBe(true);
    });

    it('should contain index.html', () => {
      const indexPath = join(DIST_DIR, 'index.html');
      expect(existsSync(indexPath)).toBe(true);
    });

    it('should contain CSS files in assets/', () => {
      expect(existsSync(ASSETS_DIR)).toBe(true);
      const files = readdirSync(ASSETS_DIR);
      const cssFiles = files.filter(f => f.endsWith('.css'));
      expect(cssFiles.length).toBeGreaterThan(0);
    });

    it('should contain JS files in assets/', () => {
      expect(existsSync(ASSETS_DIR)).toBe(true);
      const files = readdirSync(ASSETS_DIR);
      const jsFiles = files.filter(f => f.endsWith('.js'));
      expect(jsFiles.length).toBeGreaterThan(0);
    });
  });

  describe('Test Case 3: CSS is minified in production build', () => {
    it('should have minified CSS (single line, no extra whitespace)', () => {
      const files = readdirSync(ASSETS_DIR);
      const cssFile = files.find(f => f.endsWith('.css'));
      expect(cssFile).toBeDefined();

      const cssContent = readFileSync(join(ASSETS_DIR, cssFile!), 'utf-8');

      // Minified CSS should have minimal newlines (may have some for readability)
      // Count newlines - minified CSS typically has very few
      const newlineCount = (cssContent.match(/\n/g) || []).length;
      // Minified CSS should have < 10 newlines typically
      expect(newlineCount).toBeLessThan(50);

      // Should not have multiple consecutive spaces (outside of content)
      expect(cssContent.includes('  ')).toBe(false);
    });

    it('should have reasonable CSS file size for minified content', () => {
      const files = readdirSync(ASSETS_DIR);
      const cssFile = files.find(f => f.endsWith('.css'));
      expect(cssFile).toBeDefined();

      const cssPath = join(ASSETS_DIR, cssFile!);
      const stats = statSync(cssPath);

      // CSS should be under 100KB for a single page app
      expect(stats.size).toBeLessThan(100 * 1024);
    });
  });

  describe('Test Case 4: JavaScript is minified in production build', () => {
    it('should have minified JS (no excessive whitespace)', () => {
      const files = readdirSync(ASSETS_DIR);
      const jsFile = files.find(f => f.endsWith('.js'));
      expect(jsFile).toBeDefined();

      const jsContent = readFileSync(join(ASSETS_DIR, jsFile!), 'utf-8');

      // Minified JS should have minimal newlines
      const newlineCount = (jsContent.match(/\n/g) || []).length;
      expect(newlineCount).toBeLessThan(50);

      // Should not have JS comments (minifier removes them)
      expect(jsContent.includes('// ')).toBe(false);
      expect(jsContent.includes('/* ')).toBe(false);
    });

    it('should have reasonable JS file size for minified content', () => {
      const files = readdirSync(ASSETS_DIR);
      const jsFile = files.find(f => f.endsWith('.js'));
      expect(jsFile).toBeDefined();

      const jsPath = join(ASSETS_DIR, jsFile!);
      const stats = statSync(jsPath);

      // JS should be under 200KB for a single page app
      expect(stats.size).toBeLessThan(200 * 1024);
    });
  });

  describe('Test Case 5: Static files can be served without backend', () => {
    it('should have self-contained HTML with proper asset references', () => {
      const indexPath = join(DIST_DIR, 'index.html');
      const htmlContent = readFileSync(indexPath, 'utf-8');

      // Should have valid HTML structure
      expect(htmlContent).toContain('<!DOCTYPE html>');
      expect(htmlContent).toContain('<html');
      expect(htmlContent).toContain('<head>');
      expect(htmlContent).toContain('<body>');

      // Should reference JS and CSS assets
      expect(htmlContent).toMatch(/<script.*src="[^"]*\.js"/);
      expect(htmlContent).toMatch(/<link.*href="[^"]*\.css"/);
    });

    it('should not require server-side rendering', () => {
      const indexPath = join(DIST_DIR, 'index.html');
      const htmlContent = readFileSync(indexPath, 'utf-8');

      // Should not have PHP, ASP, or other server-side includes
      expect(htmlContent).not.toContain('<?php');
      expect(htmlContent).not.toContain('<%');
      expect(htmlContent).not.toContain('{{');  // No template tags
    });

    it('should have all referenced assets present in dist/', () => {
      const indexPath = join(DIST_DIR, 'index.html');
      const htmlContent = readFileSync(indexPath, 'utf-8');

      // Extract asset paths from HTML
      const jsMatch = htmlContent.match(/src="([^"]*\.js)"/);
      const cssMatch = htmlContent.match(/href="([^"]*\.css)"/);

      expect(jsMatch).not.toBeNull();
      expect(cssMatch).not.toBeNull();

      // Check JS asset exists (strip leading /)
      const jsPath = jsMatch![1].replace(/^\//, '');
      expect(existsSync(join(DIST_DIR, jsPath))).toBe(true);

      // Check CSS asset exists (strip leading /)
      const cssPath = cssMatch![1].replace(/^\//, '');
      expect(existsSync(join(DIST_DIR, cssPath))).toBe(true);
    });
  });

  describe('Test Case 6: Asset paths are correct in built HTML', () => {
    it('should have valid absolute paths for assets', () => {
      const indexPath = join(DIST_DIR, 'index.html');
      const htmlContent = readFileSync(indexPath, 'utf-8');

      // JS should be in /assets/
      expect(htmlContent).toMatch(/src="\/assets\/[^"]*\.js"/);

      // CSS should be in /assets/
      expect(htmlContent).toMatch(/href="\/assets\/[^"]*\.css"/);
    });

    it('should have hashed filenames for cache busting', () => {
      const files = readdirSync(ASSETS_DIR);

      // JS files should have hash in name
      const jsFile = files.find(f => f.endsWith('.js'));
      expect(jsFile).toMatch(/index-[A-Za-z0-9]+\.js/);

      // CSS files should have hash in name
      const cssFile = files.find(f => f.endsWith('.css'));
      expect(cssFile).toMatch(/index-[A-Za-z0-9]+\.css/);
    });

    it('should copy public assets to dist/', () => {
      const files = readdirSync(ASSETS_DIR);

      // logo.gif should be copied from public/assets/
      expect(files.includes('logo.gif')).toBe(true);
    });
  });
});
