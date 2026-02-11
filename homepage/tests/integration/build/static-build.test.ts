/**
 * Integration tests for static site build verification.
 * Owner: Scenario 15 - Static Site Build
 *
 * These tests verify that the Vite build process:
 * 1. Completes without errors
 * 2. Produces the expected static files
 * 3. Includes all required assets (logo, usage.gif)
 * 4. Can be served as a static site
 *
 * Requirements:
 * - NFR-5: Static site generator for deployment
 */

import { describe, it, expect, beforeAll } from 'vitest';
import { execSync } from 'child_process';
import * as fs from 'fs';
import * as path from 'path';

const PROJECT_ROOT = path.resolve(__dirname, '../../..');
const BUILD_OUTPUT_DIR = path.join(PROJECT_ROOT, 'dist');
const ASSETS_DIR = path.join(BUILD_OUTPUT_DIR, 'assets');

describe('Static Site Build', () => {
  beforeAll(() => {
    // Clean any existing build output
    if (fs.existsSync(BUILD_OUTPUT_DIR)) {
      fs.rmSync(BUILD_OUTPUT_DIR, { recursive: true });
    }
  });

  describe('Test Case 1: Build completes without errors', () => {
    it('should run npm run build successfully', () => {
      // Execute build command and capture output
      let buildSuccessful = false;
      let buildOutput = '';
      let buildError = '';

      try {
        buildOutput = execSync('npm run build', {
          cwd: PROJECT_ROOT,
          encoding: 'utf-8',
          stdio: 'pipe',
          timeout: 120000, // 2 minute timeout
        });
        buildSuccessful = true;
      } catch (error: unknown) {
        if (error instanceof Error && 'stderr' in error) {
          buildError = (error as { stderr?: string }).stderr || '';
        }
        buildSuccessful = false;
      }

      expect(buildSuccessful).toBe(true);
      expect(buildError).toBe('');
    });

    it('should create the dist directory', () => {
      expect(fs.existsSync(BUILD_OUTPUT_DIR)).toBe(true);
    });
  });

  describe('Test Case 2: Build output contains required files', () => {
    it('should create index.html in build output', () => {
      const indexPath = path.join(BUILD_OUTPUT_DIR, 'index.html');
      expect(fs.existsSync(indexPath)).toBe(true);
    });

    it('should have valid HTML structure in index.html', () => {
      const indexPath = path.join(BUILD_OUTPUT_DIR, 'index.html');
      const htmlContent = fs.readFileSync(indexPath, 'utf-8');

      // Check for essential HTML elements
      expect(htmlContent).toContain('<!DOCTYPE html>');
      expect(htmlContent).toContain('<html');
      expect(htmlContent).toContain('<head>');
      expect(htmlContent).toContain('<body>');
      expect(htmlContent).toContain('</html>');
      expect(htmlContent).toContain('<div id="root">');
    });

    it('should reference bundled JavaScript', () => {
      const indexPath = path.join(BUILD_OUTPUT_DIR, 'index.html');
      const htmlContent = fs.readFileSync(indexPath, 'utf-8');

      // Check for script tag with hashed JS file
      expect(htmlContent).toMatch(/<script[^>]+src="[^"]*\.js"/);
    });

    it('should reference bundled CSS', () => {
      const indexPath = path.join(BUILD_OUTPUT_DIR, 'index.html');
      const htmlContent = fs.readFileSync(indexPath, 'utf-8');

      // Check for link tag with hashed CSS file
      expect(htmlContent).toMatch(/<link[^>]+href="[^"]*\.css"/);
    });

    it('should create assets directory', () => {
      expect(fs.existsSync(ASSETS_DIR)).toBe(true);
    });

    it('should contain JavaScript bundle in assets', () => {
      const files = fs.readdirSync(ASSETS_DIR);
      const jsFiles = files.filter((f) => f.endsWith('.js'));
      expect(jsFiles.length).toBeGreaterThan(0);
    });

    it('should contain CSS bundle in assets', () => {
      const files = fs.readdirSync(ASSETS_DIR);
      const cssFiles = files.filter((f) => f.endsWith('.css'));
      expect(cssFiles.length).toBeGreaterThan(0);
    });
  });

  describe('Test Case 3: Static files can be served', () => {
    it('should have all files with valid content (not empty)', () => {
      const indexPath = path.join(BUILD_OUTPUT_DIR, 'index.html');
      const indexStats = fs.statSync(indexPath);
      expect(indexStats.size).toBeGreaterThan(0);

      const files = fs.readdirSync(ASSETS_DIR);
      for (const file of files) {
        const filePath = path.join(ASSETS_DIR, file);
        const stats = fs.statSync(filePath);
        expect(stats.size).toBeGreaterThan(0);
      }
    });

    it('should have correct MIME-type compatible file extensions', () => {
      const indexPath = path.join(BUILD_OUTPUT_DIR, 'index.html');
      expect(indexPath.endsWith('.html')).toBe(true);

      const files = fs.readdirSync(ASSETS_DIR);

      // Check that asset files have valid extensions
      const validExtensions = ['.js', '.css', '.gif', '.png', '.jpg', '.jpeg', '.svg', '.woff', '.woff2'];
      for (const file of files) {
        const ext = path.extname(file);
        expect(validExtensions).toContain(ext);
      }
    });

    it('should have proper HTML meta tags for viewport and charset', () => {
      const indexPath = path.join(BUILD_OUTPUT_DIR, 'index.html');
      const htmlContent = fs.readFileSync(indexPath, 'utf-8');

      expect(htmlContent).toContain('charset="UTF-8"');
      expect(htmlContent).toContain('viewport');
    });
  });

  describe('Test Case 4: usage.gif is included in build output', () => {
    it('should include usage.gif in assets directory', () => {
      const usageGifPath = path.join(ASSETS_DIR, 'usage.gif');
      expect(fs.existsSync(usageGifPath)).toBe(true);
    });

    it('should have non-empty usage.gif file', () => {
      const usageGifPath = path.join(ASSETS_DIR, 'usage.gif');
      const stats = fs.statSync(usageGifPath);
      expect(stats.size).toBeGreaterThan(0);
    });

    it('should have valid GIF file header for usage.gif', () => {
      const usageGifPath = path.join(ASSETS_DIR, 'usage.gif');
      const buffer = fs.readFileSync(usageGifPath);

      // GIF files start with 'GIF87a' or 'GIF89a'
      const header = buffer.slice(0, 6).toString('ascii');
      expect(header === 'GIF87a' || header === 'GIF89a').toBe(true);
    });
  });

  describe('Test Case 5: Logo is included in build output', () => {
    it('should include logo.gif in assets directory', () => {
      const logoPath = path.join(ASSETS_DIR, 'logo.gif');
      expect(fs.existsSync(logoPath)).toBe(true);
    });

    it('should have non-empty logo.gif file', () => {
      const logoPath = path.join(ASSETS_DIR, 'logo.gif');
      const stats = fs.statSync(logoPath);
      expect(stats.size).toBeGreaterThan(0);
    });

    it('should have valid GIF file header for logo.gif', () => {
      const logoPath = path.join(ASSETS_DIR, 'logo.gif');
      const buffer = fs.readFileSync(logoPath);

      // GIF files start with 'GIF87a' or 'GIF89a'
      const header = buffer.slice(0, 6).toString('ascii');
      expect(header === 'GIF87a' || header === 'GIF89a').toBe(true);
    });
  });

  describe('Additional Build Quality Checks', () => {
    it('should have title tag in HTML', () => {
      const indexPath = path.join(BUILD_OUTPUT_DIR, 'index.html');
      const htmlContent = fs.readFileSync(indexPath, 'utf-8');

      expect(htmlContent).toMatch(/<title>.*<\/title>/);
    });

    it('should have module type script for modern browser support', () => {
      const indexPath = path.join(BUILD_OUTPUT_DIR, 'index.html');
      const htmlContent = fs.readFileSync(indexPath, 'utf-8');

      expect(htmlContent).toContain('type="module"');
    });
  });
});
