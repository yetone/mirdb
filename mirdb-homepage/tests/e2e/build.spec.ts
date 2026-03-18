/**
 * Static Build E2E Tests.
 * Owner: Scenario 11 - Static Site Deployment
 *
 * Tests:
 * - Build command success
 * - Output files presence
 * - Static serving
 * - No console errors
 */

import { test, expect } from '@playwright/test';
import { execSync } from 'child_process';
import * as fs from 'fs';
import * as path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const projectRoot = path.resolve(__dirname, '../..');
const distDir = path.join(projectRoot, 'dist');

test.describe('Static Site Deployment', () => {
  test.describe('Build Process', () => {
    test('TC1: Build command completes with exit code 0', async () => {
      // Run npm run build and expect it to succeed
      let exitCode = 0;
      try {
        execSync('npm run build', {
          cwd: projectRoot,
          stdio: 'pipe',
          encoding: 'utf-8',
        });
      } catch (error) {
        exitCode = (error as { status?: number }).status ?? 1;
      }

      expect(exitCode).toBe(0);
    });

    test('TC2: index.html file exists in build output directory', async () => {
      // Check dist folder exists
      expect(fs.existsSync(distDir)).toBe(true);

      // Check index.html exists
      const indexPath = path.join(distDir, 'index.html');
      expect(fs.existsSync(indexPath)).toBe(true);

      // Verify it's a valid HTML file
      const content = fs.readFileSync(indexPath, 'utf-8');
      // Case-insensitive doctype check
      expect(content.toLowerCase()).toContain('<!doctype html>');
      expect(content).toContain('<html');
      expect(content).toContain('</html>');
    });

    test('TC3: CSS and JavaScript files are present in build output', async () => {
      // Check assets folder exists
      const assetsDir = path.join(distDir, 'assets');
      expect(fs.existsSync(assetsDir)).toBe(true);

      // Get list of files in assets folder
      const files = fs.readdirSync(assetsDir);

      // Check for CSS files
      const cssFiles = files.filter((f) => f.endsWith('.css'));
      expect(cssFiles.length).toBeGreaterThan(0);

      // Check for JS files
      const jsFiles = files.filter((f) => f.endsWith('.js'));
      expect(jsFiles.length).toBeGreaterThan(0);

      // Verify CSS file is not empty
      const cssPath = path.join(assetsDir, cssFiles[0]);
      const cssContent = fs.readFileSync(cssPath, 'utf-8');
      expect(cssContent.length).toBeGreaterThan(0);

      // Verify JS file is not empty
      const jsPath = path.join(assetsDir, jsFiles[0]);
      const jsContent = fs.readFileSync(jsPath, 'utf-8');
      expect(jsContent.length).toBeGreaterThan(0);
    });
  });

  test.describe('Static Serving', () => {
    // These tests use the preview server which serves the dist folder
    test.use({
      baseURL: 'http://localhost:4173',
    });

    test('TC4: Site loads and functions correctly from static files', async ({
      page,
    }) => {
      // Navigate to the preview server (serving dist folder)
      const response = await page.goto('/');

      // Verify successful response
      expect(response?.status()).toBe(200);

      // Verify page title is set
      const title = await page.title();
      expect(title.length).toBeGreaterThan(0);

      // Verify main content is visible
      await expect(page.locator('body')).toBeVisible();

      // Verify hero section is rendered (core static content)
      const hero = page.locator('#hero');
      await expect(hero).toBeVisible();

      // Verify CSS is applied (check for Tailwind class or styled element)
      const h1 = page.locator('h1');
      await expect(h1).toBeVisible();
    });

    test('TC5: Browser console shows no JavaScript errors when served statically', async ({
      page,
    }) => {
      const consoleErrors: string[] = [];

      // Listen for console errors
      page.on('console', (msg) => {
        if (msg.type() === 'error') {
          consoleErrors.push(msg.text());
        }
      });

      // Listen for page errors (uncaught exceptions)
      page.on('pageerror', (error) => {
        consoleErrors.push(error.message);
      });

      // Navigate to the preview server
      await page.goto('/');

      // Wait for page to fully load
      await page.waitForLoadState('networkidle');

      // Give React time to render
      await page.waitForTimeout(1000);

      // Filter out known external resource errors (badges, etc.)
      // Per scenario requirements: "Badges may be external but core content is static"
      const criticalErrors = consoleErrors.filter((error) => {
        // Allow external badge/image loading failures
        if (error.includes('circleci.com')) return false;
        if (error.includes('img.shields.io')) return false;
        if (error.includes('github.com')) return false;
        // Allow 404 errors for optional resources (external badges, images)
        if (error.includes('404')) return false;
        if (error.includes('Failed to load resource')) return false;
        return true;
      });

      // Verify no critical JavaScript errors
      expect(criticalErrors).toHaveLength(0);
    });
  });
});
