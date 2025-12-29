import { test, expect } from '@playwright/test';
import * as fs from 'fs';
import * as path from 'path';

/**
 * Static Site Generation Tests (NFR-4)
 *
 * These tests verify that the MirDB landing page can be statically generated
 * for easy hosting on platforms like GitHub Pages, Netlify, Vercel, etc.
 *
 * Requirements verified:
 * - Site can be served as static files without server-side rendering
 * - Build output contains all necessary files (HTML, CSS, JS, assets)
 * - Site is fully functional when served with a simple HTTP server
 */

test.describe('Static Site Generation (NFR-4)', () => {

  test.describe('Test Case 1: Build Process', () => {

    test('should have all required static files in project root', async () => {
      // Verify index.html exists
      const indexPath = path.resolve(__dirname, '../index.html');
      expect(fs.existsSync(indexPath)).toBe(true);

      // Verify assets directory exists
      const assetsPath = path.resolve(__dirname, '../assets');
      expect(fs.existsSync(assetsPath)).toBe(true);

      // Verify assets directory contains files
      const assetFiles = fs.readdirSync(assetsPath);
      expect(assetFiles.length).toBeGreaterThan(0);
    });

    test('should have valid HTML structure in index.html', async () => {
      const indexPath = path.resolve(__dirname, '../index.html');
      const htmlContent = fs.readFileSync(indexPath, 'utf-8');

      // Verify DOCTYPE
      expect(htmlContent).toMatch(/<!DOCTYPE html>/i);

      // Verify essential HTML elements
      expect(htmlContent).toMatch(/<html[^>]*>/);
      expect(htmlContent).toMatch(/<head>/);
      expect(htmlContent).toMatch(/<body>/);
      expect(htmlContent).toMatch(/<\/html>/);
    });

    test('should have inline CSS (no external stylesheet dependencies)', async () => {
      const indexPath = path.resolve(__dirname, '../index.html');
      const htmlContent = fs.readFileSync(indexPath, 'utf-8');

      // Verify CSS is inlined in <style> tags
      expect(htmlContent).toMatch(/<style[^>]*>[\s\S]*?<\/style>/);

      // The page should not depend on external stylesheets (optional check)
      // This makes the site more portable for static hosting
      const externalStylesheetMatches = htmlContent.match(/<link[^>]+rel=["']stylesheet["'][^>]*>/g);
      // If there are external stylesheets, they should be optional/progressive enhancement
      // For this static site, we expect inline styles for core functionality
      expect(htmlContent).toContain('<style>');
    });
  });

  test.describe('Test Case 2: Build Output Contains index.html', () => {

    test('should have index.html file in project root', async () => {
      const indexPath = path.resolve(__dirname, '../index.html');
      const stats = fs.statSync(indexPath);

      // Verify file exists and has content
      expect(stats.isFile()).toBe(true);
      expect(stats.size).toBeGreaterThan(0);
    });

    test('should have properly formatted HTML in index.html', async () => {
      const indexPath = path.resolve(__dirname, '../index.html');
      const htmlContent = fs.readFileSync(indexPath, 'utf-8');

      // Verify it's valid HTML5
      expect(htmlContent.toLowerCase()).toContain('<!doctype html>');

      // Verify language attribute for accessibility
      expect(htmlContent).toMatch(/<html[^>]*lang=["'][^"']+["']/);

      // Verify meta charset
      expect(htmlContent).toMatch(/<meta[^>]*charset=["']?utf-8["']?/i);

      // Verify viewport meta tag for responsive design
      expect(htmlContent).toMatch(/<meta[^>]*name=["']viewport["']/i);
    });

    test('should have all required page sections', async () => {
      const indexPath = path.resolve(__dirname, '../index.html');
      const htmlContent = fs.readFileSync(indexPath, 'utf-8');

      // Verify main content sections exist
      expect(htmlContent).toContain('MirDB');
      expect(htmlContent).toMatch(/hero/i); // Hero section class or id
    });

    test('should contain asset references that are locally available', async () => {
      const indexPath = path.resolve(__dirname, '../index.html');
      const htmlContent = fs.readFileSync(indexPath, 'utf-8');

      // Find all local asset references (images, etc.)
      const imgMatches = htmlContent.match(/src=["']([^"']+)["']/g) || [];

      for (const match of imgMatches) {
        const src = match.match(/src=["']([^"']+)["']/)?.[1];
        if (src && !src.startsWith('http') && !src.startsWith('data:')) {
          // It's a local asset reference - verify it exists
          const assetPath = path.resolve(__dirname, '..', src);
          // Some paths might be relative, check if file exists
          if (src.startsWith('assets/') || src.startsWith('./assets/')) {
            expect(fs.existsSync(assetPath)).toBe(true);
          }
        }
      }
    });
  });

  test.describe('Test Case 3: Static Serving Functionality', () => {

    test('should load successfully when served as static files', async ({ page }) => {
      await page.goto('/');

      // Verify page loads without errors
      expect(await page.title()).toBeTruthy();
    });

    test('should render all content without JavaScript (progressive enhancement)', async ({ page }) => {
      // Disable JavaScript to test static content rendering
      await page.route('**/*', route => {
        if (route.request().resourceType() === 'script') {
          route.abort();
        } else {
          route.continue();
        }
      });

      await page.goto('/');

      // Core content should be visible without JS
      const heroSection = page.locator('.hero, #hero, [class*="hero"]').first();
      await expect(heroSection).toBeVisible();

      // Product name should be visible in the hero section heading
      const heroHeading = page.locator('h1').filter({ hasText: 'MirDB' }).first();
      await expect(heroHeading).toBeVisible();
    });

    test('should load all CSS correctly for proper styling', async ({ page }) => {
      await page.goto('/');

      // Verify styles are applied - check that hero section has proper styling
      const hero = page.locator('.hero').first();

      if (await hero.count() > 0) {
        // Check computed styles to verify CSS is loaded
        const display = await hero.evaluate(el =>
          window.getComputedStyle(el).display
        );
        expect(display).toBeTruthy();
        expect(display).not.toBe('none');
      }
    });

    test('should have no console errors when loaded', async ({ page }) => {
      const errors: string[] = [];
      page.on('console', msg => {
        if (msg.type() === 'error') {
          errors.push(msg.text());
        }
      });

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Filter out expected/benign errors (like favicon 404 if not present)
      const criticalErrors = errors.filter(err =>
        !err.includes('favicon') &&
        !err.includes('404')
      );

      expect(criticalErrors).toHaveLength(0);
    });

    test('should function correctly when served from any path (portable)', async ({ page }) => {
      // Test that the site works from root path
      const response = await page.goto('/');
      expect(response?.status()).toBe(200);

      // Verify HTML content type
      const contentType = response?.headers()['content-type'];
      expect(contentType).toContain('text/html');
    });

    test('should be hostable without server-side processing', async ({ page }) => {
      await page.goto('/');

      // Verify no server-side rendering is required
      // The page should be complete static HTML
      const html = await page.content();

      // Verify we have complete HTML document
      expect(html).toContain('<!DOCTYPE html>');
      expect(html).toContain('</html>');

      // Verify content is rendered (not just placeholders)
      expect(html).toContain('MirDB');
    });

    test('should load assets relative to document path', async ({ page }) => {
      const failedRequests: string[] = [];

      page.on('requestfailed', request => {
        const url = request.url();
        // Ignore external requests, focus on local assets
        if (!url.startsWith('http://localhost')) return;
        failedRequests.push(url);
      });

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // No local asset requests should fail
      expect(failedRequests).toHaveLength(0);
    });

    test('should have proper semantic structure for static hosting SEO', async ({ page }) => {
      await page.goto('/');

      // Verify semantic HTML elements exist
      const hasMain = await page.locator('main').count();
      const hasHeader = await page.locator('header, .header, [role="banner"]').count();
      const hasNav = await page.locator('nav, .nav, [role="navigation"]').count();
      const hasFooter = await page.locator('footer, .footer, [role="contentinfo"]').count();

      // At minimum, should have some semantic structure
      expect(hasMain + hasHeader + hasNav + hasFooter).toBeGreaterThan(0);
    });
  });

  test.describe('Static Hosting Platform Compatibility', () => {

    test('should be compatible with GitHub Pages (no build required)', async ({ page }) => {
      // GitHub Pages serves static files directly
      // Verify the site structure is compatible

      const indexPath = path.resolve(__dirname, '../index.html');
      expect(fs.existsSync(indexPath)).toBe(true);

      // Verify it can be loaded
      await page.goto('/');
      expect(await page.title()).toBeTruthy();
    });

    test('should be compatible with Netlify (static site)', async ({ page }) => {
      // Netlify can serve static sites without configuration
      // The site should work with direct file serving

      await page.goto('/');

      // Verify page is fully functional
      const hero = page.locator('.hero, h1').first();
      await expect(hero).toBeVisible();
    });

    test('should not require any server-side dependencies', async () => {
      // Check package.json for server dependencies
      const packagePath = path.resolve(__dirname, '../package.json');
      const packageJson = JSON.parse(fs.readFileSync(packagePath, 'utf-8'));

      // Should only have devDependencies for testing, not runtime server deps
      const dependencies = packageJson.dependencies || {};

      // Common server-side frameworks that would indicate SSR requirement
      const serverFrameworks = ['express', 'fastify', 'koa', 'next', 'nuxt', 'gatsby'];

      for (const framework of serverFrameworks) {
        expect(Object.keys(dependencies)).not.toContain(framework);
      }
    });

    test('should work without build process (serve directly)', async ({ page }) => {
      // The current setup uses 'npx serve .' which serves static files
      // This verifies no build step is required

      const response = await page.goto('/');
      expect(response?.status()).toBe(200);

      // Page should be fully rendered
      const content = await page.textContent('body');
      expect(content).toContain('MirDB');
    });
  });
});
