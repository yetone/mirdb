// @ts-check
const { test, expect } = require('@playwright/test');
const fs = require('fs');
const path = require('path');

/**
 * Static Hosting Compatibility Tests
 *
 * These tests verify that the MirDB homepage can be hosted on static hosting
 * platforms like GitHub Pages or Netlify.
 */

test.describe('Static Hosting Compatibility', () => {

  // Test Case 1: Check for static HTML file (index.html exists as entry point)
  test('index.html exists as entry point', async () => {
    const indexPath = path.join(process.cwd(), 'index.html');
    const exists = fs.existsSync(indexPath);
    expect(exists).toBe(true);

    // Verify it's a valid HTML file
    const content = fs.readFileSync(indexPath, 'utf-8');
    expect(content).toContain('<!DOCTYPE html>');
    expect(content).toContain('<html');
    expect(content).toContain('</html>');
  });

  // Test Case 2: Verify no backend dependencies (page functions without server-side code)
  test('page functions without server-side code', async ({ page }) => {
    // Navigate to the page - it should load without any server-side rendering
    await page.goto('/');

    // Verify the page loaded successfully
    await expect(page).toHaveTitle(/MirDB/);

    // Check that all critical sections are present and rendered client-side
    await expect(page.locator('.hero')).toBeVisible();
    await expect(page.locator('.features')).toBeVisible();
    await expect(page.locator('.commands')).toBeVisible();
    await expect(page.locator('.getting-started')).toBeVisible();
    await expect(page.locator('.footer')).toBeVisible();

    // Verify there are no server-side rendering indicators
    // (no __NEXT_DATA__, no NUXT, no server-rendered comments)
    const htmlContent = await page.content();
    expect(htmlContent).not.toContain('__NEXT_DATA__');
    expect(htmlContent).not.toContain('window.__NUXT__');
    expect(htmlContent).not.toContain('data-server-rendered');
  });

  // Test Case 3: Test with simple HTTP server (page loads correctly)
  test('page loads correctly when served via static server', async ({ page, baseURL }) => {
    // The test runner uses a static server (npx serve)
    // This test verifies the page works correctly with it

    const response = await page.goto('/');

    // Check that the response is successful
    expect(response?.status()).toBe(200);

    // Verify content type is HTML
    const contentType = response?.headers()['content-type'];
    expect(contentType).toContain('text/html');

    // Verify all main content is visible
    await expect(page.locator('h1')).toContainText('MirDB');
    await expect(page.locator('.tagline')).toBeVisible();

    // Check that CSS is loaded and applied
    const heroSection = page.locator('.hero');
    // Check that the hero has styling applied (background-image for gradient or background-color)
    const heroStyle = await heroSection.evaluate(el => {
      const style = window.getComputedStyle(el);
      return {
        backgroundImage: style.backgroundImage,
        backgroundColor: style.backgroundColor
      };
    });
    // The hero should have either a background gradient or color (CSS is loaded)
    const hasBackground = heroStyle.backgroundImage !== 'none' ||
                          heroStyle.backgroundColor !== 'rgba(0, 0, 0, 0)';
    expect(hasBackground).toBe(true);
  });

  // Test Case 4: Check asset paths are relative
  test('CSS, JS, and image paths are relative or absolute for static hosting', async () => {
    const indexPath = path.join(process.cwd(), 'index.html');
    const content = fs.readFileSync(indexPath, 'utf-8');

    // Check CSS paths - should be relative (no http:// or https:// for local assets)
    const cssLinkRegex = /<link[^>]*rel=["']stylesheet["'][^>]*href=["']([^"']+)["']/gi;
    let match;
    while ((match = cssLinkRegex.exec(content)) !== null) {
      const href = match[1];
      // Local CSS should be relative paths (not starting with http/https)
      if (!href.startsWith('http://') && !href.startsWith('https://') && !href.startsWith('//')) {
        // It's a local asset - verify it's a relative path
        expect(href).not.toMatch(/^[A-Za-z]:/); // No Windows absolute paths
        // Relative paths are fine (with or without ./)
      }
    }

    // Check image paths (img src and srcset)
    const imgSrcRegex = /<(?:img|source)[^>]*(?:src|srcset)=["']([^"']+)["']/gi;
    while ((match = imgSrcRegex.exec(content)) !== null) {
      const src = match[1];
      // Local images should be relative paths
      if (!src.startsWith('http://') && !src.startsWith('https://') && !src.startsWith('//') && !src.startsWith('data:')) {
        // Check it's a relative path (starts with ./ or word character or assets/)
        const isRelative = !src.match(/^[A-Za-z]:/) && !src.match(/^\//);
        const isRootRelative = src.startsWith('/');
        expect(isRelative || isRootRelative).toBe(true);
      }
    }
  });

  // Additional test: Verify static files structure
  test('static file structure is complete', async () => {
    const rootDir = process.cwd();

    // Required files for static hosting
    const requiredFiles = [
      'index.html',
      'styles.css'
    ];

    for (const file of requiredFiles) {
      const filePath = path.join(rootDir, file);
      expect(fs.existsSync(filePath)).toBe(true);
    }

    // Verify assets directory exists
    const assetsDir = path.join(rootDir, 'assets');
    expect(fs.existsSync(assetsDir)).toBe(true);
    expect(fs.statSync(assetsDir).isDirectory()).toBe(true);
  });

  // Additional test: Verify no PHP, Python, or other server-side extensions
  test('no server-side file dependencies', async () => {
    const indexPath = path.join(process.cwd(), 'index.html');
    const content = fs.readFileSync(indexPath, 'utf-8');

    // Check there are no references to server-side files
    expect(content).not.toMatch(/\.php["']/);
    expect(content).not.toMatch(/\.py["']/);
    expect(content).not.toMatch(/\.rb["']/);
    expect(content).not.toMatch(/\.asp["']/);
    expect(content).not.toMatch(/\.jsp["']/);

    // Check there are no server-side includes
    expect(content).not.toMatch(/<%/);
    expect(content).not.toMatch(/<\?php/);
  });

  // Test: Verify page works with JavaScript disabled (basic content)
  test('page displays content without JavaScript', async ({ browser }) => {
    // Create context with JavaScript disabled
    const context = await browser.newContext({
      javaScriptEnabled: false
    });
    const page = await context.newPage();

    await page.goto('/');

    // Core content should be visible without JS
    await expect(page.locator('h1')).toContainText('MirDB');
    await expect(page.locator('.tagline')).toBeVisible();
    await expect(page.locator('.feature-card').first()).toBeVisible();
    await expect(page.locator('.command-card').first()).toBeVisible();
    await expect(page.locator('.step').first()).toBeVisible();

    await context.close();
  });

  // Test: Verify all local assets load successfully
  test('all local assets load successfully', async ({ page }) => {
    const failedRequests = [];

    page.on('requestfailed', request => {
      const url = request.url();
      // Only track local assets (not external URLs)
      if (!url.startsWith('http://') || url.includes('localhost')) {
        failedRequests.push({
          url: url,
          error: request.failure()?.errorText
        });
      }
    });

    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // No local assets should have failed to load
    expect(failedRequests).toHaveLength(0);
  });
});
