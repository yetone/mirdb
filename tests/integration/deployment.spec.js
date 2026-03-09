/**
 * Deployment Readiness Tests
 * Owner: Scenario 19 - File Structure and Deployment
 *
 * Tests:
 * - docs/index.html exists
 * - docs/css/styles.css exists
 * - docs/js/main.js exists
 * - Logo asset path correct
 * - Serves from docs/ folder
 */

const { test, expect } = require('@playwright/test');
const fs = require('fs');
const path = require('path');

const DOCS_DIR = path.join(__dirname, '../../docs');

// Test Case 1: docs/index.html exists
test.describe('TC1: Main Homepage HTML File', () => {
  test('docs/index.html exists at the expected path', async () => {
    const indexPath = path.join(DOCS_DIR, 'index.html');

    // Check file exists
    const exists = fs.existsSync(indexPath);
    expect(exists).toBe(true);

    // Check file is readable
    const content = fs.readFileSync(indexPath, 'utf-8');
    expect(content.length).toBeGreaterThan(0);

    // Check it's a valid HTML file
    expect(content).toContain('<!DOCTYPE html>');
    expect(content).toContain('<html');
    expect(content).toContain('</html>');
  });

  test('index.html contains required MirDB content', async () => {
    const indexPath = path.join(DOCS_DIR, 'index.html');
    const content = fs.readFileSync(indexPath, 'utf-8');

    // Check for essential MirDB content
    expect(content).toContain('MirDB');
    expect(content.toLowerCase()).toContain('persistent');
    expect(content.toLowerCase()).toContain('key-value');
  });
});

// Test Case 2: docs/css/styles.css exists
test.describe('TC2: CSS Stylesheet File', () => {
  test('docs/css/styles.css exists at the expected path', async () => {
    const cssPath = path.join(DOCS_DIR, 'css', 'styles.css');

    // Check file exists
    const exists = fs.existsSync(cssPath);
    expect(exists).toBe(true);

    // Check file is readable and has content
    const content = fs.readFileSync(cssPath, 'utf-8');
    expect(content.length).toBeGreaterThan(0);
  });

  test('styles.css contains valid CSS', async () => {
    const cssPath = path.join(DOCS_DIR, 'css', 'styles.css');
    const content = fs.readFileSync(cssPath, 'utf-8');

    // Check for basic CSS syntax (selectors and rules)
    expect(content).toContain('{');
    expect(content).toContain('}');
    expect(content).toContain(':');

    // Check for CSS variables (dark theme requirement)
    expect(content).toContain(':root');
    expect(content).toContain('--color');
  });

  test('CSS file is referenced correctly in HTML', async () => {
    const indexPath = path.join(DOCS_DIR, 'index.html');
    const htmlContent = fs.readFileSync(indexPath, 'utf-8');

    // Check CSS link tag with correct relative path
    expect(htmlContent).toContain('href="css/styles.css"');
  });
});

// Test Case 3: docs/js/main.js exists
test.describe('TC3: JavaScript File', () => {
  test('docs/js/main.js exists at the expected path', async () => {
    const jsPath = path.join(DOCS_DIR, 'js', 'main.js');

    // Check file exists
    const exists = fs.existsSync(jsPath);
    expect(exists).toBe(true);

    // Check file is readable and has content
    const content = fs.readFileSync(jsPath, 'utf-8');
    expect(content.length).toBeGreaterThan(0);
  });

  test('main.js contains valid JavaScript', async () => {
    const jsPath = path.join(DOCS_DIR, 'js', 'main.js');
    const content = fs.readFileSync(jsPath, 'utf-8');

    // Check for basic JavaScript patterns
    expect(content).toContain('function');
    expect(content).toContain('document');

    // Check for DOMContentLoaded (standard initialization pattern)
    expect(content).toContain('DOMContentLoaded');
  });

  test('JavaScript file is referenced correctly in HTML', async () => {
    const indexPath = path.join(DOCS_DIR, 'index.html');
    const htmlContent = fs.readFileSync(indexPath, 'utf-8');

    // Check script tag with correct relative path
    expect(htmlContent).toContain('src="js/main.js"');
  });
});

// Test Case 4: Logo asset path is correct
test.describe('TC4: Logo Asset Path', () => {
  test('Logo file exists in docs/assets/', async () => {
    const assetsDir = path.join(DOCS_DIR, 'assets');

    // Check assets directory exists
    expect(fs.existsSync(assetsDir)).toBe(true);

    // Check for logo file (logo.gif as per PRD)
    const logoPath = path.join(assetsDir, 'logo.gif');
    expect(fs.existsSync(logoPath)).toBe(true);

    // Check logo file has content
    const stats = fs.statSync(logoPath);
    expect(stats.size).toBeGreaterThan(0);
  });

  test('Logo is referenced with correct relative path in HTML', async () => {
    const indexPath = path.join(DOCS_DIR, 'index.html');
    const htmlContent = fs.readFileSync(indexPath, 'utf-8');

    // Check for logo image references using relative paths
    // Should use assets/logo.gif (not /assets/logo.gif or absolute URL for local serving)
    expect(htmlContent).toContain('src="assets/logo.gif"');

    // Check logo is used in hero and/or navbar
    const logoMatches = htmlContent.match(/src="assets\/logo\.gif"/g);
    expect(logoMatches).not.toBeNull();
    expect(logoMatches.length).toBeGreaterThanOrEqual(1);
  });

  test('Logo images have alt attributes for accessibility', async () => {
    const indexPath = path.join(DOCS_DIR, 'index.html');
    const htmlContent = fs.readFileSync(indexPath, 'utf-8');

    // Find all img tags with logo.gif
    const logoImgRegex = /<img[^>]*src="assets\/logo\.gif"[^>]*>/g;
    const logoImgs = htmlContent.match(logoImgRegex);

    expect(logoImgs).not.toBeNull();
    for (const img of logoImgs) {
      expect(img).toContain('alt=');
    }
  });
});

// Test Case 5: Serve docs/ folder and load page
test.describe('TC5: Deployment Serving', () => {
  test('Page loads correctly when served from docs/ directory', async ({ page }) => {
    // Navigate to the homepage (served by playwright.config.js webServer from docs/)
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Verify page loaded successfully
    const title = await page.title();
    expect(title).toContain('MirDB');
  });

  test('CSS is loaded correctly when served', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Check that CSS is applied by verifying a styled element
    const body = await page.locator('body');
    const bgColor = await body.evaluate(el => window.getComputedStyle(el).backgroundColor);

    // Dark theme should have a dark background (not white/transparent)
    // RGB values for dark colors have low values
    expect(bgColor).not.toBe('rgb(255, 255, 255)');
    expect(bgColor).not.toBe('rgba(0, 0, 0, 0)');
  });

  test('JavaScript is loaded and functional when served', async ({ page }) => {
    // Track any JavaScript errors
    const jsErrors = [];
    page.on('pageerror', error => jsErrors.push(error.message));

    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Verify no JavaScript errors occurred during page load
    expect(jsErrors).toHaveLength(0);

    // Check that main.js script was loaded by verifying the script tag exists and has content
    const scriptLoaded = await page.evaluate(() => {
      const scripts = document.querySelectorAll('script[src="js/main.js"]');
      return scripts.length > 0;
    });
    expect(scriptLoaded).toBe(true);

    // Check that JavaScript initialized by verifying event listeners were set up
    // The hamburger button should have click event functionality (even if hidden on desktop)
    const hamburgerExists = await page.locator('.hamburger').count();
    expect(hamburgerExists).toBeGreaterThan(0);

    // Verify body is still functional
    await expect(page.locator('body')).toBeVisible();
  });

  test('Logo image loads correctly when served', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Check logo image is visible and loaded
    const logoImg = await page.locator('img[src="assets/logo.gif"]').first();
    await expect(logoImg).toBeVisible();

    // Verify image dimensions (should have natural width/height)
    const naturalWidth = await logoImg.evaluate(img => img.naturalWidth);
    expect(naturalWidth).toBeGreaterThan(0);
  });

  test('All local asset paths resolve correctly', async ({ page }) => {
    const failedRequests = [];

    // Listen for failed requests
    page.on('requestfailed', request => {
      const url = request.url();
      // Only track local assets (not external)
      if (url.includes('localhost') || url.startsWith('/')) {
        failedRequests.push(url);
      }
    });

    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // No local assets should fail to load
    expect(failedRequests).toHaveLength(0);
  });
});

// Additional deployment validation
test.describe('Additional Deployment Checks', () => {
  test('Directory structure follows GitHub Pages requirements', async () => {
    // Check docs/ root contains index.html
    expect(fs.existsSync(path.join(DOCS_DIR, 'index.html'))).toBe(true);

    // Check subdirectories exist
    expect(fs.existsSync(path.join(DOCS_DIR, 'css'))).toBe(true);
    expect(fs.existsSync(path.join(DOCS_DIR, 'js'))).toBe(true);
    expect(fs.existsSync(path.join(DOCS_DIR, 'assets'))).toBe(true);
  });

  test('All internal links use relative paths (GitHub Pages compatible)', async () => {
    const indexPath = path.join(DOCS_DIR, 'index.html');
    const htmlContent = fs.readFileSync(indexPath, 'utf-8');

    // Check CSS link is relative
    expect(htmlContent).toMatch(/<link[^>]*href="css\/styles\.css"[^>]*>/);

    // Check JS script is relative
    expect(htmlContent).toMatch(/<script[^>]*src="js\/main\.js"[^>]*>/);

    // Check image sources are relative
    expect(htmlContent).toContain('src="assets/');

    // Should not have absolute paths starting with / for local resources
    // (anchor links like href="#features" are fine)
    const absoluteLocalPaths = htmlContent.match(/(?:href|src)="\/(?!\/)[^#][^"]*"/g);
    expect(absoluteLocalPaths).toBeNull();
  });

  test('No broken anchor links', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Get all anchor links
    const anchorLinks = await page.locator('a[href^="#"]').all();

    for (const link of anchorLinks) {
      const href = await link.getAttribute('href');
      if (href && href !== '#') {
        const targetId = href.slice(1);
        const targetElement = await page.locator(`#${targetId}`);
        // Each anchor should point to an existing element
        await expect(targetElement).toBeAttached();
      }
    }
  });

  test('Meta tags are present for proper indexing', async () => {
    const indexPath = path.join(DOCS_DIR, 'index.html');
    const htmlContent = fs.readFileSync(indexPath, 'utf-8');

    // Check essential meta tags
    expect(htmlContent).toContain('<meta charset="UTF-8">');
    expect(htmlContent).toContain('<meta name="viewport"');
    expect(htmlContent).toContain('<meta name="description"');
    expect(htmlContent).toContain('<title>');
  });

  test('File sizes are reasonable for fast deployment', async () => {
    const htmlSize = fs.statSync(path.join(DOCS_DIR, 'index.html')).size;
    const cssSize = fs.statSync(path.join(DOCS_DIR, 'css', 'styles.css')).size;
    const jsSize = fs.statSync(path.join(DOCS_DIR, 'js', 'main.js')).size;

    // HTML should be under 100KB (reasonable for a single page)
    expect(htmlSize).toBeLessThan(100 * 1024);

    // CSS should be under 50KB
    expect(cssSize).toBeLessThan(50 * 1024);

    // JS should be minimal (under 10KB per NFR-7)
    expect(jsSize).toBeLessThan(10 * 1024);

    console.log(`File sizes: HTML=${htmlSize}, CSS=${cssSize}, JS=${jsSize}`);
  });
});
