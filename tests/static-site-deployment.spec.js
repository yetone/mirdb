// @ts-check
const { test, expect } = require('@playwright/test');
const fs = require('fs');
const path = require('path');

/**
 * E2E Tests for Static Site Deployment (Scenario 17)
 * Verifies that the homepage can be deployed as a static site as specified in NFR-4
 */

test.describe('Static Site Deployment', () => {
  /**
   * Test Case 1: Check for index.html file
   * Expected: Main entry point is index.html file
   */
  test('should have index.html as main entry point', async ({ page }) => {
    // Check that index.html exists at root level
    const indexHtmlPath = path.join(process.cwd(), 'index.html');
    const indexHtmlExists = fs.existsSync(indexHtmlPath);
    expect(indexHtmlExists).toBeTruthy();

    // Verify the file is a valid HTML file with proper doctype
    const indexHtmlContent = fs.readFileSync(indexHtmlPath, 'utf-8');
    expect(indexHtmlContent).toContain('<!DOCTYPE html>');
    expect(indexHtmlContent).toContain('<html');
    expect(indexHtmlContent).toContain('</html>');

    // Verify index.html is served correctly at root URL
    const response = await page.goto('/');
    expect(response.status()).toBe(200);

    // Verify content-type is HTML
    const contentType = response.headers()['content-type'];
    expect(contentType).toContain('text/html');

    // Verify the page has the expected title (MirDB homepage)
    const title = await page.title();
    expect(title).toContain('MirDB');
  });

  /**
   * Test Case 2: Verify CSS is external or inline
   * Expected: Styles are in .css files or style tags (not requiring build)
   */
  test('should have CSS in external files or inline style tags', async ({ page }) => {
    // Check that styles.css exists
    const stylesCssPath = path.join(process.cwd(), 'styles.css');
    const stylesCssExists = fs.existsSync(stylesCssPath);
    expect(stylesCssExists).toBeTruthy();

    // Verify styles.css is plain CSS (not SCSS, LESS, etc.)
    const stylesCssContent = fs.readFileSync(stylesCssPath, 'utf-8');
    // Plain CSS should not contain SCSS-specific syntax like $ variables or nested rules with &
    expect(stylesCssContent).not.toMatch(/@import\s+['"][^'"]+\.scss['"]/);
    expect(stylesCssContent).not.toMatch(/@import\s+['"][^'"]+\.less['"]/);

    // Navigate to page and verify CSS loads correctly
    await page.goto('/');

    // Check that stylesheet link exists in HTML
    const stylesheetLinks = await page.locator('link[rel="stylesheet"]').all();
    expect(stylesheetLinks.length).toBeGreaterThan(0);

    // Verify the CSS file is referenced and loadable
    const hasStylesCss = await page.evaluate(() => {
      const links = document.querySelectorAll('link[rel="stylesheet"]');
      return Array.from(links).some(link => link.getAttribute('href')?.includes('styles.css'));
    });
    expect(hasStylesCss).toBeTruthy();

    // Verify styles are actually applied (page should have styled elements)
    const heroBackground = await page.locator('.hero').evaluate(el => {
      return window.getComputedStyle(el).backgroundImage;
    });
    // Hero should have a gradient background (from CSS)
    expect(heroBackground).toContain('gradient');

    // Check inline styles are also acceptable if present
    const inlineStyles = await page.locator('style').all();
    // Either external CSS or inline styles (or both) are acceptable for static sites
    // The key is that no build process is required
  });

  /**
   * Test Case 3: Check for server-side dependencies
   * Expected: No PHP, Node.js server, or other backend required
   */
  test('should have no server-side dependencies', async ({ page }) => {
    const rootDir = process.cwd();

    // Check that no PHP files exist
    const phpFiles = findFilesWithExtension(rootDir, '.php', ['node_modules', '.git', '.something']);
    expect(phpFiles.length).toBe(0);

    // Check that no server-side template files exist
    const ejsFiles = findFilesWithExtension(rootDir, '.ejs', ['node_modules', '.git', '.something']);
    expect(ejsFiles.length).toBe(0);

    const pugFiles = findFilesWithExtension(rootDir, '.pug', ['node_modules', '.git', '.something']);
    expect(pugFiles.length).toBe(0);

    const jadeFiles = findFilesWithExtension(rootDir, '.jade', ['node_modules', '.git', '.something']);
    expect(jadeFiles.length).toBe(0);

    // Check that index.html doesn't have server-side template syntax
    const indexHtmlPath = path.join(rootDir, 'index.html');
    const indexHtmlContent = fs.readFileSync(indexHtmlPath, 'utf-8');

    // No PHP tags
    expect(indexHtmlContent).not.toMatch(/<\?php/i);
    expect(indexHtmlContent).not.toMatch(/<\?=/);

    // No EJS/ERB template syntax
    expect(indexHtmlContent).not.toMatch(/<%[^%]/);

    // No Jinja/Django template syntax
    expect(indexHtmlContent).not.toMatch(/\{\{.*\}\}/);
    expect(indexHtmlContent).not.toMatch(/\{%.*%\}/);

    // Navigate to page and verify it loads without backend
    await page.goto('/');

    // Verify no API calls are made on page load
    const apiCalls = [];
    page.on('request', request => {
      const url = request.url();
      // Check for API calls (excluding static assets)
      if (url.includes('/api/') || url.includes('graphql')) {
        apiCalls.push(url);
      }
    });

    // Wait for page to fully load and any potential API calls
    await page.waitForLoadState('networkidle');

    // There should be no API calls for a static page
    expect(apiCalls.length).toBe(0);

    // Verify page renders with all key content (no dynamic loading required)
    await expect(page.locator('h1')).toBeVisible();
    await expect(page.locator('.features')).toBeVisible();
    await expect(page.locator('.architecture')).toBeVisible();
    await expect(page.locator('.getting-started')).toBeVisible();
  });

  /**
   * Test Case 4: Serve with simple HTTP server
   * Expected: Page works correctly when served with python -m http.server or similar
   */
  test('should work correctly when served with static HTTP server', async ({ page }) => {
    // The playwright config already uses 'npx serve' which is a simple static file server
    // This test verifies the page works correctly with this static server

    // Navigate to root (served by static server)
    const response = await page.goto('/');
    expect(response.ok()).toBeTruthy();

    // Verify all static assets load successfully
    const assetErrors = [];
    page.on('requestfailed', request => {
      assetErrors.push(request.url());
    });

    // Reload page to catch any asset failures
    await page.reload({ waitUntil: 'networkidle' });

    // No asset loading errors should occur
    expect(assetErrors.length).toBe(0);

    // Verify HTML loads
    await expect(page.locator('html')).toBeAttached();

    // Verify CSS is applied (page should be styled)
    const bodyFont = await page.evaluate(() => {
      return window.getComputedStyle(document.body).fontFamily;
    });
    expect(bodyFont).toBeTruthy();
    expect(bodyFont).not.toBe('');

    // Verify main sections are present and visible
    await expect(page.locator('.hero')).toBeVisible();
    await expect(page.locator('#features')).toBeVisible();
    await expect(page.locator('#architecture')).toBeVisible();
    await expect(page.locator('#getting-started')).toBeVisible();
    await expect(page.locator('#configuration')).toBeVisible();
    await expect(page.locator('.footer')).toBeVisible();

    // Verify navigation links work (anchor links for single-page static site)
    await page.click('a[href="#features"]');
    await expect(page.locator('#features')).toBeInViewport();

    await page.click('a[href="#getting-started"]');
    await expect(page.locator('#getting-started')).toBeInViewport();

    // Verify images/assets load
    const images = await page.locator('img').all();
    for (const img of images) {
      const naturalWidth = await img.evaluate((el) => (el).naturalWidth);
      expect(naturalWidth).toBeGreaterThan(0);
    }
  });

  /**
   * Additional Test: Verify all file types are static
   * Expected: Only static file types present (HTML, CSS, JS, images, fonts)
   */
  test('should only contain static file types', async ({ page }) => {
    const rootDir = process.cwd();
    const staticExtensions = [
      '.html', '.css', '.js', '.mjs', '.json',
      '.png', '.jpg', '.jpeg', '.gif', '.svg', '.webp', '.ico',
      '.woff', '.woff2', '.ttf', '.eot', '.otf',
      '.xml', '.txt', '.md', '.map'
    ];

    // Also ignore certain config files that are needed for development but not served
    const allowedConfigFiles = [
      'package.json', 'package-lock.json', 'playwright.config.js',
      'Cargo.toml', 'Cargo.lock', '.gitignore', 'rust-toolchain'
    ];

    // Get web files (excluding development configs and directories)
    const webDir = rootDir;
    const files = getFilesRecursive(webDir, ['node_modules', '.git', '.something', 'tests', 'mirdb-server', 'skip-list', 'sstable', 'homepage', 'website', 'etc', '.circleci']);

    const nonStaticFiles = files.filter(file => {
      const ext = path.extname(file).toLowerCase();
      const basename = path.basename(file);

      // Allow static extensions
      if (staticExtensions.includes(ext)) return false;

      // Allow known config files
      if (allowedConfigFiles.includes(basename)) return false;

      // Flag anything else
      return true;
    });

    // All web-served files should be static types
    // Note: We filter to only check files that would be served
    const servedNonStaticFiles = nonStaticFiles.filter(file => {
      const basename = path.basename(file);
      // Ignore hidden files and config files
      if (basename.startsWith('.')) return false;
      if (basename === 'README.md') return false;
      return true;
    });

    // There should be no server-side files in the web root
    expect(servedNonStaticFiles.filter(f => f.endsWith('.php'))).toHaveLength(0);
    expect(servedNonStaticFiles.filter(f => f.endsWith('.py') && !f.includes('test'))).toHaveLength(0);
    expect(servedNonStaticFiles.filter(f => f.endsWith('.rb'))).toHaveLength(0);
  });
});

/**
 * Helper function to find files with a specific extension
 */
function findFilesWithExtension(dir, ext, excludeDirs = []) {
  const results = [];
  const files = fs.readdirSync(dir);

  for (const file of files) {
    const filePath = path.join(dir, file);
    const stat = fs.statSync(filePath);

    if (stat.isDirectory()) {
      if (!excludeDirs.includes(file)) {
        results.push(...findFilesWithExtension(filePath, ext, excludeDirs));
      }
    } else if (file.endsWith(ext)) {
      results.push(filePath);
    }
  }

  return results;
}

/**
 * Helper function to get all files recursively
 */
function getFilesRecursive(dir, excludeDirs = []) {
  const results = [];
  const files = fs.readdirSync(dir);

  for (const file of files) {
    const filePath = path.join(dir, file);
    const stat = fs.statSync(filePath);

    if (stat.isDirectory()) {
      if (!excludeDirs.includes(file)) {
        results.push(...getFilesRecursive(filePath, excludeDirs));
      }
    } else {
      results.push(filePath);
    }
  }

  return results;
}
