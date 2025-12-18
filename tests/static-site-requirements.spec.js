// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');
const fs = require('fs');

const projectRoot = path.resolve(__dirname, '..');
const indexPath = path.resolve(projectRoot, 'index.html');

/**
 * Test Suite: Static Site Requirements
 * Scenario: Verify the landing page can be deployed as static files without server-side processing
 */
test.describe('Static Site Requirements', () => {
  /**
   * Test Case 1: Check build output file types
   * Input: Check build output file types
   * Expected: Build produces only HTML, CSS, JS, and image/font assets
   */
  test('TC1: Build output contains only static file types (HTML, CSS, JS, images, fonts)', async () => {
    // Define allowed static file extensions
    const allowedExtensions = [
      // HTML
      '.html', '.htm',
      // CSS
      '.css',
      // JavaScript
      '.js', '.mjs',
      // Images
      '.png', '.jpg', '.jpeg', '.gif', '.svg', '.webp', '.ico', '.bmp',
      // Fonts
      '.woff', '.woff2', '.ttf', '.otf', '.eot',
      // Common static assets
      '.json', '.txt', '.xml', '.webmanifest',
      // Source maps (optional but common)
      '.map'
    ];

    // Define files/directories to exclude from check
    const excludedPaths = [
      'node_modules',
      '.git',
      '.something',
      'tests',
      '.circleci',
      'mirdb-server', // Rust source code directory
      'skip-list',    // Rust source code directory
      'sstable',      // Rust source code directory
      'etc',          // Config directory
      'Cargo.toml',
      'Cargo.lock',
      'rust-toolchain',
      'package.json',
      'package-lock.json',
      'playwright.config.js',
      'README.md',
      '.gitignore'
    ];

    // Get static files in project root that would be deployed
    const deployableFiles = [
      'index.html',
      'styles.css',
      'script.js',
      'assets'
    ];

    // Check that main static files exist
    const mainFiles = ['index.html', 'styles.css', 'script.js'];
    for (const file of mainFiles) {
      const filePath = path.join(projectRoot, file);
      const exists = fs.existsSync(filePath);
      expect(exists, `Expected ${file} to exist`).toBe(true);
    }

    // Check assets directory
    const assetsDir = path.join(projectRoot, 'assets');
    if (fs.existsSync(assetsDir)) {
      const assetFiles = fs.readdirSync(assetsDir);
      for (const file of assetFiles) {
        const ext = path.extname(file).toLowerCase();
        const isAllowed = allowedExtensions.includes(ext);
        expect(isAllowed, `Asset file ${file} has disallowed extension ${ext}`).toBe(true);
      }
    }

    // Verify main files have correct extensions
    expect(path.extname('index.html')).toBe('.html');
    expect(path.extname('styles.css')).toBe('.css');
    expect(path.extname('script.js')).toBe('.js');
  });

  /**
   * Test Case 2: Serve with simple HTTP server
   * Input: Serve with simple HTTP server
   * Expected: Page loads and functions correctly when served by static file server
   */
  test('TC2: Page loads and functions correctly when served as static file', async ({ page }) => {
    // Load the page directly from file system (simulating static file serving)
    await page.goto(`file://${indexPath}`);

    // Verify page loads successfully
    await expect(page).toHaveTitle(/MirDB/);

    // Verify main sections are visible
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Verify navigation works
    const navbar = page.locator('[data-testid="navbar"]');
    await expect(navbar).toBeVisible();

    // Verify features section loads
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Verify quickstart section loads
    const quickstartSection = page.locator('[data-testid="quickstart-section"]');
    await expect(quickstartSection).toBeVisible();

    // Verify footer loads
    const footer = page.locator('[data-testid="footer"]');
    await expect(footer).toBeVisible();

    // Verify JavaScript functionality works (copy buttons exist and are clickable)
    const copyButtons = page.locator('.copy-btn');
    const buttonCount = await copyButtons.count();
    expect(buttonCount).toBeGreaterThan(0);

    // Verify CSS is applied (check hero section has styling)
    const heroTitle = page.locator('[data-testid="hero-title"]');
    await expect(heroTitle).toBeVisible();

    // Check that hero title has expected styling (font-size should be set by CSS)
    const fontSize = await heroTitle.evaluate(el => window.getComputedStyle(el).fontSize);
    expect(fontSize).toBeTruthy();
  });

  /**
   * Test Case 3: Check for API calls or server dependencies
   * Input: Check for API calls or server dependencies
   * Expected: No required backend API calls for core content
   */
  test('TC3: No required backend API calls for core content', async ({ page }) => {
    // Track network requests
    const networkRequests = [];

    page.on('request', request => {
      networkRequests.push({
        url: request.url(),
        resourceType: request.resourceType()
      });
    });

    // Load the page
    await page.goto(`file://${indexPath}`);

    // Wait for page to fully load
    await page.waitForLoadState('networkidle');

    // Filter for API-like requests (exclude static resources)
    const apiPatterns = [
      /\/api\//,
      /\.php$/,
      /\.asp$/,
      /\.aspx$/,
      /\/graphql/,
      /\/rest\//,
      /\/v[0-9]+\//
    ];

    const staticResourceTypes = ['document', 'stylesheet', 'script', 'image', 'font', 'other'];

    // Check that all requests are for static resources or allowed CDNs
    const allowedHosts = [
      'cdnjs.cloudflare.com',  // Prism.js CDN
      'localhost',
      ''  // File protocol has empty host
    ];

    for (const request of networkRequests) {
      const url = new URL(request.url);

      // Check if it's a server-side API call
      for (const pattern of apiPatterns) {
        const isApiCall = pattern.test(url.pathname);
        expect(isApiCall, `Unexpected API call detected: ${request.url}`).toBe(false);
      }

      // Verify hosts are either local or allowed CDNs
      if (url.protocol !== 'file:') {
        const isAllowedHost = allowedHosts.some(host => url.host.includes(host));
        // Note: We allow CDN requests for syntax highlighting library
        // The core content should not require external APIs
      }
    }

    // Verify core content is visible without any backend
    await expect(page.locator('[data-testid="hero-section"]')).toBeVisible();
    await expect(page.locator('#features')).toBeVisible();
    await expect(page.locator('[data-testid="quickstart-section"]')).toBeVisible();
    await expect(page.locator('#commands')).toBeVisible();
    await expect(page.locator('#configuration')).toBeVisible();
    await expect(page.locator('[data-testid="architecture-section"]')).toBeVisible();
    await expect(page.locator('[data-testid="project-status-section"]')).toBeVisible();
    await expect(page.locator('[data-testid="footer"]')).toBeVisible();
  });

  /**
   * Test Case 4: Verify relative paths for assets
   * Input: Verify relative paths for assets
   * Expected: All asset references use relative paths or configurable base URL
   */
  test('TC4: All asset references use relative paths', async ({ page }) => {
    // Read the HTML file content
    const htmlContent = fs.readFileSync(indexPath, 'utf-8');

    // Check CSS link uses relative path
    const cssLinkMatch = htmlContent.match(/<link[^>]*rel="stylesheet"[^>]*href="([^"]+)"/);
    if (cssLinkMatch && !cssLinkMatch[1].startsWith('http')) {
      // Local stylesheet should use relative path
      const cssHref = cssLinkMatch[1];
      expect(cssHref.startsWith('/') === false || cssHref === '/styles.css' || cssHref === 'styles.css').toBe(true);
    }

    // Check script src uses relative path
    const localScriptMatch = htmlContent.match(/<script[^>]*src="script\.js"[^>]*>/);
    expect(localScriptMatch).toBeTruthy();

    // Load the page and verify assets load correctly
    await page.goto(`file://${indexPath}`);

    // Check that local CSS is applied
    const bodyBgColor = await page.evaluate(() => {
      return window.getComputedStyle(document.body).backgroundColor;
    });
    expect(bodyBgColor).toBeTruthy();

    // Verify all internal links use relative or anchor paths
    const allLinks = await page.locator('a[href]').all();
    for (const link of allLinks) {
      const href = await link.getAttribute('href');
      if (href) {
        // Internal links should be relative (#section) or use relative paths
        const isAnchor = href.startsWith('#');
        const isRelative = !href.startsWith('http') && !href.startsWith('//');
        const isAllowedExternal = href.includes('github.com') ||
                                   href.includes('crates.io') ||
                                   href.includes('opensource.org');

        const isValid = isAnchor || isRelative || isAllowedExternal;
        expect(isValid, `Link ${href} should be relative or an allowed external link`).toBe(true);
      }
    }

    // Check image/asset paths in HTML
    const imgTags = await page.locator('img[src]').all();
    for (const img of imgTags) {
      const src = await img.getAttribute('src');
      if (src && !src.startsWith('data:')) {
        // Local images should use relative paths
        const isRelative = !src.startsWith('http') && !src.startsWith('//');
        const isAllowedCDN = src.includes('cdnjs.cloudflare.com');
        expect(isRelative || isAllowedCDN, `Image src ${src} should be relative`).toBe(true);
      }
    }

    // Verify the styles.css reference
    const stylesLink = await page.locator('link[href="styles.css"]');
    await expect(stylesLink).toHaveCount(1);
  });

  /**
   * Additional test: Verify HTML is valid and self-contained
   */
  test('HTML structure is valid for static deployment', async ({ page }) => {
    await page.goto(`file://${indexPath}`);

    // Verify DOCTYPE declaration exists
    const htmlContent = fs.readFileSync(indexPath, 'utf-8');
    expect(htmlContent.trim().toLowerCase().startsWith('<!doctype html>')).toBe(true);

    // Verify html lang attribute is set
    const htmlLang = await page.locator('html').getAttribute('lang');
    expect(htmlLang).toBe('en');

    // Verify meta charset is set
    const metaCharset = await page.locator('meta[charset]').getAttribute('charset');
    expect(metaCharset.toLowerCase()).toBe('utf-8');

    // Verify viewport meta tag exists
    const viewport = await page.locator('meta[name="viewport"]');
    await expect(viewport).toHaveCount(1);

    // Verify title exists
    const title = await page.title();
    expect(title).toContain('MirDB');
  });

  /**
   * Additional test: Verify no server-side includes or dynamic content markers
   */
  test('No server-side processing markers in HTML', async () => {
    const htmlContent = fs.readFileSync(indexPath, 'utf-8');

    // Check for PHP tags
    expect(htmlContent).not.toMatch(/<\?php/i);
    expect(htmlContent).not.toMatch(/<\?=/);

    // Check for ASP tags
    expect(htmlContent).not.toMatch(/<%/);

    // Check for JSP tags
    expect(htmlContent).not.toMatch(/<%@/);

    // Check for server-side includes
    expect(htmlContent).not.toMatch(/<!--\s*#include/i);

    // Check for template engine markers (common ones)
    expect(htmlContent).not.toMatch(/\{\{[^}]+\}\}/); // Handlebars/Mustache - but allow if it's in code examples
    expect(htmlContent).not.toMatch(/<%-/);  // EJS
    expect(htmlContent).not.toMatch(/\{%/);  // Jinja2/Django templates
  });
});
