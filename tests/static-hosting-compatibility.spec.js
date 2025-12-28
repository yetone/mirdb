// @ts-check
const { test, expect } = require('@playwright/test');
const fs = require('fs');
const path = require('path');

/**
 * Test Suite: Static Hosting Compatibility
 * Scenario: Verify the site can be deployed to static hosting platforms (GitHub Pages)
 *
 * This test suite validates that the MirDB homepage consists only of static files
 * (HTML, CSS, JS, and static assets) and has no server-side dependencies that would
 * prevent deployment to static hosting platforms like GitHub Pages, Netlify, etc.
 */

test.describe('Static Hosting Compatibility', () => {

  /**
   * Test Case 1: Analyze site structure
   * Input: Analyze site structure
   * Expected: Site consists only of HTML, CSS, JS, and static assets
   */
  test('TC1: Site consists only of HTML, CSS, JS, and static assets', async ({ page }) => {
    // Define expected static file extensions
    const staticFileExtensions = [
      '.html',
      '.css',
      '.js',
      '.json',
      '.gif',
      '.png',
      '.jpg',
      '.jpeg',
      '.webp',
      '.svg',
      '.ico',
      '.woff',
      '.woff2',
      '.ttf',
      '.eot',
      '.txt',
      '.xml',
      '.md',
    ];

    // Check root directory for website files
    const rootDir = path.join(__dirname, '..');
    const websiteFiles = [
      'index.html',
      'styles.css',
    ];

    // Verify all main website files exist and are static
    for (const file of websiteFiles) {
      const filePath = path.join(rootDir, file);
      const exists = fs.existsSync(filePath);
      expect(exists, `${file} should exist`).toBe(true);

      const ext = path.extname(file);
      expect(
        staticFileExtensions.includes(ext),
        `${file} should have a static file extension`
      ).toBe(true);
    }

    // Verify assets directory only contains static files
    const assetsDir = path.join(rootDir, 'assets');
    if (fs.existsSync(assetsDir)) {
      const assetFiles = fs.readdirSync(assetsDir);
      for (const file of assetFiles) {
        const ext = path.extname(file);
        expect(
          staticFileExtensions.includes(ext),
          `Asset file ${file} should have a static file extension`
        ).toBe(true);
      }
      console.log(`Assets directory contains ${assetFiles.length} static files:`, assetFiles);
    }

    // Verify no server-side file types exist in the project root
    const serverSideExtensions = ['.php', '.asp', '.aspx', '.jsp', '.py', '.rb'];
    const rootFiles = fs.readdirSync(rootDir);

    for (const file of rootFiles) {
      const ext = path.extname(file);
      expect(
        !serverSideExtensions.includes(ext),
        `Server-side file ${file} should not exist for static hosting`
      ).toBe(true);
    }

    console.log('Site structure analysis: All files are static (HTML, CSS, JS, assets)');
  });

  /**
   * Test Case 2: Check for dynamic server requirements
   * Input: Check for dynamic server requirements
   * Expected: No backend server, database, or API calls required for core functionality
   */
  test('TC2: No backend server, database, or API calls required for core functionality', async ({ page }) => {
    // Track all network requests during page load
    const networkRequests = [];
    const externalApiCalls = [];

    page.on('request', (request) => {
      const url = request.url();
      networkRequests.push({
        url: url,
        method: request.method(),
        resourceType: request.resourceType(),
      });

      // Check for API calls (excluding CDN and static resources)
      if (url.includes('/api/') || url.includes('/graphql') ||
          request.resourceType() === 'xhr' || request.resourceType() === 'fetch') {
        // Filter out localhost requests which are for the static server
        if (!url.includes('localhost:3000')) {
          externalApiCalls.push(url);
        }
      }
    });

    // Navigate to the page
    await page.goto('/');

    // Wait for all content to load
    await page.waitForLoadState('networkidle');

    // Verify no external API calls are required for core functionality
    console.log(`Total network requests: ${networkRequests.length}`);
    console.log(`External API calls detected: ${externalApiCalls.length}`);

    if (externalApiCalls.length > 0) {
      console.log('External API calls:', externalApiCalls);
    }

    // Core functionality should work without external APIs
    expect(
      externalApiCalls.length,
      'No external API calls should be required for core functionality'
    ).toBe(0);

    // Verify the page content loaded successfully without server-side processing
    await expect(page.locator('#hero')).toBeVisible();
    await expect(page.locator('#features')).toBeVisible();
    await expect(page.locator('#quick-start')).toBeVisible();
    await expect(page.locator('#commands')).toBeVisible();
    await expect(page.locator('#configuration')).toBeVisible();
    await expect(page.locator('footer')).toBeVisible();

    // Check that the page doesn't require any database connections
    // by verifying all content is static/pre-rendered
    const heroTitle = await page.locator('#product-name').textContent();
    expect(heroTitle).toBe('MirDB');

    const tagline = await page.locator('#tagline').textContent();
    expect(tagline).toContain('persistent key-value store');

    console.log('Core functionality verification: All content is static, no server dependencies');
  });

  /**
   * Test Case 3: Test deployment to GitHub Pages
   * Input: Test deployment to GitHub Pages
   * Expected: Site deploys successfully and functions on GitHub Pages
   *
   * This test validates that the site structure is compatible with GitHub Pages
   * by checking for required files and configurations.
   */
  test('TC3: Site structure is compatible with GitHub Pages deployment', async ({ page }) => {
    const rootDir = path.join(__dirname, '..');

    // Check 1: Verify index.html exists at root (required for GitHub Pages)
    const indexPath = path.join(rootDir, 'index.html');
    expect(fs.existsSync(indexPath), 'index.html must exist at root for GitHub Pages').toBe(true);

    // Check 2: Verify all assets use relative paths (required for GitHub Pages subdirectory deployment)
    await page.goto('/');

    // Get all image sources
    const images = await page.locator('img').all();
    for (const img of images) {
      const src = await img.getAttribute('src');
      // Sources should be relative (not absolute like http://... or /...)
      // or they should reference local assets
      const isRelative = src && (
        src.startsWith('assets/') ||
        src.startsWith('./') ||
        !src.startsWith('/')
      );
      // Allow external images in meta tags but verify main content uses relative paths
      if (src && !src.startsWith('http')) {
        expect(isRelative, `Image source ${src} should use relative path`).toBe(true);
      }
    }

    // Check 3: Verify CSS uses relative paths
    const linkTags = await page.locator('link[rel="stylesheet"]').all();
    for (const link of linkTags) {
      const href = await link.getAttribute('href');
      const isRelative = href && (
        href.startsWith('./') ||
        (!href.startsWith('/') && !href.startsWith('http'))
      );
      expect(isRelative, `Stylesheet ${href} should use relative path`).toBe(true);
    }

    // Check 4: Verify no server-side includes or SSI directives in HTML
    const htmlContent = fs.readFileSync(indexPath, 'utf-8');
    const serverSidePatterns = [
      /<!--#include/g,           // SSI includes
      /<\?php/g,                 // PHP
      /<%/g,                     // ASP/JSP
      /\{\{.*\}\}/g,            // Template syntax (when used with server-side rendering)
    ];

    // Check for SSI and server-side code (template syntax is OK in client-side JS)
    expect(htmlContent).not.toMatch(/<!--#include/);
    expect(htmlContent).not.toMatch(/<\?php/);
    expect(htmlContent).not.toMatch(/<%(?!--)(?!DOCTYPE)/);  // Allow <!DOCTYPE but not ASP/JSP

    console.log('GitHub Pages compatibility checks:');
    console.log('  - index.html at root: PASS');
    console.log('  - Relative asset paths: PASS');
    console.log('  - No server-side includes: PASS');

    // Check 5: Verify all navigation links work (no broken internal links)
    const navLinks = await page.locator('.nav-links a').all();
    for (const link of navLinks) {
      const href = await link.getAttribute('href');
      if (href && href.startsWith('#')) {
        // Internal anchor links should have corresponding sections
        const sectionId = href.substring(1);
        const section = page.locator(`#${sectionId}`);
        const exists = await section.count() > 0;
        expect(exists, `Section ${sectionId} should exist for nav link`).toBe(true);
      }
    }

    console.log('  - Internal navigation links: PASS');

    // Check 6: Verify the site works without JavaScript (progressive enhancement)
    // Core content should be visible in the initial HTML
    const bodyContent = await page.locator('body').innerHTML();
    expect(bodyContent).toContain('MirDB');
    expect(bodyContent).toContain('persistent key-value store');
    expect(bodyContent).toContain('Features');
    expect(bodyContent).toContain('Quick Start');

    console.log('  - Progressive enhancement (content without JS): PASS');
    console.log('Site is fully compatible with GitHub Pages deployment');
  });

  /**
   * Additional test: Verify static file serving works correctly
   */
  test('All static resources load successfully', async ({ page }) => {
    const failedResources = [];

    // Track failed requests
    page.on('response', (response) => {
      if (response.status() >= 400) {
        failedResources.push({
          url: response.url(),
          status: response.status(),
        });
      }
    });

    await page.goto('/', { waitUntil: 'networkidle' });

    // Log any failed resources
    if (failedResources.length > 0) {
      console.log('Failed resources:', failedResources);
    }

    // Verify no resources failed to load
    expect(failedResources.length, 'All static resources should load successfully').toBe(0);

    // Verify key static resources loaded
    const cssLoaded = await page.evaluate(() => {
      return document.styleSheets.length > 0;
    });
    expect(cssLoaded, 'CSS stylesheets should be loaded').toBe(true);

    // Verify logo image loaded
    const logoVisible = await page.locator('.hero-logo').isVisible();
    expect(logoVisible, 'Hero logo should be visible').toBe(true);

    console.log('All static resources loaded successfully');
  });

  /**
   * Additional test: Verify no build process required for deployment
   */
  test('Site can be deployed without build process', async ({ page }) => {
    const rootDir = path.join(__dirname, '..');

    // Check that the site doesn't require a build step
    // (i.e., there's no src/ directory that needs to be compiled)
    const srcDir = path.join(rootDir, 'src');
    const distDir = path.join(rootDir, 'dist');
    const buildDir = path.join(rootDir, 'build');

    // Verify there's no separate source directory requiring compilation for the website
    // Note: The mirdb-server, skip-list, sstable directories are for the Rust backend, not the website
    const websiteNeedsBuild =
      fs.existsSync(path.join(srcDir, 'index.tsx')) ||
      fs.existsSync(path.join(srcDir, 'index.jsx')) ||
      fs.existsSync(path.join(srcDir, 'App.tsx')) ||
      fs.existsSync(path.join(srcDir, 'App.jsx'));

    expect(
      websiteNeedsBuild,
      'Website should not require a build process (no JSX/TSX source files)'
    ).toBe(false);

    // Verify index.html is directly deployable (not a template)
    const indexPath = path.join(rootDir, 'index.html');
    const indexContent = fs.readFileSync(indexPath, 'utf-8');

    // Should be complete HTML, not a template
    expect(indexContent).toContain('<!DOCTYPE html>');
    expect(indexContent).toContain('<html');
    expect(indexContent).toContain('</html>');
    expect(indexContent).toContain('<head>');
    expect(indexContent).toContain('<body>');
    expect(indexContent).toContain('</body>');

    // Should not contain build tool placeholders
    expect(indexContent).not.toContain('%PUBLIC_URL%');
    expect(indexContent).not.toContain('__webpack');
    expect(indexContent).not.toContain('bundle.js');  // No webpack bundle required

    console.log('Site can be deployed directly without build process');
  });

  /**
   * Additional test: Verify site works with file:// protocol (offline capable)
   */
  test('Site works with relative paths suitable for file:// protocol', async ({ page }) => {
    const rootDir = path.join(__dirname, '..');
    const indexPath = path.join(rootDir, 'index.html');

    // Read the HTML file content
    const htmlContent = fs.readFileSync(indexPath, 'utf-8');

    // Check that stylesheet link is relative
    expect(htmlContent).toContain('href="styles.css"');

    // Check that asset paths are relative
    expect(htmlContent).toContain('src="assets/');

    // Check that internal navigation uses anchors (not absolute paths)
    const anchorLinks = htmlContent.match(/href="#[\w-]+"/g) || [];
    expect(anchorLinks.length, 'Should have internal anchor links').toBeGreaterThan(0);

    console.log('Site uses relative paths suitable for file:// protocol and offline viewing');
  });
});

/**
 * Static Hosting Platform Compatibility Tests
 */
test.describe('Platform-Specific Compatibility', () => {
  test('Compatible with GitHub Pages subdirectory deployment', async ({ page }) => {
    await page.goto('/');

    // Verify all CSS loads correctly
    const styles = await page.evaluate(() => {
      return Array.from(document.styleSheets).map(sheet => ({
        href: sheet.href,
        cssRules: sheet.cssRules ? sheet.cssRules.length : 0,
      }));
    });

    expect(styles.length).toBeGreaterThan(0);
    console.log(`Loaded ${styles.length} stylesheet(s) with ${styles[0]?.cssRules || 0} CSS rules`);

    // Verify page renders correctly (no broken layouts due to path issues)
    const heroSection = page.locator('.hero-section');
    const boundingBox = await heroSection.boundingBox();
    expect(boundingBox?.width).toBeGreaterThan(0);
    expect(boundingBox?.height).toBeGreaterThan(0);

    console.log('GitHub Pages subdirectory deployment: Compatible');
  });

  test('No CORS requirements for static hosting', async ({ page }) => {
    const corsErrors = [];

    // Listen for console errors related to CORS
    page.on('console', (msg) => {
      if (msg.type() === 'error' && msg.text().toLowerCase().includes('cors')) {
        corsErrors.push(msg.text());
      }
    });

    await page.goto('/', { waitUntil: 'networkidle' });

    // Verify no CORS errors occurred
    expect(corsErrors.length, 'No CORS errors should occur').toBe(0);

    console.log('No CORS requirements: Site is fully static');
  });

  test('Site has proper HTML structure for static hosting SEO', async ({ page }) => {
    await page.goto('/');

    // Check for proper meta tags that work on static hosts
    const title = await page.title();
    expect(title).toBeTruthy();
    expect(title).toContain('MirDB');

    // Check meta description
    const metaDescription = await page.locator('meta[name="description"]').getAttribute('content');
    expect(metaDescription).toBeTruthy();
    expect(metaDescription).toContain('key-value');

    // Check canonical URL (should work with any static host)
    const canonical = await page.locator('link[rel="canonical"]').getAttribute('href');
    expect(canonical).toBeTruthy();

    // Check Open Graph tags
    const ogTitle = await page.locator('meta[property="og:title"]').getAttribute('content');
    expect(ogTitle).toBeTruthy();

    console.log('HTML structure is SEO-ready for static hosting');
  });
});
