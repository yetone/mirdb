// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');
const fs = require('fs');

const pageUrl = 'file://' + path.join(__dirname, '..', 'index.html');
const projectRoot = path.join(__dirname, '..');

test.describe('Static Deployment Compatibility (NFR-4)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(pageUrl);
  });

  // Test Case 1: Check for server-side rendering requirements
  test('TC1: No server-side code or rendering required', async ({ page }) => {
    // Verify the page loads successfully via file:// protocol
    // This confirms no server-side rendering is needed
    const title = await page.title();
    expect(title).toBe('MirDB - Persistent Key-Value Store with Memcached Protocol');

    // Check that the main content is rendered
    const hero = page.locator('.hero');
    await expect(hero).toBeVisible();

    const heroH1 = page.locator('.hero h1');
    await expect(heroH1).toHaveText('MirDB');

    // Verify features section is rendered statically
    const features = page.locator('#features');
    await expect(features).toBeVisible();

    // Verify getting started section is rendered statically
    const gettingStarted = page.locator('#getting-started');
    await expect(gettingStarted).toBeVisible();

    // Verify footer is rendered statically
    const footer = page.locator('footer.footer');
    await expect(footer).toBeVisible();

    // Check that index.html is pure HTML without server-side templating syntax
    const htmlPath = path.join(projectRoot, 'index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf8');

    // No PHP tags
    expect(htmlContent).not.toMatch(/<\?php/i);
    // No JSP tags
    expect(htmlContent).not.toMatch(/<%/);
    // No ERB tags
    expect(htmlContent).not.toMatch(/<%=/);
    // No Jinja/Django templates
    expect(htmlContent).not.toMatch(/\{\{.*\}\}/);
    // No Handlebars/Mustache
    expect(htmlContent).not.toMatch(/\{\{#/);
    // No ASP.NET
    expect(htmlContent).not.toMatch(/@\{/);
    // No server-side includes
    expect(htmlContent).not.toMatch(/<!--#include/i);
  });

  // Test Case 2: Check for API calls or fetch requests
  test('TC2: No dynamic data fetching required for core functionality', async ({ page }) => {
    // Create arrays to track network requests
    const networkRequests = [];

    // Listen for network requests
    page.on('request', request => {
      const url = request.url();
      // Filter out file:// protocol and external CDN resources (CSS/JS for syntax highlighting)
      if (!url.startsWith('file://') &&
          !url.includes('cdnjs.cloudflare.com/ajax/libs/prism')) {
        networkRequests.push({
          url: url,
          method: request.method(),
          resourceType: request.resourceType()
        });
      }
    });

    // Reload the page to capture all requests
    await page.goto(pageUrl);

    // Wait for page to fully load
    await page.waitForLoadState('networkidle');

    // Check for API/fetch calls (excluding CDN for syntax highlighting which is acceptable)
    const apiCalls = networkRequests.filter(req =>
      req.resourceType === 'fetch' ||
      req.resourceType === 'xhr' ||
      req.url.includes('/api/')
    );

    // Verify no API calls are made for core functionality
    expect(apiCalls).toHaveLength(0);

    // Verify core content is present without needing API calls
    const heroText = await page.locator('.hero .tagline').textContent();
    expect(heroText).toBe('Persistent Key-Value Store with Memcached Protocol');

    const featureCards = page.locator('.feature-card');
    await expect(featureCards).toHaveCount(3);

    // Verify code examples are present statically
    const codeBlocks = page.locator('pre code');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThan(0);
  });

  // Test Case 3: Verify all assets are relative paths
  test('TC3: Assets use relative paths for portability', async ({ page }) => {
    // Read the HTML file to check paths
    const htmlPath = path.join(projectRoot, 'index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf8');

    // Check CSS link tags - should use relative paths (not absolute starting with /)
    const cssLinks = htmlContent.match(/<link[^>]+rel=["']stylesheet["'][^>]*>/gi) || [];

    for (const link of cssLinks) {
      const hrefMatch = link.match(/href=["']([^"']+)["']/i);
      if (hrefMatch) {
        const href = hrefMatch[1];
        // Allow CDN URLs for external libraries (like Prism)
        if (!href.startsWith('http://') && !href.startsWith('https://')) {
          // Local paths should be relative (not starting with /)
          expect(href).not.toMatch(/^\/[^/]/);
        }
      }
    }

    // Check script tags - local scripts should use relative paths
    const scriptTags = htmlContent.match(/<script[^>]+src=["'][^"']+["'][^>]*>/gi) || [];

    for (const script of scriptTags) {
      const srcMatch = script.match(/src=["']([^"']+)["']/i);
      if (srcMatch) {
        const src = srcMatch[1];
        // Allow CDN URLs for external libraries
        if (!src.startsWith('http://') && !src.startsWith('https://')) {
          // Local scripts should use relative paths
          expect(src).not.toMatch(/^\/[^/]/);
        }
      }
    }

    // Check image tags
    const imgTags = htmlContent.match(/<img[^>]+src=["'][^"']+["'][^>]*>/gi) || [];

    for (const img of imgTags) {
      const srcMatch = img.match(/src=["']([^"']+)["']/i);
      if (srcMatch) {
        const src = srcMatch[1];
        if (!src.startsWith('http://') && !src.startsWith('https://') && !src.startsWith('data:')) {
          // Local images should use relative paths
          expect(src).not.toMatch(/^\/[^/]/);
        }
      }
    }

    // Check internal links (href for anchor tags to local resources)
    const anchorTags = htmlContent.match(/<a[^>]+href=["'][^"']+["'][^>]*>/gi) || [];

    for (const anchor of anchorTags) {
      const hrefMatch = anchor.match(/href=["']([^"']+)["']/i);
      if (hrefMatch) {
        const href = hrefMatch[1];
        // Skip external URLs, hash links, and mailto/tel links
        if (!href.startsWith('http://') &&
            !href.startsWith('https://') &&
            !href.startsWith('#') &&
            !href.startsWith('mailto:') &&
            !href.startsWith('tel:')) {
          // Local links should use relative paths
          expect(href).not.toMatch(/^\/[^/]/);
        }
      }
    }

    // Verify CSS file uses relative paths for any url() references
    const cssPath = path.join(projectRoot, 'styles.css');
    const cssContent = fs.readFileSync(cssPath, 'utf8');

    // Extract all url() references
    const urlMatches = cssContent.match(/url\(["']?([^"')]+)["']?\)/gi) || [];

    for (const urlMatch of urlMatches) {
      const urlPath = urlMatch.replace(/url\(["']?([^"')]+)["']?\)/i, '$1');
      // Skip data URIs and external URLs
      if (!urlPath.startsWith('data:') &&
          !urlPath.startsWith('http://') &&
          !urlPath.startsWith('https://')) {
        // Local URLs in CSS should be relative
        expect(urlPath).not.toMatch(/^\/[^/]/);
      }
    }
  });

  // Test Case 4: Check file structure for GitHub Pages
  test('TC4: File structure is compatible with GitHub Pages', async ({ page }) => {
    // Check that index.html exists at root level
    const indexPath = path.join(projectRoot, 'index.html');
    expect(fs.existsSync(indexPath)).toBe(true);

    // Check that index.html is a valid HTML file
    const htmlContent = fs.readFileSync(indexPath, 'utf8');
    expect(htmlContent).toMatch(/<!DOCTYPE html>/i);
    expect(htmlContent).toMatch(/<html/i);
    expect(htmlContent).toMatch(/<head/i);
    expect(htmlContent).toMatch(/<body/i);
    expect(htmlContent).toMatch(/<\/html>/i);

    // Check that CSS file exists and is referenced correctly
    const stylesPath = path.join(projectRoot, 'styles.css');
    expect(fs.existsSync(stylesPath)).toBe(true);
    expect(htmlContent).toContain('href="styles.css"');

    // Verify no server configuration files that would indicate server dependency
    // .htaccess would indicate Apache server dependency
    const htaccessPath = path.join(projectRoot, '.htaccess');
    // It's OK if it doesn't exist; if it does, it should only have simple redirects
    if (fs.existsSync(htaccessPath)) {
      const htaccessContent = fs.readFileSync(htaccessPath, 'utf8');
      // Should not contain PHP or server-side processing directives
      expect(htaccessContent).not.toMatch(/AddHandler.*php/i);
      expect(htaccessContent).not.toMatch(/SetHandler.*php/i);
    }

    // Check for web.config (IIS configuration) - should not require server-side features
    const webConfigPath = path.join(projectRoot, 'web.config');
    if (fs.existsSync(webConfigPath)) {
      const webConfigContent = fs.readFileSync(webConfigPath, 'utf8');
      // Should not contain ASP.NET handlers
      expect(webConfigContent).not.toMatch(/aspx/i);
    }

    // Verify assets directory structure is appropriate
    const assetsPath = path.join(projectRoot, 'assets');
    if (fs.existsSync(assetsPath)) {
      const assetStats = fs.statSync(assetsPath);
      expect(assetStats.isDirectory()).toBe(true);
    }

    // Verify js directory exists if referenced
    const jsPath = path.join(projectRoot, 'js');
    if (fs.existsSync(jsPath)) {
      const jsStats = fs.statSync(jsPath);
      expect(jsStats.isDirectory()).toBe(true);
    }

    // Verify css directory exists if present
    const cssPath = path.join(projectRoot, 'css');
    if (fs.existsSync(cssPath)) {
      const cssStats = fs.statSync(cssPath);
      expect(cssStats.isDirectory()).toBe(true);
    }

    // Verify the page renders correctly via file:// protocol
    // This is the ultimate test for GitHub Pages compatibility
    await expect(page.locator('body')).toBeVisible();
    await expect(page.locator('.hero')).toBeVisible();
    await expect(page.locator('#features')).toBeVisible();
    await expect(page.locator('#getting-started')).toBeVisible();

    // Check that no CNAME file requires custom domain (optional, not required)
    // GitHub Pages works fine without CNAME for *.github.io domains

    // Verify no Jekyll configuration that would require Jekyll build
    const jekyllConfigPath = path.join(projectRoot, '_config.yml');
    // Having no Jekyll config is fine - pure static HTML doesn't need it
    // If it exists, verify it doesn't require complex builds
    if (fs.existsSync(jekyllConfigPath)) {
      const jekyllConfig = fs.readFileSync(jekyllConfigPath, 'utf8');
      // Check it's not requiring complex plugins that need server processing
      expect(jekyllConfig).not.toMatch(/plugins:[\s\S]*jekyll-admin/i);
    }

    // Check for .nojekyll file which tells GitHub Pages to serve as static
    // This is a good practice for static sites
    const nojekyllPath = path.join(projectRoot, '.nojekyll');
    // It's OK if it doesn't exist - the site will still work
    // If present, it confirms intention to serve as pure static
  });

  // Additional: Verify page works with file:// protocol completely
  test('Page fully functional with file:// protocol (no server needed)', async ({ page }) => {
    // Verify URL is using file:// protocol
    const url = page.url();
    expect(url).toMatch(/^file:\/\//);

    // Check all main sections are visible
    await expect(page.locator('header.header')).toBeVisible();
    await expect(page.locator('.hero')).toBeVisible();
    await expect(page.locator('#features')).toBeVisible();
    await expect(page.locator('#getting-started')).toBeVisible();
    await expect(page.locator('#commands')).toBeVisible();
    await expect(page.locator('footer.footer')).toBeVisible();

    // Check navigation links work (internal anchors)
    const featuresLink = page.locator('.nav-links a[href="#features"]');
    await expect(featuresLink).toBeVisible();
    await featuresLink.click();
    await expect(page).toHaveURL(/#features$/);
    await expect(page.locator('#features')).toBeInViewport();

    // Check hero CTA buttons are functional
    const getStartedBtn = page.locator('.cta-buttons a[href="#getting-started"]');
    await expect(getStartedBtn).toBeVisible();
    await getStartedBtn.click();
    await expect(page).toHaveURL(/#getting-started$/);
    await expect(page.locator('#getting-started')).toBeInViewport();

    // Verify content is all present statically
    const heroH1 = page.locator('.hero h1');
    await expect(heroH1).toHaveText('MirDB');

    const tagline = page.locator('.hero .tagline');
    await expect(tagline).toHaveText('Persistent Key-Value Store with Memcached Protocol');

    const featureCards = page.locator('.feature-card');
    await expect(featureCards).toHaveCount(3);

    // Verify code examples are rendered
    const codeBlocks = page.locator('pre code');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThan(0);
  });
});
