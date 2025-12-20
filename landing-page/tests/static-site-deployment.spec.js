// @ts-check
const { test, expect } = require('@playwright/test');
const fs = require('fs');
const path = require('path');
const http = require('http');

/**
 * Static Site Deployment Compatibility Tests for MirDB Landing Page
 * Verifies NFR-7: Page must be deployable as a static site (GitHub Pages, Netlify, or similar)
 *
 * This test suite validates:
 * 1. Build produces static HTML, CSS, and JS files
 * 2. Page loads correctly when served as static files
 * 3. Core content is accessible without JavaScript (progressive enhancement)
 */

// Helper to serve static files for testing
function createStaticServer(rootDir, port) {
  return new Promise((resolve, reject) => {
    const mimeTypes = {
      '.html': 'text/html',
      '.css': 'text/css',
      '.js': 'text/javascript',
      '.json': 'application/json',
      '.png': 'image/png',
      '.jpg': 'image/jpeg',
      '.gif': 'image/gif',
      '.svg': 'image/svg+xml',
      '.ico': 'image/x-icon'
    };

    const server = http.createServer((req, res) => {
      let filePath = path.join(rootDir, req.url === '/' ? 'index.html' : req.url);
      const ext = path.extname(filePath).toLowerCase();
      const contentType = mimeTypes[ext] || 'application/octet-stream';

      fs.readFile(filePath, (err, content) => {
        if (err) {
          if (err.code === 'ENOENT') {
            res.writeHead(404);
            res.end('File not found');
          } else {
            res.writeHead(500);
            res.end('Server error');
          }
        } else {
          res.writeHead(200, { 'Content-Type': contentType });
          res.end(content);
        }
      });
    });

    server.listen(port, () => {
      resolve(server);
    });

    server.on('error', reject);
  });
}

test.describe('Static Site Deployment Compatibility (NFR-7)', () => {

  /**
   * Test Case 1: Build produces static HTML, CSS, and JS files
   * Verifies that the landing page consists only of static files that can be
   * deployed to any static hosting service without server-side processing.
   */
  test('Build produces static HTML, CSS, and JS files', async () => {
    const landingPageDir = path.join(__dirname, '..');

    // Check that index.html exists
    const indexHtmlPath = path.join(landingPageDir, 'index.html');
    expect(fs.existsSync(indexHtmlPath)).toBe(true);

    // Check that styles.css exists
    const stylesCssPath = path.join(landingPageDir, 'styles.css');
    expect(fs.existsSync(stylesCssPath)).toBe(true);

    // Read and validate index.html
    const indexHtml = fs.readFileSync(indexHtmlPath, 'utf-8');

    // Verify it's valid HTML (starts with DOCTYPE)
    expect(indexHtml.trim()).toMatch(/^<!DOCTYPE html>/i);

    // Verify it contains required HTML structure
    expect(indexHtml).toContain('<html');
    expect(indexHtml).toContain('<head>');
    expect(indexHtml).toContain('<body>');
    expect(indexHtml).toContain('</html>');

    // Verify CSS is linked properly (relative path, not server-generated)
    expect(indexHtml).toMatch(/<link[^>]+rel=["']stylesheet["'][^>]*>/);
    expect(indexHtml).toContain('styles.css');

    // Verify there are no server-side directives or templating
    expect(indexHtml).not.toMatch(/<%.*%>/); // No EJS/ERB
    expect(indexHtml).not.toMatch(/{{.*}}/); // No Handlebars/Mustache
    expect(indexHtml).not.toMatch(/<\?php/i); // No PHP
    expect(indexHtml).not.toMatch(/{%.*%}/); // No Jinja/Liquid

    // Read and validate styles.css
    const stylesCss = fs.readFileSync(stylesCssPath, 'utf-8');

    // Verify CSS file has content
    expect(stylesCss.length).toBeGreaterThan(100);

    // Verify it contains valid CSS (has CSS selectors and rules)
    expect(stylesCss).toMatch(/[a-zA-Z-]+\s*:\s*[^;]+;/); // CSS property pattern
    expect(stylesCss).toContain('{');
    expect(stylesCss).toContain('}');

    // No server-side processing in CSS
    expect(stylesCss).not.toMatch(/<%.*%>/);
    expect(stylesCss).not.toMatch(/@import\s+url\s*\(\s*['"]?http/i); // No external HTTP imports

    // Log file sizes for informational purposes
    const htmlStats = fs.statSync(indexHtmlPath);
    const cssStats = fs.statSync(stylesCssPath);

    console.log(`Static files verified:`);
    console.log(`  - index.html: ${(htmlStats.size / 1024).toFixed(2)} KB`);
    console.log(`  - styles.css: ${(cssStats.size / 1024).toFixed(2)} KB`);
    console.log(`  Total: ${((htmlStats.size + cssStats.size) / 1024).toFixed(2)} KB`);
  });

  /**
   * Test Case 1 (Additional): Verify no server-side dependencies in file references
   */
  test('No server-side dependencies in file references', async () => {
    const landingPageDir = path.join(__dirname, '..');
    const indexHtmlPath = path.join(landingPageDir, 'index.html');
    const indexHtml = fs.readFileSync(indexHtmlPath, 'utf-8');

    // All src and href attributes should use relative paths or valid URLs
    // Not absolute server paths that require routing
    const srcMatches = indexHtml.match(/src=["'][^"']+["']/g) || [];
    const hrefMatches = indexHtml.match(/href=["'][^"']+["']/g) || [];

    for (const match of srcMatches) {
      const value = match.replace(/src=["']|["']/g, '');
      // Should be relative path, data URI, or full URL
      expect(value).not.toMatch(/^\/[a-zA-Z]/); // Not absolute path like /api/...
      console.log(`Verified src: ${value}`);
    }

    // Verify stylesheets use relative paths
    const cssLinks = hrefMatches.filter(h => h.includes('.css'));
    for (const link of cssLinks) {
      const value = link.replace(/href=["']|["']/g, '');
      expect(value).not.toMatch(/^\/(?!\/)/); // Not server-absolute path
      console.log(`Verified CSS href: ${value}`);
    }
  });

  /**
   * Test Case 2: Page loads correctly when served as static files
   * Serves the page with a simple HTTP server and verifies all content loads.
   */
  test('Page loads correctly when served as static files', async ({ page }) => {
    const landingPageDir = path.join(__dirname, '..');
    const port = 3847; // Use an uncommon port to avoid conflicts

    // Create a simple static file server
    let server;
    try {
      server = await createStaticServer(landingPageDir, port);

      // Navigate to the static server
      const response = await page.goto(`http://localhost:${port}/`);

      // Verify response status
      expect(response.status()).toBe(200);

      // Verify the page title loads
      const title = await page.title();
      expect(title).toContain('MirDB');

      // Verify hero section is rendered
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();

      // Verify main heading
      const h1 = page.locator('h1');
      await expect(h1).toHaveText('MirDB');

      // Verify tagline loads
      const tagline = page.locator('[data-testid="hero-tagline"]');
      await expect(tagline).toBeVisible();
      await expect(tagline).toContainText('Persistent Memcached-Compatible');

      // Verify CSS is loaded by checking styles are applied
      const heroBackground = await page.evaluate(() => {
        const hero = document.querySelector('.hero');
        if (!hero) return null;
        const styles = window.getComputedStyle(hero);
        return styles.background || styles.backgroundColor;
      });

      // Hero should have a gradient background (not default transparent)
      expect(heroBackground).toBeTruthy();
      expect(heroBackground).not.toBe('rgba(0, 0, 0, 0)');

      // Verify features section loads
      const featuresSection = page.locator('[data-testid="features-section"]');
      await expect(featuresSection).toBeVisible();

      // Verify footer loads
      const footer = page.locator('[data-testid="footer"]');
      await expect(footer).toBeVisible();

      console.log('Page loaded successfully from static server');

    } finally {
      if (server) {
        server.close();
      }
    }
  });

  /**
   * Test Case 2 (Additional): Verify static CSS loads correctly
   */
  test('CSS stylesheet loads and applies styles correctly', async ({ page }) => {
    const landingPageDir = path.join(__dirname, '..');
    const port = 3848;

    let server;
    try {
      server = await createStaticServer(landingPageDir, port);

      // Listen for CSS request
      let cssLoaded = false;
      page.on('response', (response) => {
        if (response.url().includes('styles.css')) {
          cssLoaded = true;
          expect(response.status()).toBe(200);
        }
      });

      await page.goto(`http://localhost:${port}/`);

      // Verify CSS was loaded
      expect(cssLoaded).toBe(true);

      // Verify CSS rules are applied
      const buttonStyles = await page.evaluate(() => {
        const btn = document.querySelector('.btn-primary');
        if (!btn) return null;
        const styles = window.getComputedStyle(btn);
        return {
          display: styles.display,
          padding: styles.padding,
          borderRadius: styles.borderRadius,
          background: styles.background || styles.backgroundImage
        };
      });

      expect(buttonStyles).not.toBeNull();
      // Button should have display set (inline-block on desktop, block on mobile)
      expect(['inline-block', 'block']).toContain(buttonStyles.display);
      // Button should have gradient background (CSS applied)
      expect(buttonStyles.background).toContain('gradient');

      console.log('CSS loaded and applied correctly');

    } finally {
      if (server) {
        server.close();
      }
    }
  });

  /**
   * Test Case 3: Core content is accessible without JavaScript
   * Disables JavaScript and verifies product name, tagline, and key content
   * are still visible (progressive enhancement).
   */
  test('Product name, tagline, and key content visible without JavaScript', async ({ browser }) => {
    // Create a context with JavaScript disabled
    const context = await browser.newContext({
      javaScriptEnabled: false
    });

    const page = await context.newPage();

    try {
      // Navigate to the page with JavaScript disabled
      const indexPath = path.join(__dirname, '..', 'index.html');
      await page.goto(`file://${indexPath}`);

      // Verify product name is visible
      const productName = page.locator('h1');
      await expect(productName).toBeVisible();
      await expect(productName).toHaveText('MirDB');

      // Verify tagline is visible
      const tagline = page.locator('[data-testid="hero-tagline"]');
      await expect(tagline).toBeVisible();
      const taglineText = await tagline.textContent();
      expect(taglineText).toContain('Persistent');
      expect(taglineText).toContain('Memcached');
      expect(taglineText).toContain('Rust');

      // Verify description is visible
      const description = page.locator('.description');
      await expect(description).toBeVisible();
      await expect(description).toContainText('high-performance');

      // Verify CTA buttons are visible
      const primaryCta = page.locator('[data-testid="cta-primary"]');
      await expect(primaryCta).toBeVisible();
      await expect(primaryCta).toHaveText('Get Started');

      const secondaryCta = page.locator('[data-testid="cta-secondary"]');
      await expect(secondaryCta).toBeVisible();
      await expect(secondaryCta).toHaveText('View Documentation');

      console.log('Hero section content verified without JavaScript');

    } finally {
      await context.close();
    }
  });

  /**
   * Test Case 3 (Additional): Verify all key sections are accessible without JavaScript
   */
  test('All key sections accessible without JavaScript', async ({ browser }) => {
    const context = await browser.newContext({
      javaScriptEnabled: false
    });

    const page = await context.newPage();

    try {
      const indexPath = path.join(__dirname, '..', 'index.html');
      await page.goto(`file://${indexPath}`);

      // Verify Features section
      const featuresSection = page.locator('[data-testid="features-section"]');
      await expect(featuresSection).toBeVisible();

      // Verify all three feature cards are visible
      const memcachedFeature = page.locator('[data-testid="feature-card-memcached"]');
      await expect(memcachedFeature).toBeVisible();
      await expect(memcachedFeature).toContainText('Memcached Protocol Compatible');

      const persistenceFeature = page.locator('[data-testid="feature-card-persistence"]');
      await expect(persistenceFeature).toBeVisible();
      await expect(persistenceFeature).toContainText('Persistent Storage');

      const lsmFeature = page.locator('[data-testid="feature-card-lsm"]');
      await expect(lsmFeature).toBeVisible();
      await expect(lsmFeature).toContainText('LSM Tree Architecture');

      console.log('Feature cards verified without JavaScript');

      // Verify Code Examples section
      const codeExamplesSection = page.locator('[data-testid="code-examples-section"]');
      await expect(codeExamplesSection).toBeVisible();

      // Verify supported commands are visible
      const supportedCommands = page.locator('[data-testid="supported-commands"]');
      await expect(supportedCommands).toBeVisible();

      console.log('Code examples section verified without JavaScript');

      // Verify Getting Started section
      const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
      await expect(gettingStartedSection).toBeVisible();

      // Verify installation steps are visible
      const step1 = page.locator('[data-testid="step-1"]');
      await expect(step1).toBeVisible();
      await expect(step1).toContainText('Install via Cargo');

      const step2 = page.locator('[data-testid="step-2"]');
      await expect(step2).toBeVisible();
      await expect(step2).toContainText('Configuration File');

      console.log('Getting started section verified without JavaScript');

      // Verify Footer
      const footer = page.locator('[data-testid="footer"]');
      await expect(footer).toBeVisible();

      // Verify GitHub link is accessible
      const githubLink = page.locator('[data-testid="github-link"]');
      await expect(githubLink).toBeVisible();
      await expect(githubLink).toHaveAttribute('href', 'https://github.com/mirdb/mirdb');

      // Verify documentation link is accessible
      const docsLink = page.locator('[data-testid="docs-link"]');
      await expect(docsLink).toBeVisible();

      console.log('Footer section verified without JavaScript');

    } finally {
      await context.close();
    }
  });

  /**
   * Test Case 3 (Additional): Verify navigation works without JavaScript
   */
  test('Anchor links work without JavaScript', async ({ browser }) => {
    const context = await browser.newContext({
      javaScriptEnabled: false
    });

    const page = await context.newPage();

    try {
      const indexPath = path.join(__dirname, '..', 'index.html');
      await page.goto(`file://${indexPath}`);

      // Verify the "Get Started" button links to the getting-started section
      const getStartedBtn = page.locator('[data-testid="cta-primary"]');
      const href = await getStartedBtn.getAttribute('href');
      expect(href).toBe('#getting-started');

      // Click the button and verify navigation works
      await getStartedBtn.click();

      // The getting started section should be in viewport after clicking
      const gettingStartedSection = page.locator('#getting-started');
      await expect(gettingStartedSection).toBeInViewport();

      console.log('Anchor navigation works without JavaScript');

    } finally {
      await context.close();
    }
  });

  /**
   * Test for static deployment: No build step required
   * The landing page should work directly without npm run build or similar.
   */
  test('No build step required - files are deployment ready', async () => {
    const landingPageDir = path.join(__dirname, '..');

    // Verify the files needed for deployment exist in the root
    // No dist/ or build/ directory should be required
    const requiredFiles = ['index.html', 'styles.css'];

    for (const file of requiredFiles) {
      const filePath = path.join(landingPageDir, file);
      expect(fs.existsSync(filePath)).toBe(true);
      console.log(`Deployment file exists: ${file}`);
    }

    // Verify there's no requirement for a build step
    const packageJsonPath = path.join(landingPageDir, 'package.json');
    const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, 'utf-8'));

    // The package.json should not have a required build script that generates
    // the main HTML/CSS (test script is fine, build for optimization is optional)
    // The site should work without running build
    expect(packageJson.scripts).toBeDefined();

    // If there is a build script, verify it's not required for basic functionality
    if (packageJson.scripts.build) {
      // index.html should exist without running build
      expect(fs.existsSync(path.join(landingPageDir, 'index.html'))).toBe(true);
      console.log('Build script exists but index.html is already present (optional build)');
    } else {
      console.log('No build script required - site is pre-built/static');
    }
  });

  /**
   * Test GitHub Pages compatibility
   * Verifies the site can be deployed directly to GitHub Pages.
   */
  test('Compatible with GitHub Pages deployment', async () => {
    const landingPageDir = path.join(__dirname, '..');
    const indexHtmlPath = path.join(landingPageDir, 'index.html');
    const indexHtml = fs.readFileSync(indexHtmlPath, 'utf-8');

    // GitHub Pages serves from root, so all paths should be relative
    // No absolute paths starting with /

    // Check stylesheet link
    const cssLinkMatch = indexHtml.match(/href=["']([^"']+\.css)["']/);
    expect(cssLinkMatch).not.toBeNull();
    const cssPath = cssLinkMatch[1];

    // Should be relative (not starting with /)
    expect(cssPath.startsWith('/')).toBe(false);
    expect(cssPath).toBe('styles.css');

    // No API calls or server-side routes
    expect(indexHtml).not.toContain('fetch(');
    expect(indexHtml).not.toContain('XMLHttpRequest');

    console.log('Site is compatible with GitHub Pages');
  });

  /**
   * Test Netlify compatibility
   * Verifies the site can be deployed to Netlify.
   */
  test('Compatible with Netlify deployment', async () => {
    const landingPageDir = path.join(__dirname, '..');

    // For Netlify, the site just needs to be static HTML/CSS/JS
    // No _redirects or netlify.toml required for basic static site

    // Verify all assets are self-contained (no external dependencies required)
    const indexHtmlPath = path.join(landingPageDir, 'index.html');
    const indexHtml = fs.readFileSync(indexHtmlPath, 'utf-8');

    // All required CSS is local
    const cssLinks = indexHtml.match(/href=["'][^"']+\.css["']/g) || [];
    for (const link of cssLinks) {
      const href = link.replace(/href=["']|["']/g, '');
      // Verify local CSS file exists
      const cssPath = path.join(landingPageDir, href);
      expect(fs.existsSync(cssPath)).toBe(true);
    }

    // Site doesn't require server-side functions
    expect(indexHtml).not.toContain('/.netlify/functions');

    console.log('Site is compatible with Netlify deployment');
  });
});
