// @ts-check
const { test, expect } = require('@playwright/test');
const fs = require('fs');
const path = require('path');

/**
 * Test Scenario: Static Site Structure (NFR-5)
 *
 * Verify that the site is implemented as static files without server-side rendering.
 * This ensures the site can be deployed to any static file hosting service.
 */

const HOMEPAGE_DIR = path.resolve(__dirname, '..');
const STATIC_FILES = ['index.html', 'styles.css', 'main.js'];

// Test Case 1: Build produces static HTML, CSS, and JS files in output directory
test.describe('Static Site Structure - Build Output', () => {
  test('should have static HTML file', () => {
    const htmlPath = path.join(HOMEPAGE_DIR, 'index.html');
    expect(fs.existsSync(htmlPath)).toBe(true);

    const htmlContent = fs.readFileSync(htmlPath, 'utf-8');
    expect(htmlContent).toContain('<!DOCTYPE html>');
    expect(htmlContent).toContain('<html');
    expect(htmlContent).toContain('</html>');
  });

  test('should have static CSS file', () => {
    const cssPath = path.join(HOMEPAGE_DIR, 'styles.css');
    expect(fs.existsSync(cssPath)).toBe(true);

    const cssContent = fs.readFileSync(cssPath, 'utf-8');
    // Check that it contains valid CSS (selectors and rules)
    expect(cssContent).toMatch(/[a-z-]+\s*\{[\s\S]*?\}/i);
  });

  test('should have static JavaScript file', () => {
    const jsPath = path.join(HOMEPAGE_DIR, 'main.js');
    expect(fs.existsSync(jsPath)).toBe(true);

    const jsContent = fs.readFileSync(jsPath, 'utf-8');
    // Check that it contains JavaScript code
    expect(jsContent.length).toBeGreaterThan(0);
  });

  test('should have all required static files present', () => {
    for (const file of STATIC_FILES) {
      const filePath = path.join(HOMEPAGE_DIR, file);
      expect(fs.existsSync(filePath), `File ${file} should exist`).toBe(true);
    }
  });

  test('HTML file should reference CSS and JS files correctly', () => {
    const htmlPath = path.join(HOMEPAGE_DIR, 'index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    // Check CSS link
    expect(htmlContent).toContain('href="styles.css"');
    // Check JS script
    expect(htmlContent).toContain('src="main.js"');
  });
});

// Test Case 2: Site is functional when served by static file server
test.describe('Static Site Structure - Static Server Compatibility', () => {
  test('should load and render the homepage correctly', async ({ page }) => {
    await page.goto('/');

    // Check that the page loads without server-side processing
    const title = await page.title();
    expect(title).toBeTruthy();
    expect(title).toContain('MirDB');
  });

  test('should serve static assets correctly', async ({ page }) => {
    // Check CSS is loaded
    const cssResponse = await page.request.get('/styles.css');
    expect(cssResponse.status()).toBe(200);
    expect(cssResponse.headers()['content-type']).toContain('text/css');

    // Check JS is loaded
    const jsResponse = await page.request.get('/main.js');
    expect(jsResponse.status()).toBe(200);
    expect(jsResponse.headers()['content-type']).toContain('javascript');
  });

  test('should render all main sections without server processing', async ({ page }) => {
    await page.goto('/');

    // All sections should be present in the static HTML
    await expect(page.locator('[data-testid="hero-section"]')).toBeVisible();
    await expect(page.locator('[data-testid="features-section"]')).toBeVisible();
    await expect(page.locator('[data-testid="architecture-section"]')).toBeVisible();
    await expect(page.locator('[data-testid="commands-section"]')).toBeVisible();
    await expect(page.locator('[data-testid="getting-started-section"]')).toBeVisible();
  });

  test('should have working navigation links (client-side only)', async ({ page }) => {
    await page.goto('/');

    // Click on "Learn More" button and verify smooth scroll (client-side)
    await page.click('[data-testid="secondary-cta"]');

    // Wait for scroll animation
    await page.waitForTimeout(500);

    // Features section should be in view
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();
  });

  test('should not require any server-side processing for content', async ({ page }) => {
    await page.goto('/');

    // Check that content is pre-rendered in HTML (not dynamically loaded)
    const productName = await page.locator('[data-testid="product-name"]').textContent();
    expect(productName).toBe('MirDB');

    const tagline = await page.locator('[data-testid="tagline"]').textContent();
    expect(tagline).toContain('persistent key-value store');
  });
});

// Test Case 3: No server-side code in output
test.describe('Static Site Structure - No Server-Side Code', () => {
  test('should not contain PHP code', () => {
    const htmlPath = path.join(HOMEPAGE_DIR, 'index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    // Check for PHP opening tags
    expect(htmlContent).not.toMatch(/<\?php/gi);
    expect(htmlContent).not.toMatch(/<\?=/gi);
    expect(htmlContent).not.toMatch(/<\?(?!xml)/gi);
  });

  test('should not contain server-side template syntax', () => {
    const htmlPath = path.join(HOMEPAGE_DIR, 'index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    // Check for common server-side template patterns
    // EJS: <%= %>, <%- %>, <% %>
    expect(htmlContent).not.toMatch(/<%[=-]?\s/g);
    // Jinja/Django: {{ }}, {% %}
    // Note: Mermaid diagrams may use {{ }} so we check for server patterns specifically
    expect(htmlContent).not.toMatch(/{%\s*(?:if|for|block|extends|include|import)/gi);
    // ASP.NET: <% %>
    expect(htmlContent).not.toMatch(/<%@/g);
    // Ruby ERB
    expect(htmlContent).not.toMatch(/<%=?\s*@/g);
  });

  test('should not contain Node.js/Express server code in JS file', () => {
    const jsPath = path.join(HOMEPAGE_DIR, 'main.js');
    const jsContent = fs.readFileSync(jsPath, 'utf-8');

    // Check for server-side Node.js patterns
    expect(jsContent).not.toMatch(/require\s*\(\s*['"]express['"]\s*\)/);
    expect(jsContent).not.toMatch(/require\s*\(\s*['"]http['"]\s*\)/);
    expect(jsContent).not.toMatch(/require\s*\(\s*['"]https['"]\s*\)/);
    expect(jsContent).not.toMatch(/require\s*\(\s*['"]fs['"]\s*\)/);
    expect(jsContent).not.toMatch(/require\s*\(\s*['"]path['"]\s*\)/);
    expect(jsContent).not.toMatch(/createServer\s*\(/);
    expect(jsContent).not.toMatch(/app\.listen\s*\(/);
    expect(jsContent).not.toMatch(/process\.env/);
  });

  test('should not have PHP files in homepage directory', () => {
    const files = fs.readdirSync(HOMEPAGE_DIR);
    const phpFiles = files.filter(f => f.endsWith('.php'));
    expect(phpFiles.length).toBe(0);
  });

  test('should not have server configuration files', () => {
    const serverConfigFiles = [
      '.htaccess',
      'web.config',
      'server.js',
      'app.js',
      'index.php',
      'Procfile'
    ];

    for (const configFile of serverConfigFiles) {
      // server.js and app.js could exist but should not be part of the static site
      // We allow package.json for development dependencies only
      if (configFile === 'server.js' || configFile === 'app.js') {
        const filePath = path.join(HOMEPAGE_DIR, configFile);
        if (fs.existsSync(filePath)) {
          const content = fs.readFileSync(filePath, 'utf-8');
          // If these files exist, they should not contain actual server code
          expect(content).not.toMatch(/createServer|app\.listen|http\.Server/);
        }
      } else {
        const filePath = path.join(HOMEPAGE_DIR, configFile);
        expect(fs.existsSync(filePath), `Server config file ${configFile} should not exist`).toBe(false);
      }
    }
  });

  test('HTML should be complete and self-contained', () => {
    const htmlPath = path.join(HOMEPAGE_DIR, 'index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    // Check for complete HTML structure
    expect(htmlContent).toMatch(/<!DOCTYPE html>/i);
    expect(htmlContent).toMatch(/<html[^>]*>/i);
    expect(htmlContent).toMatch(/<head>/i);
    expect(htmlContent).toMatch(/<\/head>/i);
    expect(htmlContent).toMatch(/<body>/i);
    expect(htmlContent).toMatch(/<\/body>/i);
    expect(htmlContent).toMatch(/<\/html>/i);

    // Check that content is pre-rendered (not loaded via AJAX)
    expect(htmlContent).toContain('MirDB');
    expect(htmlContent).toContain('Key Features');
    expect(htmlContent).toContain('Getting Started');
  });

  test('should only use client-side JavaScript features', () => {
    const jsPath = path.join(HOMEPAGE_DIR, 'main.js');
    const jsContent = fs.readFileSync(jsPath, 'utf-8');

    // Should use browser APIs
    expect(jsContent).toContain('document');

    // Should not use Node.js specific globals
    expect(jsContent).not.toMatch(/\bmodule\.exports\b/);
    expect(jsContent).not.toMatch(/\bexports\./);
    expect(jsContent).not.toMatch(/\b__dirname\b/);
    expect(jsContent).not.toMatch(/\b__filename\b/);
  });
});

// Additional tests for static site deployment readiness
test.describe('Static Site Structure - Deployment Readiness', () => {
  test('should have proper meta tags for SEO', () => {
    const htmlPath = path.join(HOMEPAGE_DIR, 'index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    expect(htmlContent).toContain('charset="UTF-8"');
    expect(htmlContent).toContain('viewport');
    expect(htmlContent).toContain('name="description"');
  });

  test('should have relative paths for assets (static hosting compatible)', () => {
    const htmlPath = path.join(HOMEPAGE_DIR, 'index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    // CSS and JS should use relative paths
    expect(htmlContent).toContain('href="styles.css"');
    expect(htmlContent).toContain('src="main.js"');

    // Should not use absolute server paths
    expect(htmlContent).not.toMatch(/href=["']\/[^"']*styles\.css["']/);
    expect(htmlContent).not.toMatch(/src=["']\/[^"']*main\.js["']/);
  });

  test('CSS file should be valid and contain styling rules', () => {
    const cssPath = path.join(HOMEPAGE_DIR, 'styles.css');
    const cssContent = fs.readFileSync(cssPath, 'utf-8');

    // Should contain proper CSS structure
    expect(cssContent).toContain('.hero');
    expect(cssContent).toContain('.features');
    expect(cssContent).toContain('@media');
  });
});
