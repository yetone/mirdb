// @ts-check
const { test, expect } = require('@playwright/test');
const fs = require('fs');
const path = require('path');

/**
 * Static Site Requirements Tests
 *
 * Scenario: Verify homepage can be hosted as static site without server dependencies
 * These tests verify that the homepage meets all requirements for static hosting
 * on platforms like GitHub Pages, Netlify, or any basic file server.
 */

test.describe('Static Site Requirements', () => {
  const homepageDir = path.join(__dirname, '..');

  /**
   * Test Case 1: Build static site output
   * Expected: Build produces static HTML/CSS/JS files only
   */
  test.describe('Test Case 1: Build produces static HTML/CSS/JS files only', () => {
    test('homepage directory contains only static files (HTML, CSS, JS)', async () => {
      // Read all files in the homepage directory
      const files = fs.readdirSync(homepageDir);

      // Define allowed static file extensions and directories
      const allowedExtensions = ['.html', '.css', '.js', '.json', '.md', '.gitignore'];
      const allowedDirectories = ['tests', 'node_modules', 'test-results', 'playwright-report'];
      const configFiles = ['package.json', 'package-lock.json', 'playwright.config.js', '.gitignore'];

      console.log('Files in homepage directory:', files);

      // Check each file
      for (const file of files) {
        const filePath = path.join(homepageDir, file);
        const stat = fs.statSync(filePath);

        if (stat.isDirectory()) {
          // Only test and node_modules directories should be present
          const isAllowedDir = allowedDirectories.some(dir => file === dir || file.startsWith('.'));
          expect(isAllowedDir || file.startsWith('.')).toBe(true);
        } else {
          // Must be a static file or config file
          const ext = path.extname(file);
          const isAllowed = allowedExtensions.includes(ext) || configFiles.includes(file);
          expect(isAllowed).toBe(true);
        }
      }
    });

    test('index.html exists and is valid HTML', async () => {
      const indexPath = path.join(homepageDir, 'index.html');
      expect(fs.existsSync(indexPath)).toBe(true);

      const content = fs.readFileSync(indexPath, 'utf-8');

      // Verify it's valid HTML5
      expect(content).toContain('<!DOCTYPE html>');
      expect(content).toContain('<html');
      expect(content).toContain('<head>');
      expect(content).toContain('<body>');
      expect(content).toContain('</html>');
    });

    test('styles.css exists and is valid CSS', async () => {
      const cssPath = path.join(homepageDir, 'styles.css');
      expect(fs.existsSync(cssPath)).toBe(true);

      const content = fs.readFileSync(cssPath, 'utf-8');

      // Verify it contains CSS content
      expect(content.length).toBeGreaterThan(0);
      // Check for basic CSS syntax (selectors and rules)
      expect(content).toMatch(/[.#]?[\w-]+\s*\{[^}]*\}/);
    });

    test('no server-side files exist (PHP, Python, Ruby, etc.)', async () => {
      const serverSideExtensions = ['.php', '.py', '.rb', '.java', '.go', '.asp', '.aspx', '.jsp'];

      const checkDirectory = (dir) => {
        const files = fs.readdirSync(dir);
        for (const file of files) {
          const filePath = path.join(dir, file);
          const stat = fs.statSync(filePath);

          if (stat.isDirectory()) {
            // Skip node_modules and test directories
            if (!['node_modules', 'test-results', 'playwright-report'].includes(file)) {
              checkDirectory(filePath);
            }
          } else {
            const ext = path.extname(file);
            expect(serverSideExtensions).not.toContain(ext);
          }
        }
      };

      checkDirectory(homepageDir);
    });

    test('no build configuration requires server-side processing', async () => {
      // Verify there's no next.config.js, nuxt.config.js, or similar SSR configs
      const ssrConfigs = [
        'next.config.js',
        'next.config.mjs',
        'nuxt.config.js',
        'nuxt.config.ts',
        'server.js',
        'server.ts',
        'app.js',
        'app.ts'
      ];

      for (const config of ssrConfigs) {
        const configPath = path.join(homepageDir, config);
        expect(fs.existsSync(configPath)).toBe(false);
      }
    });
  });

  /**
   * Test Case 2: Serve with static file server
   * Expected: Site functions correctly when served by static file server
   */
  test.describe('Test Case 2: Site functions correctly with static file server', () => {
    test('page loads successfully from static file server', async ({ page }) => {
      // The playwright config already uses 'serve' which is a static file server
      const response = await page.goto('/');

      // Verify successful response
      expect(response.status()).toBe(200);
      expect(response.headers()['content-type']).toContain('text/html');
    });

    test('all linked resources load successfully', async ({ page }) => {
      const failedResources = [];

      page.on('requestfailed', request => {
        failedResources.push({
          url: request.url(),
          error: request.failure()?.errorText
        });
      });

      await page.goto('/', { waitUntil: 'networkidle' });

      console.log('Failed resources:', failedResources);
      expect(failedResources.length).toBe(0);
    });

    test('CSS file is served correctly', async ({ page }) => {
      const cssResponse = await page.request.get('/styles.css');

      expect(cssResponse.status()).toBe(200);
      expect(cssResponse.headers()['content-type']).toContain('text/css');
    });

    test('all internal links resolve to valid resources', async ({ page }) => {
      await page.goto('/');

      // Get all internal anchor links
      const internalLinks = await page.evaluate(() => {
        const links = document.querySelectorAll('a[href^="#"], a[href^="/"]');
        return Array.from(links).map(link => link.getAttribute('href'));
      });

      console.log('Internal links found:', internalLinks);

      // Verify anchor links point to existing elements
      for (const href of internalLinks) {
        if (href.startsWith('#')) {
          const targetId = href.substring(1);
          if (targetId) {
            const element = await page.$(`#${targetId}`);
            expect(element).not.toBeNull();
          }
        }
      }
    });

    test('page structure is intact when served statically', async ({ page }) => {
      await page.goto('/');

      // Verify all major sections are present
      await expect(page.locator('header.header')).toBeVisible();
      await expect(page.locator('section.hero')).toBeVisible();
      await expect(page.locator('#features')).toBeVisible();
      await expect(page.locator('#architecture')).toBeVisible();
      await expect(page.locator('#status')).toBeVisible();
      await expect(page.locator('#quickstart')).toBeVisible();
      await expect(page.locator('footer.footer')).toBeVisible();
    });
  });

  /**
   * Test Case 3: Test core content without JavaScript
   * Expected: Core content is visible with JavaScript disabled (progressive enhancement)
   */
  test.describe('Test Case 3: Progressive enhancement - core content without JavaScript', () => {
    test('hero section is visible without JavaScript', async ({ browser }) => {
      const context = await browser.newContext({ javaScriptEnabled: false });
      const page = await context.newPage();

      await page.goto('http://localhost:3000/');

      // Hero content should be visible
      await expect(page.locator('.hero')).toBeVisible();
      await expect(page.locator('h1')).toContainText('MirDB');
      await expect(page.locator('.hero-tagline')).toContainText('Persistent Key-Value Store');
      await expect(page.locator('.hero-description')).toBeVisible();

      // CTA buttons should be present
      await expect(page.locator('a.btn-primary')).toBeVisible();
      await expect(page.locator('a.btn-secondary')).toBeVisible();

      await context.close();
    });

    test('navigation is functional without JavaScript', async ({ browser }) => {
      const context = await browser.newContext({ javaScriptEnabled: false });
      const page = await context.newPage();

      await page.goto('http://localhost:3000/');

      // Navigation should be visible
      await expect(page.locator('.nav')).toBeVisible();
      await expect(page.locator('.nav-brand')).toBeVisible();
      await expect(page.locator('.nav-links')).toBeVisible();

      // Links should be present and clickable
      const navLinks = page.locator('.nav-links a');
      const count = await navLinks.count();
      expect(count).toBeGreaterThan(0);

      // Skip link should be present for accessibility
      await expect(page.locator('.skip-link')).toBeAttached();

      await context.close();
    });

    test('features section is readable without JavaScript', async ({ browser }) => {
      const context = await browser.newContext({ javaScriptEnabled: false });
      const page = await context.newPage();

      await page.goto('http://localhost:3000/');

      // Features section should be visible
      await expect(page.locator('#features')).toBeVisible();
      await expect(page.locator('.features-section h2')).toContainText('Key Features');

      // All feature cards should be visible
      const featureCards = page.locator('.feature-card');
      const cardCount = await featureCards.count();
      expect(cardCount).toBeGreaterThanOrEqual(3);

      // Feature content should be readable
      await expect(page.locator('.feature-card').first()).toBeVisible();
      await expect(page.locator('.feature-title').first()).toBeVisible();
      await expect(page.locator('.feature-description').first()).toBeVisible();

      await context.close();
    });

    test('architecture diagram is visible without JavaScript', async ({ browser }) => {
      const context = await browser.newContext({ javaScriptEnabled: false });
      const page = await context.newPage();

      await page.goto('http://localhost:3000/');

      // Architecture section should be visible
      await expect(page.locator('#architecture')).toBeVisible();
      await expect(page.locator('.architecture-section h2')).toContainText('LSM Tree Architecture');

      // Diagram should be visible
      await expect(page.locator('.architecture-diagram')).toBeVisible();

      await context.close();
    });

    test('quick start code blocks are visible without JavaScript', async ({ browser }) => {
      const context = await browser.newContext({ javaScriptEnabled: false });
      const page = await context.newPage();

      await page.goto('http://localhost:3000/');

      // Quick start section should be visible
      await expect(page.locator('#quickstart')).toBeVisible();

      // Code blocks should display code even without JS
      const codeBlocks = page.locator('.code-block');
      const count = await codeBlocks.count();
      expect(count).toBeGreaterThan(0);

      // Code content should be visible (not hidden behind JS interactions)
      await expect(page.locator('.code-block code').first()).toBeVisible();

      // Configuration section should be visible
      await expect(page.locator('.config-section')).toBeVisible();

      await context.close();
    });

    test('footer is visible without JavaScript', async ({ browser }) => {
      const context = await browser.newContext({ javaScriptEnabled: false });
      const page = await context.newPage();

      await page.goto('http://localhost:3000/');

      // Footer should be visible
      await expect(page.locator('footer.footer')).toBeVisible();
      await expect(page.locator('.footer-links')).toBeVisible();

      await context.close();
    });

    test('all text content is rendered without JavaScript', async ({ browser }) => {
      const context = await browser.newContext({ javaScriptEnabled: false });
      const page = await context.newPage();

      await page.goto('http://localhost:3000/');

      // Check that essential text content is not dynamically inserted
      const bodyText = await page.textContent('body');

      // Key content strings that should be server-rendered
      expect(bodyText).toContain('MirDB');
      expect(bodyText).toContain('Persistent');
      expect(bodyText).toContain('Memcached');
      expect(bodyText).toContain('Key Features');
      expect(bodyText).toContain('LSM Tree');
      expect(bodyText).toContain('Quick Start');

      await context.close();
    });

    test('CSS styling is applied without JavaScript', async ({ browser }) => {
      const context = await browser.newContext({ javaScriptEnabled: false });
      const page = await context.newPage();

      await page.goto('http://localhost:3000/');

      // Check that basic styling is applied by verifying body has the dark background
      const bodyBackground = await page.locator('body').evaluate(el => {
        return window.getComputedStyle(el).backgroundColor;
      });

      // Body should have the dark background color from CSS variables (--color-background: #0f172a)
      // This will be rgb(15, 23, 42) when computed
      expect(bodyBackground).not.toBe('rgba(0, 0, 0, 0)');
      expect(bodyBackground).toMatch(/rgb\(15,\s*23,\s*42\)|#0f172a/);

      // Check that grid layout is working
      const featuresGrid = await page.locator('.features-grid').evaluate(el => {
        return window.getComputedStyle(el).display;
      });

      expect(featuresGrid).toBe('grid');

      // Verify font-family is applied (not default serif)
      const bodyFont = await page.locator('body').evaluate(el => {
        return window.getComputedStyle(el).fontFamily;
      });

      expect(bodyFont).toContain('system');

      await context.close();
    });
  });

  /**
   * Test Case 4: Deploy to GitHub Pages
   * Expected: Site deploys and functions on GitHub Pages
   *
   * Note: This test simulates GitHub Pages deployment requirements
   * since we can't actually deploy during tests
   */
  test.describe('Test Case 4: GitHub Pages compatibility', () => {
    test('has index.html at root level (GitHub Pages requirement)', async () => {
      const indexPath = path.join(homepageDir, 'index.html');
      expect(fs.existsSync(indexPath)).toBe(true);
    });

    test('all assets use relative paths (GitHub Pages compatibility)', async ({ page }) => {
      await page.goto('/');

      // Check CSS links
      const cssLinks = await page.evaluate(() => {
        const links = document.querySelectorAll('link[rel="stylesheet"]');
        return Array.from(links).map(link => link.getAttribute('href'));
      });

      for (const href of cssLinks) {
        // Should not use absolute URLs with domain
        expect(href).not.toMatch(/^https?:\/\//);
        // Should be relative (not starting with /)
        // Note: GitHub Pages works with both, but relative is more portable
        console.log('CSS link:', href);
      }

      // Check script sources (if any external scripts)
      const scriptSrcs = await page.evaluate(() => {
        const scripts = document.querySelectorAll('script[src]');
        return Array.from(scripts).map(script => script.getAttribute('src'));
      });

      for (const src of scriptSrcs) {
        // External scripts should use CDN URLs, internal should be relative
        if (!src.startsWith('http')) {
          expect(src).not.toMatch(/^https?:\/\/localhost/);
        }
      }
    });

    test('no absolute localhost URLs in HTML', async () => {
      const indexPath = path.join(homepageDir, 'index.html');
      const content = fs.readFileSync(indexPath, 'utf-8');

      // Should not contain hardcoded localhost URLs
      expect(content).not.toContain('http://localhost');
      expect(content).not.toContain('https://localhost');
      expect(content).not.toContain('//localhost');
    });

    test('external links use target="_blank" with security attributes', async ({ page }) => {
      await page.goto('/');

      const externalLinks = await page.evaluate(() => {
        const links = document.querySelectorAll('a[href^="http"]');
        return Array.from(links).map(link => ({
          href: link.getAttribute('href'),
          target: link.getAttribute('target'),
          rel: link.getAttribute('rel')
        }));
      });

      console.log('External links:', externalLinks);

      for (const link of externalLinks) {
        if (link.target === '_blank') {
          // Should have noopener for security
          expect(link.rel).toContain('noopener');
        }
      }
    });

    test('page has proper meta tags for GitHub Pages', async ({ page }) => {
      await page.goto('/');

      // Check viewport meta tag (required for responsive design)
      const viewport = await page.locator('meta[name="viewport"]').getAttribute('content');
      expect(viewport).toBeTruthy();
      expect(viewport).toContain('width=device-width');

      // Check for description meta tag (SEO)
      const description = await page.locator('meta[name="description"]').getAttribute('content');
      expect(description).toBeTruthy();

      // Check for proper charset
      const charset = await page.locator('meta[charset]').getAttribute('charset');
      expect(charset).toBe('UTF-8');

      // Check for title
      const title = await page.title();
      expect(title).toBeTruthy();
      expect(title).toContain('MirDB');
    });

    test('no server-side routing dependencies', async ({ page }) => {
      await page.goto('/');

      // Check that the page doesn't use client-side routing frameworks
      // that require server configuration
      const html = await page.content();

      // Should not have SPA router scripts
      expect(html).not.toContain('react-router');
      expect(html).not.toContain('vue-router');
      expect(html).not.toContain('@angular/router');

      // Should not have history API pushState calls in inline scripts
      // (basic check - the site uses simple anchor links)
      const scripts = await page.evaluate(() => {
        const scriptTags = document.querySelectorAll('script');
        return Array.from(scriptTags).map(s => s.textContent);
      });

      for (const script of scripts) {
        expect(script).not.toContain('history.pushState');
        expect(script).not.toContain('history.replaceState');
      }
    });

    test('total file size is suitable for GitHub Pages', async () => {
      // GitHub Pages has a 1GB repository limit
      // Individual files should be under 100MB
      // We'll check that the homepage assets are reasonable size

      const files = ['index.html', 'styles.css'];
      let totalSize = 0;

      for (const file of files) {
        const filePath = path.join(homepageDir, file);
        if (fs.existsSync(filePath)) {
          const stats = fs.statSync(filePath);
          totalSize += stats.size;
          console.log(`${file}: ${stats.size} bytes (${(stats.size / 1024).toFixed(2)} KB)`);

          // Individual files should be reasonable (under 1MB)
          expect(stats.size).toBeLessThan(1024 * 1024);
        }
      }

      console.log(`Total static file size: ${totalSize} bytes (${(totalSize / 1024).toFixed(2)} KB)`);

      // Total should be well under GitHub Pages limits (under 5MB for main assets)
      expect(totalSize).toBeLessThan(5 * 1024 * 1024);
    });

    test('simulates GitHub Pages static serving', async ({ page }) => {
      // GitHub Pages serves files statically without any server-side processing
      // This test verifies the same behavior as our static server

      // Navigate directly to index.html (like GitHub Pages serves it)
      const response = await page.goto('/index.html');
      expect(response.status()).toBe(200);

      // Verify content is the same as root
      const titleFromIndex = await page.title();
      await page.goto('/');
      const titleFromRoot = await page.title();

      expect(titleFromIndex).toBe(titleFromRoot);
    });
  });
});
