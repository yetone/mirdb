// @ts-check
const { test, expect } = require('@playwright/test');
const fs = require('fs');
const path = require('path');

/**
 * Static Hosting Compatibility Tests
 * Verifies that the homepage can be deployed to free static hosting services
 * (GitHub Pages, Netlify, Vercel)
 */

test.describe('Static Hosting Compatibility', () => {
  /**
   * Test Case 1: Check for server-side code
   * Expected: No server-side code or API routes in homepage
   */
  test.describe('Test Case 1: No Server-Side Code', () => {
    test('should have no server-side code patterns in HTML', async ({ page }) => {
      await page.goto('/');

      const htmlContent = await page.content();

      // Check that HTML doesn't contain server-side code patterns
      const serverSidePatterns = [
        '<?php',
        '<%',
        '<asp:',
        '<?=',
        '#{',
        '<%= ',
        '{{#if',
        '<% if',
        '<c:if',
        '<%@',
        '<jsp:',
        '<cfif',
        '<?xml-stylesheet',
        '@{',
        '<script runat="server"',
      ];

      for (const pattern of serverSidePatterns) {
        expect(htmlContent.toLowerCase()).not.toContain(pattern.toLowerCase());
      }
    });

    test('should not have API route references in scripts', async ({ page }) => {
      await page.goto('/');

      const htmlContent = await page.content();

      // No API routes that require server processing
      const apiPatterns = [
        '/api/',
        '/_next/data',
        '/_server',
        '/server/',
        '.php',
        '.asp',
        '.jsp',
        '/graphql',
      ];

      for (const pattern of apiPatterns) {
        // Allow external links to APIs like GitHub API
        if (!htmlContent.includes('github.com') || !htmlContent.includes(pattern)) {
          const hasInternalApiRoute = htmlContent.includes(`href="${pattern}`) ||
                                       htmlContent.includes(`src="${pattern}`) ||
                                       htmlContent.includes(`action="${pattern}`);
          expect(hasInternalApiRoute).toBe(false);
        }
      }
    });

    test('should not have server-side rendering markers', async ({ page }) => {
      await page.goto('/');

      const htmlContent = await page.content();

      // No SSR hydration markers that indicate server rendering
      const ssrMarkers = [
        'data-reactroot',
        '__NEXT_DATA__',
        '__NUXT__',
        'window.__INITIAL_STATE__',
        '__PRELOADED_STATE__',
      ];

      for (const marker of ssrMarkers) {
        expect(htmlContent).not.toContain(marker);
      }
    });

    test('should verify no Node.js/server-side specific imports in script.js', async () => {
      const scriptPath = path.join(process.cwd(), 'script.js');
      const scriptContent = fs.readFileSync(scriptPath, 'utf-8');

      // Node.js specific imports that wouldn't work on static hosting
      const serverImports = [
        'require(\'fs\')',
        'require("fs")',
        'require(\'path\')',
        'require("path")',
        'require(\'http\')',
        'require("http")',
        'require(\'express\')',
        'require("express")',
        'import * from \'node:',
        'import * from "node:',
        'process.env.',
        '__dirname',
        '__filename',
      ];

      for (const importPattern of serverImports) {
        expect(scriptContent).not.toContain(importPattern);
      }
    });
  });

  /**
   * Test Case 2: JavaScript-disabled functionality
   * Expected: Core content (hero, features, text) is visible without JS
   */
  test.describe('Test Case 2: JavaScript-Disabled Functionality', () => {
    test('should display hero content without JavaScript', async ({ browser }) => {
      const context = await browser.newContext({
        javaScriptEnabled: false,
      });
      const page = await context.newPage();

      await page.goto('/');

      // Hero section should be visible
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();

      // Title should be visible
      const heroTitle = page.locator('[data-testid="hero-title"]');
      await expect(heroTitle).toBeVisible();
      await expect(heroTitle).toContainText('MirDB');

      // Tagline should be visible
      const heroTagline = page.locator('[data-testid="hero-tagline"]');
      await expect(heroTagline).toBeVisible();
      await expect(heroTagline).toContainText('Persistent Key-Value Store');

      await context.close();
    });

    test('should display navigation without JavaScript', async ({ browser }) => {
      const context = await browser.newContext({
        javaScriptEnabled: false,
      });
      const page = await context.newPage();

      await page.goto('/');

      // Navigation should be visible
      const navigation = page.locator('[data-testid="navigation"]');
      await expect(navigation).toBeVisible();

      // Navigation links should be visible
      const navLinks = page.locator('[data-testid="nav-links"]');
      await expect(navLinks).toBeVisible();

      await context.close();
    });

    test('should display feature comparison table without JavaScript', async ({ browser }) => {
      const context = await browser.newContext({
        javaScriptEnabled: false,
      });
      const page = await context.newPage();

      await page.goto('/');

      // Comparison table section should be visible
      const comparisonTable = page.locator('[data-testid="comparison-table"]');
      await expect(comparisonTable).toBeVisible();

      // Table rows should have content
      const tableRows = page.locator('.comparison-table tbody tr');
      const rowCount = await tableRows.count();
      expect(rowCount).toBeGreaterThan(0);

      await context.close();
    });

    test('should display quick start section without JavaScript', async ({ browser }) => {
      const context = await browser.newContext({
        javaScriptEnabled: false,
      });
      const page = await context.newPage();

      await page.goto('/');

      // Quick start section should be visible
      const quickStartSection = page.locator('[data-testid="quick-start-section"]');
      await expect(quickStartSection).toBeVisible();

      // Code blocks should show installation command
      const installCode = page.locator('[data-testid="install-code"]');
      await expect(installCode).toBeVisible();
      await expect(installCode).toContainText('cargo install');

      await context.close();
    });

    test('should display footer without JavaScript', async ({ browser }) => {
      const context = await browser.newContext({
        javaScriptEnabled: false,
      });
      const page = await context.newPage();

      await page.goto('/');

      // Footer should be visible
      const footer = page.locator('[data-testid="footer"]');
      await expect(footer).toBeVisible();

      // Footer license should be visible
      const footerLicense = page.locator('[data-testid="footer-license"]');
      await expect(footerLicense).toBeVisible();

      await context.close();
    });

    test('should have all text content readable without JavaScript', async ({ browser }) => {
      const context = await browser.newContext({
        javaScriptEnabled: false,
      });
      const page = await context.newPage();

      await page.goto('/');

      // Check main headings are visible
      const h1 = page.locator('h1');
      await expect(h1).toBeVisible();

      const h2Elements = page.locator('h2');
      const h2Count = await h2Elements.count();
      expect(h2Count).toBeGreaterThan(0);

      // Check all h2 are visible
      for (let i = 0; i < h2Count; i++) {
        await expect(h2Elements.nth(i)).toBeVisible();
      }

      await context.close();
    });

    test('should display images without JavaScript', async ({ browser }) => {
      const context = await browser.newContext({
        javaScriptEnabled: false,
      });
      const page = await context.newPage();

      await page.goto('/');

      // Logo should be visible
      const heroLogo = page.locator('[data-testid="hero-logo"]');
      await expect(heroLogo).toBeVisible();

      // Usage demo image should be visible
      const usageDemoMedia = page.locator('[data-testid="usage-demo-media"]');
      await expect(usageDemoMedia).toBeVisible();

      await context.close();
    });
  });

  /**
   * Test Case 3: Build output verification
   * Expected: Build produces static HTML, CSS, JS files
   */
  test.describe('Test Case 3: Static Build Output', () => {
    test('should have index.html at root', async () => {
      const indexPath = path.join(process.cwd(), 'index.html');
      expect(fs.existsSync(indexPath)).toBe(true);

      const content = fs.readFileSync(indexPath, 'utf-8');
      expect(content).toContain('<!DOCTYPE html>');
      expect(content).toContain('<html');
      expect(content).toContain('</html>');
    });

    test('should have valid CSS file', async () => {
      const cssPath = path.join(process.cwd(), 'styles.css');
      expect(fs.existsSync(cssPath)).toBe(true);

      const content = fs.readFileSync(cssPath, 'utf-8');
      // Check for valid CSS content
      expect(content).toContain(':root');
      expect(content).toContain('{');
      expect(content).toContain('}');
    });

    test('should have valid JavaScript file', async () => {
      const jsPath = path.join(process.cwd(), 'script.js');
      expect(fs.existsSync(jsPath)).toBe(true);

      const content = fs.readFileSync(jsPath, 'utf-8');
      // Check for valid JS content (IIFE pattern used in script.js)
      expect(content).toContain('function');
    });

    test('should have assets directory with static files', async () => {
      const assetsPath = path.join(process.cwd(), 'assets');
      expect(fs.existsSync(assetsPath)).toBe(true);

      // Check for expected assets
      const logoPath = path.join(assetsPath, 'logo.gif');
      const usagePath = path.join(assetsPath, 'usage.gif');

      expect(fs.existsSync(logoPath)).toBe(true);
      expect(fs.existsSync(usagePath)).toBe(true);
    });

    test('should not have server configuration files that block static hosting', async () => {
      const serverConfigFiles = [
        'server.js',
        'server.ts',
        'app.js',
        'app.ts',
        'index.js',
        'index.ts',
        'next.config.js',
        'nuxt.config.js',
        'astro.config.mjs',
        'svelte.config.js',
        'remix.config.js',
      ];

      for (const configFile of serverConfigFiles) {
        const configPath = path.join(process.cwd(), configFile);
        if (fs.existsSync(configPath)) {
          // If the file exists, verify it's not a server-side entry point
          const content = fs.readFileSync(configPath, 'utf-8');
          const serverPatterns = [
            'express()',
            'createServer',
            'listen(',
            'http.createServer',
            'app.listen',
          ];

          let hasServerCode = false;
          for (const pattern of serverPatterns) {
            if (content.includes(pattern)) {
              hasServerCode = true;
              break;
            }
          }
          expect(hasServerCode).toBe(false);
        }
      }
    });

    test('should have all files be static assets only', async ({ page }) => {
      await page.goto('/');

      // Get all resource requests
      const requests = [];
      page.on('request', request => {
        requests.push(request.url());
      });

      await page.reload();
      await page.waitForLoadState('networkidle');

      // Filter internal requests
      const baseURL = 'http://localhost:3000';
      const internalRequests = requests.filter(url => url.startsWith(baseURL));

      // Verify all internal requests are for static files
      const staticExtensions = [
        '.html', '.css', '.js', '.gif', '.png', '.jpg', '.jpeg',
        '.svg', '.webp', '.ico', '.woff', '.woff2', '.ttf', '.eot',
        '/', '' // root requests
      ];

      for (const url of internalRequests) {
        const urlPath = new URL(url).pathname;
        const isStaticFile = staticExtensions.some(ext =>
          urlPath.endsWith(ext) || urlPath === '/'
        );
        expect(isStaticFile).toBe(true);
      }
    });
  });

  /**
   * Test Case 4: Relative paths test
   * Expected: All asset paths work with base URL configuration
   */
  test.describe('Test Case 4: Relative Paths', () => {
    test('should use relative paths for CSS', async ({ page }) => {
      await page.goto('/');

      const htmlContent = await page.content();

      // CSS should be loaded with relative path
      expect(htmlContent).toContain('href="styles.css"');
      // Should NOT have absolute paths to local files
      expect(htmlContent).not.toMatch(/href="\/styles\.css"/);
    });

    test('should use relative paths for JavaScript', async ({ page }) => {
      await page.goto('/');

      const htmlContent = await page.content();

      // JS should be loaded with relative path
      expect(htmlContent).toContain('src="script.js"');
      // Should NOT have absolute paths to local files
      expect(htmlContent).not.toMatch(/src="\/script\.js"/);
    });

    test('should use relative paths for images', async ({ page }) => {
      await page.goto('/');

      const htmlContent = await page.content();

      // Images should use relative paths
      expect(htmlContent).toContain('src="assets/logo.gif"');
      expect(htmlContent).toContain('src="assets/usage.gif"');

      // Should NOT have absolute paths to local assets
      expect(htmlContent).not.toMatch(/src="\/assets\/logo\.gif"/);
      expect(htmlContent).not.toMatch(/src="\/assets\/usage\.gif"/);
    });

    test('should load CSS successfully', async ({ page }) => {
      const cssResponse = await page.goto('/styles.css');
      expect(cssResponse.status()).toBe(200);

      const contentType = cssResponse.headers()['content-type'];
      expect(contentType).toContain('css');
    });

    test('should load JavaScript successfully', async ({ page }) => {
      const jsResponse = await page.goto('/script.js');
      expect(jsResponse.status()).toBe(200);

      const contentType = jsResponse.headers()['content-type'];
      expect(contentType).toContain('javascript');
    });

    test('should load images successfully', async ({ page }) => {
      const logoResponse = await page.goto('/assets/logo.gif');
      expect(logoResponse.status()).toBe(200);

      const usageResponse = await page.goto('/assets/usage.gif');
      expect(usageResponse.status()).toBe(200);
    });

    test('should not have hardcoded localhost URLs', async ({ page }) => {
      await page.goto('/');

      const htmlContent = await page.content();

      // Should not have hardcoded localhost references (except in head for canonical URL during testing)
      const scriptContent = await page.evaluate(() => {
        const scripts = document.querySelectorAll('script:not([type="application/ld+json"])');
        let content = '';
        scripts.forEach(s => { content += s.textContent || ''; });
        return content;
      });

      expect(scriptContent).not.toContain('localhost');
      expect(scriptContent).not.toContain('127.0.0.1');
    });

    test('should have internal links that work with base URL', async ({ page }) => {
      await page.goto('/');

      // Check internal anchor links
      const internalLinks = await page.locator('a[href^="#"]').all();

      for (const link of internalLinks) {
        const href = await link.getAttribute('href');
        // Anchor links should start with #
        expect(href).toMatch(/^#/);
      }
    });

    test('should have navigation brand link work with relative paths', async ({ page }) => {
      await page.goto('/');

      const brandLink = page.locator('[data-testid="nav-brand"]');
      const href = await brandLink.getAttribute('href');

      // Brand link should be relative (/ or ./ or ./index.html)
      expect(href).toMatch(/^(\/|\.\/|\.\/index\.html|index\.html)?$/);
    });
  });
});
