// @ts-check
const { test, expect } = require('@playwright/test');
const fs = require('fs');
const path = require('path');

/**
 * Static Page Deployment Tests
 *
 * Test Case 1: Check for server-side code - Page contains only static HTML, CSS, and JavaScript
 * Test Case 2: Count external CDN dependencies - External dependencies are minimized for reliability
 * Test Case 3: Test page without JavaScript - Core content is accessible even if JavaScript fails to load
 */

test.describe('Static Page Deployment', () => {

  test.describe('Test Case 1: Static Files Only - No Server-Side Code', () => {

    test('should consist only of static HTML, CSS, and JS files', async () => {
      const projectRoot = path.resolve(__dirname, '..');
      const indexPath = path.join(projectRoot, 'index.html');

      // Verify index.html exists
      expect(fs.existsSync(indexPath)).toBe(true);

      // Read the HTML file
      const htmlContent = fs.readFileSync(indexPath, 'utf-8');

      // Should be a valid HTML document
      expect(htmlContent).toContain('<!DOCTYPE html>');
      expect(htmlContent).toContain('<html');
      expect(htmlContent).toContain('</html>');
    });

    test('should not contain PHP server-side code', async () => {
      const projectRoot = path.resolve(__dirname, '..');
      const indexPath = path.join(projectRoot, 'index.html');
      const htmlContent = fs.readFileSync(indexPath, 'utf-8');

      // Check for PHP markers
      expect(htmlContent).not.toContain('<?php');
      expect(htmlContent).not.toContain('<?=');
      expect(htmlContent).not.toMatch(/<\?[^xml]/);
    });

    test('should not contain ASP.NET server-side code', async () => {
      const projectRoot = path.resolve(__dirname, '..');
      const indexPath = path.join(projectRoot, 'index.html');
      const htmlContent = fs.readFileSync(indexPath, 'utf-8');

      // Check for ASP markers
      expect(htmlContent).not.toContain('<%');
      expect(htmlContent).not.toContain('%>');
      expect(htmlContent).not.toContain('@{');
      expect(htmlContent).not.toContain('runat="server"');
    });

    test('should not contain server-side template engine markers', async () => {
      const projectRoot = path.resolve(__dirname, '..');
      const indexPath = path.join(projectRoot, 'index.html');
      const htmlContent = fs.readFileSync(indexPath, 'utf-8');

      // Check for common template engines (EJS, Jinja, Handlebars server-side syntax)
      expect(htmlContent).not.toContain('<%=');
      expect(htmlContent).not.toContain('<%-');
      expect(htmlContent).not.toContain('{%');
      expect(htmlContent).not.toContain('{{#');
      expect(htmlContent).not.toContain('{{>');
    });

    test('should not reference server-side file extensions', async () => {
      const projectRoot = path.resolve(__dirname, '..');
      const indexPath = path.join(projectRoot, 'index.html');
      const htmlContent = fs.readFileSync(indexPath, 'utf-8');

      // Check for server-side script references
      expect(htmlContent).not.toMatch(/href\s*=\s*["'][^"']*\.php["']/i);
      expect(htmlContent).not.toMatch(/href\s*=\s*["'][^"']*\.asp["']/i);
      expect(htmlContent).not.toMatch(/href\s*=\s*["'][^"']*\.aspx["']/i);
      expect(htmlContent).not.toMatch(/href\s*=\s*["'][^"']*\.jsp["']/i);
      expect(htmlContent).not.toMatch(/src\s*=\s*["'][^"']*\.php["']/i);
    });

    test('should only use standard static file types for resources', async () => {
      const projectRoot = path.resolve(__dirname, '..');
      const indexPath = path.join(projectRoot, 'index.html');
      const htmlContent = fs.readFileSync(indexPath, 'utf-8');

      // Extract all src and href attributes
      const srcMatches = htmlContent.match(/src\s*=\s*["']([^"']+)["']/gi) || [];
      const hrefMatches = htmlContent.match(/href\s*=\s*["']([^"']+)["']/gi) || [];

      const allResources = [...srcMatches, ...hrefMatches];

      // For non-anchor links, verify they're static resources
      for (const resource of allResources) {
        const urlMatch = resource.match(/["']([^"']+)["']/);
        if (urlMatch) {
          const url = urlMatch[1];
          // Skip anchor links, external URLs without path extensions, and data URIs
          if (url.startsWith('#') || url.startsWith('data:') || url.startsWith('javascript:')) {
            continue;
          }
          // For URLs with file extensions, verify they're static types
          if (url.includes('.') && !url.match(/^https?:\/\//)) {
            const extension = url.split('.').pop()?.split('?')[0]?.toLowerCase();
            const staticExtensions = ['html', 'css', 'js', 'png', 'jpg', 'jpeg', 'gif', 'svg', 'webp', 'ico', 'woff', 'woff2', 'ttf', 'eot', 'json', 'xml', 'txt', 'md'];
            expect(staticExtensions).toContain(extension);
          }
        }
      }
    });

    test('should have inline styles (no external CSS requiring server processing)', async () => {
      const projectRoot = path.resolve(__dirname, '..');
      const indexPath = path.join(projectRoot, 'index.html');
      const htmlContent = fs.readFileSync(indexPath, 'utf-8');

      // Page should have <style> tags with CSS (inline or external static CSS)
      // Either inline style or valid external CSS links are acceptable
      const hasInlineStyles = htmlContent.includes('<style>') || htmlContent.includes('<style ');
      const hasExternalCSS = htmlContent.match(/<link[^>]+rel=["']stylesheet["'][^>]*>/);

      expect(hasInlineStyles || hasExternalCSS).toBe(true);
    });

    test('should use only client-side JavaScript', async () => {
      const projectRoot = path.resolve(__dirname, '..');
      const indexPath = path.join(projectRoot, 'index.html');
      const htmlContent = fs.readFileSync(indexPath, 'utf-8');

      // Check that any script tags are for client-side JS
      const scriptTags = htmlContent.match(/<script[^>]*>[\s\S]*?<\/script>/gi) || [];

      for (const script of scriptTags) {
        // Should not contain Node.js specific APIs that wouldn't work in browser
        expect(script).not.toContain('require(\'fs\')');
        expect(script).not.toContain('require("fs")');
        expect(script).not.toContain('require(\'http\')');
        expect(script).not.toContain('require("http")');
        expect(script).not.toContain('require(\'express\')');
        expect(script).not.toContain('require("express")');
        expect(script).not.toContain('process.env');
      }
    });
  });

  test.describe('Test Case 2: External CDN Dependencies', () => {

    test('should have minimal external CDN dependencies', async ({ page }) => {
      const projectRoot = path.resolve(__dirname, '..');
      const indexPath = path.join(projectRoot, 'index.html');
      const htmlContent = fs.readFileSync(indexPath, 'utf-8');

      // Count external script sources from CDNs
      const externalScripts = htmlContent.match(/<script[^>]+src=["']https?:\/\/[^"']+["'][^>]*>/gi) || [];
      const externalStyles = htmlContent.match(/<link[^>]+href=["']https?:\/\/[^"']+\.css["'][^>]*>/gi) || [];

      const totalExternalDependencies = externalScripts.length + externalStyles.length;

      // Should have 10 or fewer external dependencies for reliability
      // This is a reasonable threshold for a landing page
      expect(totalExternalDependencies).toBeLessThanOrEqual(10);
    });

    test('should have documented external dependencies', async () => {
      const projectRoot = path.resolve(__dirname, '..');
      const indexPath = path.join(projectRoot, 'index.html');
      const htmlContent = fs.readFileSync(indexPath, 'utf-8');

      // Extract all external dependencies
      const externalScriptMatches = htmlContent.match(/<script[^>]+src=["'](https?:\/\/[^"']+)["'][^>]*>/gi) || [];
      const externalStyleMatches = htmlContent.match(/<link[^>]+href=["'](https?:\/\/[^"']+\.css)["'][^>]*>/gi) || [];

      // All dependencies should be from well-known CDNs
      const knownCDNs = [
        'cdnjs.cloudflare.com',
        'cdn.jsdelivr.net',
        'unpkg.com',
        'fonts.googleapis.com',
        'fonts.gstatic.com',
        'cdn.tailwindcss.com',
        'stackpath.bootstrapcdn.com',
        'maxcdn.bootstrapcdn.com',
        'code.jquery.com'
      ];

      for (const script of externalScriptMatches) {
        const urlMatch = script.match(/src=["'](https?:\/\/[^"']+)["']/i);
        if (urlMatch) {
          const url = urlMatch[1];
          const isKnownCDN = knownCDNs.some(cdn => url.includes(cdn));
          expect(isKnownCDN).toBe(true);
        }
      }

      for (const style of externalStyleMatches) {
        const urlMatch = style.match(/href=["'](https?:\/\/[^"']+)["']/i);
        if (urlMatch) {
          const url = urlMatch[1];
          const isKnownCDN = knownCDNs.some(cdn => url.includes(cdn));
          expect(isKnownCDN).toBe(true);
        }
      }
    });

    test('should use versioned CDN URLs for stability', async () => {
      const projectRoot = path.resolve(__dirname, '..');
      const indexPath = path.join(projectRoot, 'index.html');
      const htmlContent = fs.readFileSync(indexPath, 'utf-8');

      // Extract external URLs
      const externalURLs = htmlContent.match(/https?:\/\/cdn[^"'\s]+/gi) || [];

      // CDN URLs should include version numbers for reliability
      for (const url of externalURLs) {
        // Check for version pattern in URL (e.g., /11.9.0/ or @1.2.3)
        const hasVersion = /\/\d+\.\d+(\.\d+)?\/|@\d+\.\d+(\.\d+)?/.test(url);
        expect(hasVersion).toBe(true);
      }
    });

    test('should not depend on more than 3 unique CDN providers', async () => {
      const projectRoot = path.resolve(__dirname, '..');
      const indexPath = path.join(projectRoot, 'index.html');
      const htmlContent = fs.readFileSync(indexPath, 'utf-8');

      // Extract all external CDN hosts
      const externalURLs = htmlContent.match(/https?:\/\/[^"'\s]+/gi) || [];

      const cdnHosts = new Set();
      for (const url of externalURLs) {
        try {
          const urlObj = new URL(url);
          // Only count CDN hosts, not GitHub or other links
          if (urlObj.hostname.includes('cdn') ||
              urlObj.hostname.includes('cloudflare') ||
              urlObj.hostname.includes('jsdelivr') ||
              urlObj.hostname.includes('unpkg')) {
            cdnHosts.add(urlObj.hostname);
          }
        } catch {
          // Invalid URL, skip
        }
      }

      // Should use 3 or fewer CDN providers
      expect(cdnHosts.size).toBeLessThanOrEqual(3);
    });
  });

  test.describe('Test Case 3: Accessibility Without JavaScript', () => {

    test('should display core content with JavaScript disabled', async ({ browser }) => {
      // Create a new context with JavaScript disabled
      const context = await browser.newContext({
        javaScriptEnabled: false
      });
      const page = await context.newPage();

      await page.goto('/');

      // Check that core content is visible
      await expect(page.locator('h1')).toBeVisible();
      await expect(page.locator('h1')).toContainText('MirDB');

      // Navigation should be visible
      await expect(page.locator('nav')).toBeVisible();

      // Hero section content should be visible
      await expect(page.locator('#hero')).toBeVisible();
      await expect(page.locator('.tagline')).toContainText('Persistent Key-Value Store');

      await context.close();
    });

    test('should have readable feature descriptions without JavaScript', async ({ browser }) => {
      const context = await browser.newContext({
        javaScriptEnabled: false
      });
      const page = await context.newPage();

      await page.goto('/');

      // Features section should be visible
      await expect(page.locator('#features')).toBeVisible();

      // Feature cards should have content
      const featureCards = page.locator('.feature-card');
      await expect(featureCards).toHaveCount(6);

      // Each feature card should have visible text
      const firstCard = featureCards.first();
      await expect(firstCard).toBeVisible();
      await expect(firstCard.locator('h3')).toBeVisible();
      await expect(firstCard.locator('p')).toBeVisible();

      await context.close();
    });

    test('should have accessible navigation links without JavaScript', async ({ browser }) => {
      const context = await browser.newContext({
        javaScriptEnabled: false
      });
      const page = await context.newPage();

      await page.goto('/');

      // Navigation links should be visible and have href
      const navLinks = page.locator('.nav-links a');
      const count = await navLinks.count();
      expect(count).toBeGreaterThan(0);

      for (let i = 0; i < count; i++) {
        const link = navLinks.nth(i);
        await expect(link).toBeVisible();
        const href = await link.getAttribute('href');
        expect(href).toBeTruthy();
      }

      await context.close();
    });

    test('should display Quick Start section without JavaScript', async ({ browser }) => {
      const context = await browser.newContext({
        javaScriptEnabled: false
      });
      const page = await context.newPage();

      await page.goto('/');

      // Quick Start section should be visible
      await expect(page.locator('#quick-start')).toBeVisible();
      await expect(page.locator('#quick-start h2')).toContainText('Quick Start');

      // Steps should be visible
      const steps = page.locator('.quick-start-step');
      await expect(steps).toHaveCount(4);

      await context.close();
    });

    test('should display code blocks without JavaScript (no syntax highlighting required)', async ({ browser }) => {
      const context = await browser.newContext({
        javaScriptEnabled: false
      });
      const page = await context.newPage();

      await page.goto('/');

      // Code blocks should be visible
      const codeBlocks = page.locator('pre code');
      const count = await codeBlocks.count();
      expect(count).toBeGreaterThan(0);

      // At least one code block should have text content
      const firstCodeBlock = codeBlocks.first();
      await expect(firstCodeBlock).toBeVisible();
      const textContent = await firstCodeBlock.textContent();
      expect(textContent?.trim().length).toBeGreaterThan(0);

      await context.close();
    });

    test('should display Commands section without JavaScript', async ({ browser }) => {
      const context = await browser.newContext({
        javaScriptEnabled: false
      });
      const page = await context.newPage();

      await page.goto('/');

      // Commands section should be visible
      await expect(page.locator('#commands')).toBeVisible();
      await expect(page.locator('#commands h2')).toContainText('Supported Commands');

      // Command lists should have content
      const commandLists = page.locator('.command-list');
      await expect(commandLists.first()).toBeVisible();

      await context.close();
    });

    test('should display Configuration table without JavaScript', async ({ browser }) => {
      const context = await browser.newContext({
        javaScriptEnabled: false
      });
      const page = await context.newPage();

      await page.goto('/');

      // Configuration section should be visible
      await expect(page.locator('#configuration')).toBeVisible();

      // Configuration table should be visible with headers
      const configTable = page.locator('.config-table');
      await expect(configTable).toBeVisible();

      const tableHeaders = configTable.locator('th');
      await expect(tableHeaders).toHaveCount(3);

      await context.close();
    });

    test('should display footer without JavaScript', async ({ browser }) => {
      const context = await browser.newContext({
        javaScriptEnabled: false
      });
      const page = await context.newPage();

      await page.goto('/');

      // Footer should be visible
      await expect(page.locator('footer')).toBeVisible();

      // Footer links should be accessible
      const footerLinks = page.locator('.footer-links a');
      const count = await footerLinks.count();
      expect(count).toBeGreaterThan(0);

      await context.close();
    });

    test('should maintain proper heading structure without JavaScript', async ({ browser }) => {
      const context = await browser.newContext({
        javaScriptEnabled: false
      });
      const page = await context.newPage();

      await page.goto('/');

      // Should have one h1
      const h1Elements = page.locator('h1');
      await expect(h1Elements).toHaveCount(1);

      // Should have multiple h2 elements for sections
      const h2Elements = page.locator('h2');
      const h2Count = await h2Elements.count();
      expect(h2Count).toBeGreaterThanOrEqual(4);

      await context.close();
    });

    test('should have CTA buttons accessible without JavaScript', async ({ browser }) => {
      const context = await browser.newContext({
        javaScriptEnabled: false
      });
      const page = await context.newPage();

      await page.goto('/');

      // CTA buttons should be visible and have valid href
      const ctaButtons = page.locator('.cta-buttons .btn');
      await expect(ctaButtons).toHaveCount(2);

      const primaryBtn = ctaButtons.first();
      await expect(primaryBtn).toBeVisible();
      const href = await primaryBtn.getAttribute('href');
      expect(href).toBeTruthy();

      await context.close();
    });
  });
});
