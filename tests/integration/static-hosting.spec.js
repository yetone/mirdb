/**
 * Static Hosting Compatibility Tests
 * Owner: Scenario 13 - Static Hosting
 *
 * Test cases:
 * - No server-side dependencies
 * - Relative asset paths
 * - Works with static file server
 */

const { test, expect } = require('@playwright/test');
const fs = require('fs');
const path = require('path');

test.describe('Static Hosting Compatibility', () => {
  test.describe('Static File Server Functionality', () => {
    test('should serve homepage correctly via HTTP server', async ({ page }) => {
      // Navigate to homepage served by static server
      await page.goto('/');

      // Verify page loads
      await expect(page).toHaveTitle(/MirDB/);

      // Verify main content is visible
      await expect(page.locator('h1')).toBeVisible();
      await expect(page.locator('main')).toBeVisible();
    });

    test('should load all CSS stylesheets successfully', async ({ page }) => {
      // Track failed requests
      const failedRequests = [];
      page.on('requestfailed', (request) => {
        if (request.url().endsWith('.css')) {
          failedRequests.push(request.url());
        }
      });

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // No CSS files should fail to load
      expect(failedRequests).toHaveLength(0);

      // Verify styles are applied (body has dark background)
      const bodyBg = await page.evaluate(() => {
        return window.getComputedStyle(document.body).backgroundColor;
      });
      expect(bodyBg).not.toBe('rgba(0, 0, 0, 0)');
    });

    test('should load all JavaScript files successfully', async ({ page }) => {
      const failedRequests = [];
      page.on('requestfailed', (request) => {
        if (request.url().endsWith('.js')) {
          failedRequests.push(request.url());
        }
      });

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // No JS files should fail to load
      expect(failedRequests).toHaveLength(0);
    });

    test('should load local images successfully', async ({ page }) => {
      const failedImages = [];
      page.on('requestfailed', (request) => {
        if (
          request.resourceType() === 'image' &&
          !request.url().includes('circleci')
        ) {
          failedImages.push(request.url());
        }
      });

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Local images should load (excluding external CI badges which may fail)
      expect(failedImages).toHaveLength(0);
    });

    test('should function without JavaScript errors', async ({ page }) => {
      const jsErrors = [];
      page.on('pageerror', (error) => {
        jsErrors.push(error.message);
      });

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // No JavaScript errors should occur
      expect(jsErrors).toHaveLength(0);
    });
  });

  test.describe('Relative Path Verification', () => {
    test('all stylesheet paths should be relative', async ({ page }) => {
      await page.goto('/');

      const stylesheets = await page.evaluate(() => {
        const links = document.querySelectorAll('link[rel="stylesheet"]');
        return Array.from(links).map((link) => link.getAttribute('href'));
      });

      stylesheets.forEach((href) => {
        // Should not be root-relative
        expect(href).not.toMatch(/^\//);
        // Should not be absolute URL
        expect(href).not.toMatch(/^https?:\/\//);
      });
    });

    test('all script paths should be relative', async ({ page }) => {
      await page.goto('/');

      const scripts = await page.evaluate(() => {
        const scriptElements = document.querySelectorAll('script[src]');
        return Array.from(scriptElements).map((script) =>
          script.getAttribute('src')
        );
      });

      scripts.forEach((src) => {
        // Should not be root-relative
        expect(src).not.toMatch(/^\//);
        // Should not be absolute URL
        expect(src).not.toMatch(/^https?:\/\//);
      });
    });

    test('local image paths should be relative', async ({ page }) => {
      await page.goto('/');

      const images = await page.evaluate(() => {
        const imgElements = document.querySelectorAll('img');
        return Array.from(imgElements)
          .map((img) => img.getAttribute('src'))
          .filter((src) => !src.startsWith('http')); // Exclude external images
      });

      images.forEach((src) => {
        // Local images should not be root-relative
        expect(src).not.toMatch(/^\//);
      });
    });
  });

  test.describe('No Backend Dependencies', () => {
    test('page should render completely without dynamic content loading', async ({
      page,
    }) => {
      await page.goto('/');

      // All main sections should be present (not lazy-loaded from API)
      await expect(page.locator('#hero')).toBeVisible();
      await expect(page.locator('#features')).toBeVisible();
      await expect(page.locator('#quickstart')).toBeVisible();
      await expect(page.locator('#status')).toBeVisible();
    });

    test('page source should not contain server-side markers', async ({
      page,
    }) => {
      await page.goto('/');

      const html = await page.content();

      // SSI markers
      expect(html).not.toMatch(/<!--\s*#include/i);
      expect(html).not.toMatch(/<!--\s*#\w+/);

      // PHP tags
      expect(html).not.toMatch(/<\?php/i);

      // ASP tags
      expect(html).not.toMatch(/<%[^-!]/);

      // Common SSR hydration markers
      expect(html).not.toMatch(/__NEXT_DATA__/);
      expect(html).not.toMatch(/__NUXT__/);
    });

    test('navigation should work without JavaScript', async ({ browser }) => {
      // Create a context with JavaScript disabled
      const context = await browser.newContext({ javaScriptEnabled: false });
      const page = await context.newPage();

      await page.goto('/');

      // Page should still render
      await expect(page.locator('h1')).toBeVisible();

      // Navigation links should be present
      const navLinks = page.locator('nav a');
      await expect(navLinks.first()).toBeVisible();

      await context.close();
    });

    test('all content should be present in initial HTML', async ({ page }) => {
      // Get the raw HTML before any JS execution
      const response = await page.goto('/');
      const html = await response.text();

      // Key content should be in the HTML
      expect(html).toContain('MirDB');
      expect(html).toContain('Memcached Protocol');
      expect(html).toContain('Persistent Storage');
      expect(html).toContain('LSM Tree');
      expect(html).toContain('Quick Start');
    });
  });

  test.describe('Cross-Origin Resource Handling', () => {
    test('external resources should have appropriate security attributes', async ({
      page,
    }) => {
      await page.goto('/');

      // External links should have rel="noopener"
      const externalLinks = await page.evaluate(() => {
        const links = document.querySelectorAll('a[target="_blank"]');
        return Array.from(links).map((link) => ({
          href: link.href,
          rel: link.rel,
        }));
      });

      externalLinks.forEach((link) => {
        expect(link.rel).toContain('noopener');
      });
    });
  });
});

test.describe('File Protocol Simulation', () => {
  // Note: Playwright cannot truly test file:// protocol,
  // but we can verify the page works without a complex server setup
  test('page should work with minimal server requirements', async ({
    page,
  }) => {
    // This test verifies the page works with a simple static file server
    // which is similar to file:// protocol behavior
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Core content should be visible
    await expect(page.locator('h1')).toContainText('MirDB');

    // Sections should be navigable
    await expect(page.locator('#hero')).toBeVisible();
    await expect(page.locator('#features')).toBeVisible();
    await expect(page.locator('#quickstart')).toBeVisible();
    await expect(page.locator('#status')).toBeVisible();
  });

  test('CSS should load without requiring special headers', async ({
    page,
  }) => {
    await page.goto('/');

    // Check that CSS is being applied
    const heroStyles = await page.evaluate(() => {
      const hero = document.querySelector('.hero');
      if (!hero) return null;
      const styles = window.getComputedStyle(hero);
      return {
        display: styles.display,
        padding: styles.padding,
      };
    });

    expect(heroStyles).not.toBeNull();
    expect(heroStyles.padding).not.toBe('0px');
  });
});
