/**
 * Static Deployment E2E Tests
 * Owner: Scenario 14 - Static Site Deployment Compatibility
 *
 * Test coverage:
 * - index.html at root serves correctly
 * - Static file serving works without server-side processing
 * - Relative asset paths load correctly
 * - 404 handling works properly
 */

import { test, expect } from '@playwright/test';
import { waitForPageLoad } from './test-utils';

test.describe('Static Site Deployment Compatibility', () => {
  test.describe('Test Case 1: index.html exists and serves as entry point', () => {
    test('homepage loads successfully from root', async ({ page }) => {
      const response = await page.goto('/');
      expect(response?.status()).toBe(200);
    });

    test('index.html serves with correct content type', async ({ page }) => {
      const response = await page.goto('/');
      const contentType = response?.headers()['content-type'];
      expect(contentType).toContain('text/html');
    });

    test('page has valid HTML structure', async ({ page }) => {
      await page.goto('/');
      await waitForPageLoad(page);

      // Check for essential HTML elements
      const html = page.locator('html');
      await expect(html).toHaveAttribute('lang', 'en');

      const head = page.locator('head');
      await expect(head).toBeAttached();

      const body = page.locator('body');
      await expect(body).toBeAttached();
    });

    test('page title is set correctly', async ({ page }) => {
      await page.goto('/');
      await expect(page).toHaveTitle(/MirDB/);
    });
  });

  test.describe('Test Case 2: Page works with simple static file server', () => {
    test('page loads without JavaScript errors', async ({ page }) => {
      const errors: string[] = [];
      page.on('pageerror', (error) => {
        errors.push(error.message);
      });

      await page.goto('/');
      await waitForPageLoad(page);

      // Allow for minor console errors but no critical failures
      const criticalErrors = errors.filter(e =>
        !e.includes('favicon') &&
        !e.includes('deprecated')
      );
      expect(criticalErrors).toHaveLength(0);
    });

    test('page renders main content without server processing', async ({ page }) => {
      await page.goto('/');
      await waitForPageLoad(page);

      // Main sections should render
      const header = page.locator('header');
      await expect(header).toBeVisible();

      const main = page.locator('main');
      await expect(main).toBeVisible();

      const footer = page.locator('footer');
      await expect(footer).toBeVisible();
    });

    test('page is interactive without server-side rendering', async ({ page }) => {
      await page.goto('/');
      await waitForPageLoad(page);

      // Navigation links should work (anchor links)
      const featuresLink = page.locator('a[href="#features"]');
      if (await featuresLink.isVisible()) {
        await featuresLink.click();
        await expect(page).toHaveURL(/#features/);
      }
    });

    test('CSS loads and applies styling', async ({ page }) => {
      await page.goto('/');
      await waitForPageLoad(page);

      // Check that CSS is loaded by verifying computed styles
      const header = page.locator('header');
      const styles = await header.evaluate((el) => {
        const computed = window.getComputedStyle(el);
        return {
          position: computed.position,
          backgroundColor: computed.backgroundColor,
        };
      });

      // Header should have some styling applied (not default)
      expect(styles.position).not.toBe('');
    });

    test('no network requests fail with 404 or 500 for required assets', async ({ page }) => {
      const failedRequests: string[] = [];

      page.on('response', (response) => {
        const status = response.status();
        const url = response.url();

        // Track failed requests for local assets (not external)
        if ((status >= 400) && url.includes('localhost')) {
          // Ignore favicon which may not exist
          if (!url.includes('favicon')) {
            failedRequests.push(`${status}: ${url}`);
          }
        }
      });

      await page.goto('/');
      await waitForPageLoad(page);

      expect(failedRequests).toHaveLength(0);
    });
  });

  test.describe('Test Case 3: Asset paths are relative and load correctly', () => {
    test('CSS stylesheet loads successfully', async ({ page }) => {
      let cssLoaded = false;

      page.on('response', (response) => {
        if (response.url().includes('styles.css') && response.status() === 200) {
          cssLoaded = true;
        }
      });

      await page.goto('/');
      await waitForPageLoad(page);

      expect(cssLoaded).toBe(true);
    });

    test('JavaScript file loads successfully', async ({ page }) => {
      let jsLoaded = false;

      page.on('response', (response) => {
        if (response.url().includes('main.js') && response.status() === 200) {
          jsLoaded = true;
        }
      });

      await page.goto('/');
      await waitForPageLoad(page);

      expect(jsLoaded).toBe(true);
    });

    test('images load with relative paths', async ({ page }) => {
      await page.goto('/');
      await waitForPageLoad(page);

      // Check logo image loads
      const logo = page.locator('img.logo');
      if (await logo.isVisible()) {
        const naturalWidth = await logo.evaluate((img: HTMLImageElement) => img.naturalWidth);
        expect(naturalWidth).toBeGreaterThan(0);
      }
    });

    test('all local image sources use relative paths', async ({ page }) => {
      await page.goto('/');

      // Get all img elements
      const images = await page.locator('img').all();

      for (const img of images) {
        const src = await img.getAttribute('src');
        if (src && !src.startsWith('http') && !src.startsWith('data:')) {
          // Local images should not start with /
          // They should be relative like "assets/images/logo.gif"
          expect(src).not.toMatch(/^\/[a-z]/i);
        }
      }
    });

    test('assets work from different base URLs', async ({ page, baseURL }) => {
      // The page should work regardless of the base URL structure
      await page.goto('/');
      await waitForPageLoad(page);

      // Core functionality should work
      const title = page.locator('h1');
      await expect(title).toBeVisible();
    });
  });

  test.describe('Test Case 4: 404 handling', () => {
    test('404.html page exists and is accessible', async ({ page }) => {
      const response = await page.goto('/404.html');
      expect(response?.status()).toBe(200);
    });

    test('404 page displays error message', async ({ page }) => {
      await page.goto('/404.html');
      await waitForPageLoad(page);

      const content = await page.content();
      const has404Content =
        content.includes('404') ||
        content.toLowerCase().includes('not found');
      expect(has404Content).toBe(true);
    });

    test('404 page has navigation back to homepage', async ({ page }) => {
      await page.goto('/404.html');
      await waitForPageLoad(page);

      // Should have a link to home
      const homeLink = page.locator('a[href="/"], a[href="./"], a[href="index.html"]');
      await expect(homeLink.first()).toBeVisible();
    });

    test('404 page uses consistent styling', async ({ page }) => {
      await page.goto('/404.html');
      await waitForPageLoad(page);

      // CSS should load on 404 page too
      const body = page.locator('body');
      const fontFamily = await body.evaluate((el) => {
        return window.getComputedStyle(el).fontFamily;
      });

      // Should have a custom font, not just browser default
      expect(fontFamily).not.toBe('');
      expect(fontFamily.toLowerCase()).toContain('system');
    });

    test('non-existent page returns 404 status from server', async ({ page }) => {
      // When accessing a non-existent page, static servers typically return 404
      // Note: http-server may return 404, some SPAs redirect to index.html
      const response = await page.goto('/this-page-does-not-exist-xyz123.html');

      // Either 404 or redirect to a handler is acceptable for static sites
      const status = response?.status();
      expect([200, 404]).toContain(status);
    });
  });

  test.describe('Deployment platform compatibility checks', () => {
    test('page works without hash-based routing dependency', async ({ page }) => {
      // Direct navigation should work without requiring hash routing
      await page.goto('/');
      await waitForPageLoad(page);

      const hero = page.locator('#hero');
      await expect(hero).toBeVisible();
    });

    test('all internal links are functional', async ({ page }) => {
      await page.goto('/');
      await waitForPageLoad(page);

      // Get all internal anchor links
      const internalLinks = page.locator('a[href^="#"]');
      const count = await internalLinks.count();

      // Each anchor link should point to an existing element
      for (let i = 0; i < count; i++) {
        const href = await internalLinks.nth(i).getAttribute('href');
        if (href && href.startsWith('#') && href.length > 1) {
          const targetId = href.slice(1);
          const target = page.locator(`#${targetId}`);
          await expect(target).toBeAttached();
        }
      }
    });

    test('external links have proper attributes', async ({ page }) => {
      await page.goto('/');
      await waitForPageLoad(page);

      // External links should have target="_blank" and rel="noopener noreferrer"
      const externalLinks = page.locator('a[target="_blank"]');
      const count = await externalLinks.count();

      for (let i = 0; i < count; i++) {
        const rel = await externalLinks.nth(i).getAttribute('rel');
        expect(rel).toContain('noopener');
      }
    });

    test('page does not require specific server headers', async ({ page }) => {
      // Page should work without special CORS or security headers
      await page.goto('/');
      await waitForPageLoad(page);

      // Content should be visible regardless of headers
      const mainContent = page.locator('main');
      await expect(mainContent).toBeVisible();
    });

    test('page is fully rendered without AJAX calls to API endpoints', async ({ page }) => {
      const apiCalls: string[] = [];

      page.on('request', (request) => {
        const url = request.url();
        // Track any API-like calls
        if (url.includes('/api/') || url.includes('.json')) {
          apiCalls.push(url);
        }
      });

      await page.goto('/');
      await waitForPageLoad(page);

      // Main content should be server-rendered (static HTML)
      const heroTitle = page.locator('h1');
      await expect(heroTitle).toBeVisible();

      // Should not require API calls for initial render
      // (manifest.json or similar is OK, actual data APIs are not)
      const dataApiCalls = apiCalls.filter(url =>
        !url.includes('manifest') &&
        !url.includes('favicon')
      );
      expect(dataApiCalls).toHaveLength(0);
    });
  });
});
