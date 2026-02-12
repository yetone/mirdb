/**
 * Performance E2E Tests
 * Owner: Scenario 8 - Performance and Load Time
 *
 * Test cases:
 * - Page loads within 3000ms (NFR-1)
 * - Total page size (minus GIFs) under 500KB
 * - No render-blocking resources
 * - CSS file size is reasonable
 */

const { test, expect } = require('@playwright/test');

test.describe('Performance and Load Time', () => {
  test('TC1: Page should load within 3000ms (DOMContentLoaded)', async ({ page }) => {
    const startTime = Date.now();

    await page.goto('/', { waitUntil: 'domcontentloaded' });

    const loadTime = Date.now() - startTime;

    // Also measure using Performance API for more accurate timing
    const perfTiming = await page.evaluate(() => {
      const navigation = performance.getEntriesByType('navigation')[0];
      if (navigation) {
        return navigation.domContentLoadedEventEnd - navigation.startTime;
      }
      return null;
    });

    // Use Performance API timing if available, otherwise use our measurement
    const actualLoadTime = perfTiming !== null ? perfTiming : loadTime;

    expect(actualLoadTime).toBeLessThan(3000);
  });

  test('TC2: Total page size (excluding GIF assets) should be under 500KB', async ({ page }) => {
    const resourceSizes = [];

    // Intercept network requests to measure sizes
    page.on('response', async (response) => {
      const url = response.url();
      const contentLength = response.headers()['content-length'];

      // Skip GIF files
      if (url.endsWith('.gif')) {
        return;
      }

      if (contentLength) {
        resourceSizes.push({
          url,
          size: parseInt(contentLength, 10)
        });
      } else {
        // For responses without content-length, try to get body size
        try {
          const body = await response.body();
          resourceSizes.push({
            url,
            size: body.length
          });
        } catch (e) {
          // Some responses may not have accessible body
        }
      }
    });

    await page.goto('/', { waitUntil: 'networkidle' });

    // Calculate total size
    const totalSize = resourceSizes.reduce((sum, r) => sum + r.size, 0);
    const totalSizeKB = totalSize / 1024;

    expect(totalSizeKB).toBeLessThan(500);
  });

  test('TC3: No large render-blocking JavaScript files', async ({ page }) => {
    const scripts = [];

    // Intercept script requests
    page.on('response', async (response) => {
      const url = response.url();
      if (url.endsWith('.js')) {
        const contentLength = response.headers()['content-length'];
        let size = 0;

        if (contentLength) {
          size = parseInt(contentLength, 10);
        } else {
          try {
            const body = await response.body();
            size = body.length;
          } catch (e) {
            // Skip if body not accessible
          }
        }

        scripts.push({ url, size });
      }
    });

    await page.goto('/', { waitUntil: 'networkidle' });

    // Check the HTML for render-blocking scripts (scripts in head without defer/async)
    const renderBlockingScripts = await page.evaluate(() => {
      const headScripts = document.querySelectorAll('head script[src]:not([defer]):not([async])');
      return Array.from(headScripts).map(s => ({
        src: s.src,
        hasDefer: s.hasAttribute('defer'),
        hasAsync: s.hasAttribute('async')
      }));
    });

    // No large render-blocking scripts should exist (threshold: 50KB)
    for (const script of scripts) {
      const isRenderBlocking = renderBlockingScripts.some(rs => script.url.includes(new URL(rs.src).pathname));
      if (isRenderBlocking) {
        expect(script.size).toBeLessThan(50 * 1024); // 50KB threshold for blocking scripts
      }
    }

    // For a static site, we expect minimal or no render-blocking scripts
    // The main.js is loaded at the end of body, so it's not render-blocking
    expect(renderBlockingScripts.length).toBe(0);
  });

  test('TC4: CSS file size should be under 50KB unminified', async ({ page }) => {
    const cssFiles = [];

    // Intercept CSS requests
    page.on('response', async (response) => {
      const url = response.url();
      if (url.endsWith('.css')) {
        const contentLength = response.headers()['content-length'];
        let size = 0;

        if (contentLength) {
          size = parseInt(contentLength, 10);
        } else {
          try {
            const body = await response.body();
            size = body.length;
          } catch (e) {
            // Skip if body not accessible
          }
        }

        cssFiles.push({ url, size });
      }
    });

    await page.goto('/', { waitUntil: 'networkidle' });

    // All CSS files should be under 50KB
    for (const css of cssFiles) {
      const sizeKB = css.size / 1024;
      expect(sizeKB).toBeLessThan(50);
    }

    // Verify at least one CSS file was loaded (styles.css)
    expect(cssFiles.length).toBeGreaterThan(0);
  });

  test('Page performance metrics are within acceptable ranges', async ({ page }) => {
    await page.goto('/', { waitUntil: 'load' });

    const metrics = await page.evaluate(() => {
      const navigation = performance.getEntriesByType('navigation')[0];
      const paint = performance.getEntriesByType('paint');

      return {
        domContentLoaded: navigation ? navigation.domContentLoadedEventEnd - navigation.startTime : null,
        loadComplete: navigation ? navigation.loadEventEnd - navigation.startTime : null,
        firstPaint: paint.find(p => p.name === 'first-paint')?.startTime || null,
        firstContentfulPaint: paint.find(p => p.name === 'first-contentful-paint')?.startTime || null,
      };
    });

    // DOM Content Loaded should be under 3 seconds (NFR-1)
    if (metrics.domContentLoaded !== null) {
      expect(metrics.domContentLoaded).toBeLessThan(3000);
    }

    // First Contentful Paint should be reasonable (under 2 seconds for a static site)
    if (metrics.firstContentfulPaint !== null) {
      expect(metrics.firstContentfulPaint).toBeLessThan(2000);
    }
  });

  test('JavaScript does not block initial render', async ({ page }) => {
    // Check that the script tag is placed at the end of body or has defer/async
    await page.goto('/');

    const scriptPlacement = await page.evaluate(() => {
      const bodyScripts = document.querySelectorAll('body script[src]');
      const headScripts = document.querySelectorAll('head script[src]');

      const bodyScriptCount = bodyScripts.length;
      const headScriptCount = headScripts.length;

      // Check if head scripts have defer or async
      const headScriptsWithoutDeferAsync = Array.from(headScripts).filter(
        s => !s.hasAttribute('defer') && !s.hasAttribute('async')
      );

      return {
        bodyScriptCount,
        headScriptCount,
        renderBlockingHeadScripts: headScriptsWithoutDeferAsync.length
      };
    });

    // For optimal performance, scripts should either:
    // 1. Be at the end of body, OR
    // 2. Have defer/async attribute if in head
    expect(scriptPlacement.renderBlockingHeadScripts).toBe(0);
  });
});
