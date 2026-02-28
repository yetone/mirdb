/**
 * Performance Integration Tests
 * Owner: Scenario 11 - Performance
 *
 * Test cases:
 * - First Contentful Paint < 1.8s
 * - Time to Interactive < 3s
 * - DOMContentLoaded < 1s
 * - Images use lazy loading where appropriate
 * - Total page weight reasonable
 */

const { test, expect } = require('@playwright/test');

test.describe('Performance - Page Load Time', () => {
  test.describe.configure({ mode: 'serial' });

  test('First Contentful Paint (FCP) is under 1.8 seconds', async ({ page }) => {
    // Navigate to the page with performance metrics collection
    await page.goto('/', { waitUntil: 'networkidle' });

    // Get the FCP metric from Performance API
    const fcpEntry = await page.evaluate(() => {
      return new Promise((resolve) => {
        // Check if performance entries are already available
        const entries = performance.getEntriesByType('paint');
        const fcp = entries.find(entry => entry.name === 'first-contentful-paint');
        if (fcp) {
          resolve(fcp.startTime);
          return;
        }

        // If not available, use PerformanceObserver
        const observer = new PerformanceObserver((list) => {
          const entries = list.getEntries();
          const fcp = entries.find(entry => entry.name === 'first-contentful-paint');
          if (fcp) {
            observer.disconnect();
            resolve(fcp.startTime);
          }
        });
        observer.observe({ type: 'paint', buffered: true });

        // Timeout fallback
        setTimeout(() => resolve(null), 5000);
      });
    });

    expect(fcpEntry).not.toBeNull();
    expect(fcpEntry).toBeLessThan(1800); // 1.8 seconds = 1800ms
  });

  test('Time to Interactive (TTI) is under 3 seconds', async ({ page }) => {
    const startTime = Date.now();

    // Navigate and wait for full page load
    await page.goto('/', { waitUntil: 'networkidle' });

    // Measure time until interactive - page should be fully loaded and interactive
    // We measure by checking when we can interact with elements
    await page.waitForSelector('.hero__cta', { state: 'visible' });
    await page.waitForSelector('.nav-menu', { state: 'visible' });

    // Verify interactivity by ensuring elements are clickable
    const ctaButton = page.locator('.hero__cta');
    await expect(ctaButton).toBeEnabled();

    // Also check that JavaScript is loaded and functional
    const copyButtons = page.locator('.copy-button');
    await expect(copyButtons.first()).toBeEnabled();

    const tti = Date.now() - startTime;

    expect(tti).toBeLessThan(3000); // 3 seconds = 3000ms
  });

  test('DOMContentLoaded fires in under 1 second', async ({ page }) => {
    // Set up listener before navigation
    const domContentLoadedTime = await page.evaluate(async () => {
      return new Promise((resolve) => {
        // Navigate will happen externally, we just need to measure
        const timing = performance.timing;
        if (timing && timing.domContentLoadedEventEnd > 0) {
          // Already loaded, calculate from navigation start
          resolve(timing.domContentLoadedEventEnd - timing.navigationStart);
        } else {
          // Use modern Navigation Timing API
          const navEntry = performance.getEntriesByType('navigation')[0];
          if (navEntry && navEntry.domContentLoadedEventEnd > 0) {
            resolve(navEntry.domContentLoadedEventEnd);
          } else {
            resolve(null);
          }
        }
      });
    });

    // For fresh page loads, we need to navigate and measure
    await page.goto('/', { waitUntil: 'domcontentloaded' });

    const dcl = await page.evaluate(() => {
      const navEntry = performance.getEntriesByType('navigation')[0];
      if (navEntry) {
        return navEntry.domContentLoadedEventEnd;
      }
      // Fallback to timing API
      const timing = performance.timing;
      if (timing) {
        return timing.domContentLoadedEventEnd - timing.navigationStart;
      }
      return null;
    });

    expect(dcl).not.toBeNull();
    expect(dcl).toBeLessThan(1000); // 1 second = 1000ms
  });

  test('Below-fold images have loading="lazy" attribute', async ({ page }) => {
    await page.goto('/', { waitUntil: 'domcontentloaded' });

    // Check usage.gif has lazy loading (it's in the quick start section - below the fold)
    const usageGif = page.locator('img.usage-gif');
    await expect(usageGif).toHaveAttribute('loading', 'lazy');

    // Verify the image has alt text as well
    await expect(usageGif).toHaveAttribute('alt');
    const altText = await usageGif.getAttribute('alt');
    expect(altText.length).toBeGreaterThan(0);

    // The logo.gif is above the fold (hero section), so it should NOT have lazy loading
    // (or if it does, that's fine too - we just verify below-fold images have it)
    const logoGif = page.locator('img.hero__logo');
    const logoLoading = await logoGif.getAttribute('loading');
    // Logo can have eager or no loading attribute (default eager behavior)
    if (logoLoading) {
      // If it has a loading attribute, it should be either eager or lazy
      expect(['lazy', 'eager']).toContain(logoLoading);
    }
  });

  test('Total page weight is under 10MB', async ({ page }) => {
    // Start capturing network requests
    const requests = [];

    page.on('response', async (response) => {
      try {
        const headers = response.headers();
        const contentLength = headers['content-length'];

        if (contentLength) {
          requests.push({
            url: response.url(),
            size: parseInt(contentLength, 10),
          });
        } else {
          // Try to get body size for responses without content-length
          try {
            const body = await response.body();
            requests.push({
              url: response.url(),
              size: body.length,
            });
          } catch (e) {
            // Some responses may not have a body (e.g., redirects)
            requests.push({
              url: response.url(),
              size: 0,
            });
          }
        }
      } catch (e) {
        // Ignore errors for failed responses
      }
    });

    await page.goto('/', { waitUntil: 'networkidle' });

    // Wait a bit more to ensure all assets are loaded
    await page.waitForTimeout(1000);

    // Calculate total page weight
    const totalSize = requests.reduce((sum, req) => sum + req.size, 0);
    const totalSizeMB = totalSize / (1024 * 1024);

    // Log for debugging
    console.log(`Total page weight: ${totalSizeMB.toFixed(2)} MB`);

    // Expected: Total assets under 10MB (accounting for GIFs)
    expect(totalSize).toBeLessThan(10 * 1024 * 1024); // 10MB in bytes
  });
});
