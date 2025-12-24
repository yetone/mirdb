// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for Page Performance (Scenario 11)
 * Verifies that the page loads within acceptable performance thresholds as specified in NFR-2
 */

test.describe('Page Performance', () => {
  /**
   * Test Case 1: Measure page load time
   * Expected: Page fully loads in under 3 seconds on broadband
   */
  test('should load page in under 3 seconds', async ({ page }) => {
    // Start timing
    const startTime = Date.now();

    // Navigate to the page and wait for load event
    await page.goto('/', { waitUntil: 'load' });

    // Calculate load time
    const loadTime = Date.now() - startTime;

    // Verify load time is under 3 seconds (3000ms)
    expect(loadTime).toBeLessThan(3000);

    // Also verify the page content is visible
    await expect(page.locator('h1')).toBeVisible();
    await expect(page.locator('#features')).toBeVisible();
  });

  /**
   * Test Case 1 (Alternative): Measure using Performance API
   * Expected: Page fully loads in under 3 seconds on broadband
   */
  test('should have DOM content loaded in under 3 seconds using Performance API', async ({ page }) => {
    // Navigate to page
    await page.goto('/', { waitUntil: 'domcontentloaded' });

    // Get performance metrics using Performance API
    const performanceMetrics = await page.evaluate(() => {
      const timing = performance.timing;
      return {
        domContentLoaded: timing.domContentLoadedEventEnd - timing.navigationStart,
        loadComplete: timing.loadEventEnd - timing.navigationStart,
        domInteractive: timing.domInteractive - timing.navigationStart
      };
    });

    // DOM content should load under 3 seconds
    expect(performanceMetrics.domContentLoaded).toBeLessThan(3000);

    // DOM should be interactive under 3 seconds
    expect(performanceMetrics.domInteractive).toBeLessThan(3000);
  });

  /**
   * Test Case 2: Check total page weight
   * Expected: Total page size is reasonable (under 2MB recommended)
   */
  test('should have total page weight under 2MB', async ({ page }) => {
    let totalBytes = 0;
    const resourceSizes = [];

    // Intercept all network requests and track their sizes
    page.on('response', async (response) => {
      try {
        const url = response.url();
        const headers = response.headers();

        // Get content-length if available, otherwise estimate from body
        let size = 0;
        const contentLength = headers['content-length'];

        if (contentLength) {
          size = parseInt(contentLength, 10);
        } else {
          // For responses without content-length, get the body size
          try {
            const body = await response.body();
            size = body.length;
          } catch (e) {
            // Some resources may not be readable
            size = 0;
          }
        }

        if (size > 0) {
          totalBytes += size;
          resourceSizes.push({ url, size });
        }
      } catch (e) {
        // Ignore errors for redirects or failed resources
      }
    });

    // Navigate to the page and wait for network to be idle
    await page.goto('/', { waitUntil: 'networkidle' });

    // Wait a bit for all responses to be processed
    await page.waitForTimeout(500);

    // 2MB = 2 * 1024 * 1024 = 2097152 bytes
    const maxBytes = 2 * 1024 * 1024;

    // Verify total page weight is under 2MB
    expect(totalBytes).toBeLessThan(maxBytes);
  });

  /**
   * Test Case 2 (Alternative): Check individual asset sizes
   * Expected: No single asset is unreasonably large (>500KB for non-images)
   */
  test('should have no excessively large individual assets', async ({ page }) => {
    const largeAssets = [];
    const maxAssetSize = 500 * 1024; // 500KB for individual assets
    const maxImageSize = 1024 * 1024; // 1MB for images

    page.on('response', async (response) => {
      try {
        const url = response.url();
        const contentType = response.headers()['content-type'] || '';

        let size = 0;
        const contentLength = response.headers()['content-length'];

        if (contentLength) {
          size = parseInt(contentLength, 10);
        } else {
          try {
            const body = await response.body();
            size = body.length;
          } catch (e) {
            size = 0;
          }
        }

        const isImage = contentType.includes('image');
        const threshold = isImage ? maxImageSize : maxAssetSize;

        if (size > threshold) {
          largeAssets.push({ url, size, type: contentType });
        }
      } catch (e) {
        // Ignore errors
      }
    });

    await page.goto('/', { waitUntil: 'networkidle' });
    await page.waitForTimeout(500);

    // There should be no excessively large assets
    expect(largeAssets.length).toBe(0);
  });

  /**
   * Test Case 3: Verify no render-blocking resources
   * Expected: CSS and JS don't significantly block initial render
   */
  test('should not have significant render-blocking resources', async ({ page }) => {
    // Navigate to the page
    await page.goto('/', { waitUntil: 'domcontentloaded' });

    // Check that CSS is loaded inline or with proper attributes
    // External CSS should ideally use media queries or preload for non-critical CSS
    const stylesheets = await page.evaluate(() => {
      const links = document.querySelectorAll('link[rel="stylesheet"]');
      return Array.from(links).map(link => ({
        href: link.getAttribute('href'),
        media: link.getAttribute('media'),
        preload: link.getAttribute('rel') === 'preload'
      }));
    });

    // Check for render-blocking scripts (scripts without async/defer)
    const blockingScripts = await page.evaluate(() => {
      const scripts = document.querySelectorAll('script[src]:not([async]):not([defer]):not([type="module"])');
      return Array.from(scripts).map(script => script.getAttribute('src'));
    });

    // There should be no render-blocking scripts (or very few)
    // Our static page shouldn't need any external JS
    expect(blockingScripts.length).toBeLessThanOrEqual(1);

    // Verify First Contentful Paint is reasonable using Performance API
    const fcp = await page.evaluate(() => {
      return new Promise((resolve) => {
        // Use PerformanceObserver if available
        if (typeof PerformanceObserver !== 'undefined') {
          const observer = new PerformanceObserver((list) => {
            const entries = list.getEntries();
            for (const entry of entries) {
              if (entry.name === 'first-contentful-paint') {
                resolve(entry.startTime);
                observer.disconnect();
                return;
              }
            }
          });
          observer.observe({ type: 'paint', buffered: true });

          // Fallback after 1 second if FCP wasn't captured
          setTimeout(() => resolve(null), 1000);
        } else {
          // Fallback for browsers without PerformanceObserver
          resolve(null);
        }
      });
    });

    // If FCP was captured, it should be under 1.5 seconds (reasonable for a static page)
    if (fcp !== null) {
      expect(fcp).toBeLessThan(1500);
    }
  });

  /**
   * Test Case 3 (Alternative): Check CSS loading doesn't block rendering
   * Expected: Main content is visible quickly
   */
  test('should render main content quickly without CSS blocking', async ({ page }) => {
    // Navigate and wait only for DOM content (not full load)
    const startTime = Date.now();
    await page.goto('/', { waitUntil: 'domcontentloaded' });
    const domReadyTime = Date.now() - startTime;

    // DOM should be ready quickly (under 2 seconds)
    expect(domReadyTime).toBeLessThan(2000);

    // Check that hero content exists in DOM
    const heroExists = await page.locator('[data-testid="hero-section"]').count() > 0 ||
                       await page.locator('.hero').count() > 0;
    expect(heroExists).toBeTruthy();

    // Check that the main heading is in the DOM
    const h1Exists = await page.locator('h1').count() > 0;
    expect(h1Exists).toBeTruthy();
  });

  /**
   * Additional Test: Verify page meets Core Web Vitals style metrics
   */
  test('should meet basic web vitals thresholds', async ({ page }) => {
    // Navigate to page
    await page.goto('/', { waitUntil: 'load' });

    // Wait for any lazy resources
    await page.waitForTimeout(1000);

    // Get Largest Contentful Paint and other metrics
    const metrics = await page.evaluate(() => {
      return new Promise((resolve) => {
        let lcp = null;
        let cls = 0;

        // Observe LCP
        if (typeof PerformanceObserver !== 'undefined') {
          try {
            const lcpObserver = new PerformanceObserver((list) => {
              const entries = list.getEntries();
              if (entries.length > 0) {
                lcp = entries[entries.length - 1].startTime;
              }
            });
            lcpObserver.observe({ type: 'largest-contentful-paint', buffered: true });

            // Observe CLS
            const clsObserver = new PerformanceObserver((list) => {
              for (const entry of list.getEntries()) {
                if (!entry.hadRecentInput) {
                  cls += entry.value;
                }
              }
            });
            clsObserver.observe({ type: 'layout-shift', buffered: true });
          } catch (e) {
            // Some metrics may not be available
          }
        }

        // Wait a bit and return metrics
        setTimeout(() => {
          resolve({ lcp, cls });
        }, 500);
      });
    });

    // LCP should be under 2.5 seconds (good threshold)
    if (metrics.lcp !== null) {
      expect(metrics.lcp).toBeLessThan(2500);
    }

    // CLS should be under 0.1 (good threshold)
    expect(metrics.cls).toBeLessThan(0.1);
  });
});
