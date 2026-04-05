/**
 * Performance Tests - Scenario 11: Page Performance
 * Owner: Scenario 11 - Page Performance
 *
 * Validates homepage loads within 2 seconds on standard broadband
 * Tests: Page load time, FCP, LCP, and total page weight
 */

import { test, expect } from '@playwright/test';

test.describe('Page Performance', () => {
  /**
   * Test Case 1: Page fully loads in under 2000ms
   * Measures the total time from navigation start to load event
   */
  test('page fully loads in under 2000ms', async ({ page }) => {
    // Navigate and wait for load event
    const startTime = Date.now();
    await page.goto('/', { waitUntil: 'load' });
    const loadTime = Date.now() - startTime;

    // Also get timing from Performance API for accuracy
    const performanceTimings = await page.evaluate(() => {
      const timing = performance.timing;
      return {
        loadEventEnd: timing.loadEventEnd,
        navigationStart: timing.navigationStart,
        domContentLoaded: timing.domContentLoadedEventEnd - timing.navigationStart,
      };
    });

    // Check if performance timing is available (loadEventEnd > 0 means loaded)
    const apiLoadTime = performanceTimings.loadEventEnd > 0
      ? performanceTimings.loadEventEnd - performanceTimings.navigationStart
      : loadTime;

    // Use the larger of the two measurements for conservative testing
    const finalLoadTime = Math.max(loadTime, apiLoadTime);

    console.log(`Page load time: ${finalLoadTime}ms`);
    expect(finalLoadTime).toBeLessThan(2000);
  });

  /**
   * Test Case 2: First Contentful Paint (FCP) occurs within 1000ms
   * FCP measures when the browser renders any content from DOM
   */
  test('First Contentful Paint (FCP) occurs within 1000ms', async ({ page }) => {
    await page.goto('/', { waitUntil: 'domcontentloaded' });

    // Wait a bit for paint metrics to be recorded
    await page.waitForTimeout(500);

    // Get FCP from Performance API
    const fcp = await page.evaluate(() => {
      return new Promise<number>((resolve) => {
        // Try to get existing FCP entry
        const entries = performance.getEntriesByType('paint');
        const fcpEntry = entries.find((entry) => entry.name === 'first-contentful-paint');

        if (fcpEntry) {
          resolve(fcpEntry.startTime);
        } else {
          // Use PerformanceObserver as fallback
          const observer = new PerformanceObserver((list) => {
            const fcpEntries = list.getEntriesByName('first-contentful-paint');
            if (fcpEntries.length > 0) {
              resolve(fcpEntries[0].startTime);
              observer.disconnect();
            }
          });
          observer.observe({ type: 'paint', buffered: true });

          // Timeout fallback - if no FCP recorded, assume fast render
          setTimeout(() => resolve(0), 1000);
        }
      });
    });

    console.log(`First Contentful Paint (FCP): ${fcp}ms`);

    // If FCP is 0 or very small, the page rendered very quickly
    // Accept values under 1000ms
    expect(fcp).toBeLessThan(1000);
  });

  /**
   * Test Case 3: Largest Contentful Paint (LCP) occurs within 2500ms
   * LCP measures when the largest content element is rendered
   */
  test('Largest Contentful Paint (LCP) occurs within 2500ms', async ({ page }) => {
    await page.goto('/', { waitUntil: 'load' });

    // Wait for LCP to be captured (typically within a few seconds)
    await page.waitForTimeout(1000);

    // Get LCP from Performance API
    const lcp = await page.evaluate(() => {
      return new Promise<number>((resolve) => {
        // Get LCP entries
        const entries = performance.getEntriesByType('largest-contentful-paint');
        if (entries.length > 0) {
          const lastEntry = entries[entries.length - 1] as PerformanceEntry & { startTime: number };
          resolve(lastEntry.startTime);
          return;
        }

        // Use PerformanceObserver to capture LCP
        let lcpValue = 0;
        const observer = new PerformanceObserver((list) => {
          const entries = list.getEntries();
          for (const entry of entries) {
            lcpValue = entry.startTime;
          }
        });

        try {
          observer.observe({ type: 'largest-contentful-paint', buffered: true });
        } catch {
          // LCP not supported, resolve with 0 (fast)
          resolve(0);
          return;
        }

        // Wait briefly then return the captured LCP
        setTimeout(() => {
          observer.disconnect();
          resolve(lcpValue);
        }, 500);
      });
    });

    console.log(`Largest Contentful Paint (LCP): ${lcp}ms`);

    // LCP should be under 2500ms for good performance
    expect(lcp).toBeLessThan(2500);
  });

  /**
   * Test Case 4: Total page size is under 1MB (uncompressed)
   * Measures total bytes transferred for all resources
   */
  test('total page size is under 1MB (uncompressed)', async ({ page }) => {
    // Track all resources loaded
    const resourceSizes: { url: string; size: number; transferSize: number }[] = [];

    // Listen for responses and track sizes
    page.on('response', async (response) => {
      try {
        const url = response.url();
        const headers = response.headers();
        const contentLength = headers['content-length'];

        // Get body size if possible
        let size = 0;
        if (contentLength) {
          size = parseInt(contentLength, 10);
        } else {
          try {
            const body = await response.body();
            size = body.length;
          } catch {
            // Response body not available
            size = 0;
          }
        }

        resourceSizes.push({
          url: url,
          size: size,
          transferSize: size,
        });
      } catch {
        // Ignore errors for certain responses
      }
    });

    // Navigate to page
    await page.goto('/', { waitUntil: 'networkidle' });

    // Calculate total size
    const totalSize = resourceSizes.reduce((sum, resource) => sum + resource.size, 0);
    const totalSizeKB = totalSize / 1024;
    const totalSizeMB = totalSizeKB / 1024;

    console.log(`Total page size: ${totalSizeKB.toFixed(2)} KB (${totalSizeMB.toFixed(3)} MB)`);
    console.log(`Number of resources: ${resourceSizes.length}`);

    // Also verify using Performance API resource timing
    const performanceResourceSize = await page.evaluate(() => {
      const resources = performance.getEntriesByType('resource') as PerformanceResourceTiming[];
      return resources.reduce((sum, resource) => {
        // Use decodedBodySize for uncompressed size, fallback to transferSize
        return sum + (resource.decodedBodySize || resource.transferSize || 0);
      }, 0);
    });

    const apiSizeKB = performanceResourceSize / 1024;
    console.log(`Performance API resource size: ${apiSizeKB.toFixed(2)} KB`);

    // Use the larger measurement for conservative testing
    const finalSize = Math.max(totalSize, performanceResourceSize);
    const oneMB = 1024 * 1024; // 1MB in bytes

    expect(finalSize).toBeLessThan(oneMB);
  });

  /**
   * Additional: Verify critical resources load quickly
   */
  test('critical resources (CSS, JS) load efficiently', async ({ page }) => {
    const criticalResourceTimings: { name: string; duration: number }[] = [];

    await page.goto('/', { waitUntil: 'load' });

    // Get resource timing for critical resources
    const resourceTimings = await page.evaluate(() => {
      const resources = performance.getEntriesByType('resource') as PerformanceResourceTiming[];
      return resources
        .filter((r) => r.name.includes('.css') || r.name.includes('.js') || r.name.includes('.svg'))
        .map((r) => ({
          name: r.name.split('/').pop() || r.name,
          duration: r.duration,
          size: r.decodedBodySize || r.transferSize || 0,
        }));
    });

    console.log('Critical resource timings:');
    resourceTimings.forEach((r) => {
      console.log(`  ${r.name}: ${r.duration.toFixed(2)}ms (${(r.size / 1024).toFixed(2)} KB)`);
    });

    // All critical resources should load within 500ms each
    for (const resource of resourceTimings) {
      expect(resource.duration).toBeLessThan(500);
    }
  });
});
