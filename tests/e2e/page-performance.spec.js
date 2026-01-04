// @ts-check
const { test, expect } = require('@playwright/test');
const fs = require('fs');
const path = require('path');

/**
 * Page Load Performance Tests - NFR-2
 * Verify page loads within 3 seconds on standard broadband connection
 */

test.describe('Page Load Performance (NFR-2)', () => {
  test('DOMContentLoaded event fires within 2 seconds', async ({ page }) => {
    // Use Performance API to measure DOMContentLoaded timing
    const navigationPromise = page.goto('/');

    // Wait for the page to load and collect timing data
    await navigationPromise;

    // Get DOMContentLoaded timing using Performance API
    const domContentLoadedTime = await page.evaluate(() => {
      const timing = performance.getEntriesByType('navigation')[0];
      if (timing && timing.domContentLoadedEventEnd) {
        return timing.domContentLoadedEventEnd;
      }
      // Fallback to legacy timing API
      const legacyTiming = performance.timing;
      return legacyTiming.domContentLoadedEventEnd - legacyTiming.navigationStart;
    });

    console.log(`DOMContentLoaded time: ${domContentLoadedTime}ms`);

    // DOMContentLoaded should be under 2000ms (2 seconds)
    expect(domContentLoadedTime).toBeLessThan(2000);
  });

  test('Full page load completes within 3 seconds', async ({ page }) => {
    // Navigate to the page and wait for load event
    await page.goto('/', { waitUntil: 'load' });

    // Get full page load timing
    const loadTime = await page.evaluate(() => {
      const timing = performance.getEntriesByType('navigation')[0];
      if (timing && timing.loadEventEnd) {
        return timing.loadEventEnd;
      }
      // Fallback to legacy timing API
      const legacyTiming = performance.timing;
      return legacyTiming.loadEventEnd - legacyTiming.navigationStart;
    });

    console.log(`Full page load time: ${loadTime}ms`);

    // Full page load should be under 3000ms (3 seconds)
    expect(loadTime).toBeLessThan(3000);
  });

  test('Total page size (HTML, CSS, JS, images) is under 500KB', async ({ page }) => {
    // Navigate to the page
    await page.goto('/', { waitUntil: 'networkidle' });

    // Get all resource sizes using Performance API
    const totalSize = await page.evaluate(() => {
      const resources = performance.getEntriesByType('resource');
      let total = 0;

      // Sum up transferred sizes from all resources
      resources.forEach(resource => {
        // Use transferSize (compressed) if available, otherwise use encodedBodySize
        total += resource.transferSize || resource.encodedBodySize || 0;
      });

      // Also add the document (HTML) size
      const navigation = performance.getEntriesByType('navigation')[0];
      if (navigation) {
        total += navigation.transferSize || navigation.encodedBodySize || 0;
      }

      return total;
    });

    // Convert to KB for readability
    const totalSizeKB = totalSize / 1024;
    console.log(`Total page size: ${totalSizeKB.toFixed(2)}KB`);

    // Total page size should be under 500KB
    expect(totalSize).toBeLessThan(500 * 1024);
  });
});
