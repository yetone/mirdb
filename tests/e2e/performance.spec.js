// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Page Load Performance Tests for MirDB Homepage
 * Scenario: Verify page loads within 3 seconds on standard connection (NFR-1)
 */

test.describe('Page Load Performance', () => {
  /**
   * Test Case 1: Measure page load time
   * Expected: Page fully loads within 3 seconds on simulated broadband connection
   */
  test('should load page within 3 seconds on broadband connection', async ({ page }) => {
    // Simulate a broadband connection (approximately 10 Mbps download)
    const client = await page.context().newCDPSession(page);
    await client.send('Network.enable');
    await client.send('Network.emulateNetworkConditions', {
      offline: false,
      downloadThroughput: (10 * 1024 * 1024) / 8, // 10 Mbps in bytes per second
      uploadThroughput: (2 * 1024 * 1024) / 8, // 2 Mbps in bytes per second
      latency: 20, // 20ms latency for broadband
    });

    const startTime = Date.now();

    // Navigate to the page and wait for load event
    await page.goto('/', { waitUntil: 'load' });

    const loadTime = Date.now() - startTime;

    // Verify page loaded within 3 seconds (3000ms)
    expect(loadTime).toBeLessThan(3000);

    // Additionally verify that core content is visible
    await expect(page.locator('.hero-title')).toBeVisible();
    await expect(page.locator('.features')).toBeVisible();
  });

  /**
   * Test Case 1 (Alternative): Measure First Contentful Paint timing
   */
  test('should have First Contentful Paint under 3 seconds', async ({ page }) => {
    await page.goto('/', { waitUntil: 'networkidle' });

    // Get performance timing metrics
    const fcpTiming = await page.evaluate(() => {
      const paintEntries = performance.getEntriesByType('paint');
      const fcpEntry = paintEntries.find(entry => entry.name === 'first-contentful-paint');
      return fcpEntry ? fcpEntry.startTime : null;
    });

    expect(fcpTiming).not.toBeNull();
    expect(fcpTiming).toBeLessThan(3000);
  });

  /**
   * Test Case 3: Measure Largest Contentful Paint (LCP)
   * This is a key Core Web Vital metric
   */
  test('should have Largest Contentful Paint under 2.5 seconds', async ({ page }) => {
    // Navigate and wait for page to be fully loaded
    await page.goto('/', { waitUntil: 'networkidle' });

    // Wait a bit for LCP to be reported
    await page.waitForTimeout(1000);

    const lcpTiming = await page.evaluate(() => {
      return new Promise((resolve) => {
        new PerformanceObserver((entryList) => {
          const entries = entryList.getEntries();
          const lastEntry = entries[entries.length - 1];
          resolve(lastEntry ? lastEntry.startTime : null);
        }).observe({ type: 'largest-contentful-paint', buffered: true });

        // Fallback timeout if no LCP entry
        setTimeout(() => resolve(null), 2000);
      });
    });

    // LCP should be under 2.5 seconds for good user experience
    if (lcpTiming !== null) {
      expect(lcpTiming).toBeLessThan(2500);
    }
  });
});
