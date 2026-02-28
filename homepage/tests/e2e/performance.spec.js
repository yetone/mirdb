/**
 * Performance E2E Tests
 * Owner: Scenario 12 - Page Load Performance
 *
 * Tests:
 * - DOMContentLoaded < 1500ms (on simulated 4G)
 * - Full page load < 2000ms (on simulated 4G)
 * - FCP < 1000ms (product name visible quickly)
 * - Total page size < 500KB
 */

import { test, expect } from '@playwright/test';

// 4G network conditions: ~9 Mbps download, ~3.4 Mbps upload, 100ms latency
const SIMULATED_4G = {
  downloadThroughput: (9 * 1024 * 1024) / 8, // 9 Mbps in bytes/second
  uploadThroughput: (3.4 * 1024 * 1024) / 8, // 3.4 Mbps in bytes/second
  latency: 100, // 100ms RTT
};

test.describe('Page Load Performance', () => {
  test.describe.configure({ mode: 'serial' });

  test('DOMContentLoaded fires within 1500ms on simulated 4G connection', async ({ page, context }) => {
    // Enable CDP session for network emulation
    const cdpSession = await context.newCDPSession(page);

    // Enable network emulation with 4G conditions
    await cdpSession.send('Network.emulateNetworkConditions', {
      offline: false,
      downloadThroughput: SIMULATED_4G.downloadThroughput,
      uploadThroughput: SIMULATED_4G.uploadThroughput,
      latency: SIMULATED_4G.latency,
    });

    let domContentLoadedTime = 0;
    const navigationStart = Date.now();

    // Listen for DOMContentLoaded event
    page.on('domcontentloaded', () => {
      domContentLoadedTime = Date.now() - navigationStart;
    });

    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // If event already fired, calculate from performance API
    if (domContentLoadedTime === 0) {
      const timing = await page.evaluate(() => {
        const nav = performance.getEntriesByType('navigation')[0];
        return nav ? nav.domContentLoadedEventEnd - nav.startTime : null;
      });
      domContentLoadedTime = timing || 0;
    }

    console.log(`DOMContentLoaded time: ${domContentLoadedTime}ms`);

    // Allow some variance but ensure it's under 1500ms
    expect(domContentLoadedTime).toBeLessThan(1500);
  });

  test('Full page load completes within 2000ms on simulated 4G connection', async ({ page, context }) => {
    // Enable CDP session for network emulation
    const cdpSession = await context.newCDPSession(page);

    // Enable network emulation with 4G conditions
    await cdpSession.send('Network.emulateNetworkConditions', {
      offline: false,
      downloadThroughput: SIMULATED_4G.downloadThroughput,
      uploadThroughput: SIMULATED_4G.uploadThroughput,
      latency: SIMULATED_4G.latency,
    });

    await page.goto('/');
    await page.waitForLoadState('load');

    // Get the full page load time from Navigation Timing API
    const loadTime = await page.evaluate(() => {
      const nav = performance.getEntriesByType('navigation')[0];
      return nav ? nav.loadEventEnd - nav.startTime : null;
    });

    console.log(`Full page load time: ${loadTime}ms`);

    expect(loadTime).toBeLessThan(2000);
  });

  test('First Contentful Paint occurs within 1000ms (product name visible within 1 second)', async ({ page, context }) => {
    // Enable CDP session for network emulation and performance metrics
    const cdpSession = await context.newCDPSession(page);

    // Enable network emulation with 4G conditions
    await cdpSession.send('Network.emulateNetworkConditions', {
      offline: false,
      downloadThroughput: SIMULATED_4G.downloadThroughput,
      uploadThroughput: SIMULATED_4G.uploadThroughput,
      latency: SIMULATED_4G.latency,
    });

    // Enable Performance domain for FCP metrics
    await cdpSession.send('Performance.enable');

    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Wait a moment for FCP to be recorded
    await page.waitForTimeout(100);

    // Get FCP from PerformanceObserver / paint entries
    const fcp = await page.evaluate(() => {
      const paintEntries = performance.getEntriesByType('paint');
      const fcpEntry = paintEntries.find(entry => entry.name === 'first-contentful-paint');
      return fcpEntry ? fcpEntry.startTime : null;
    });

    console.log(`First Contentful Paint: ${fcp}ms`);

    // Verify FCP is under 1000ms
    expect(fcp).not.toBeNull();
    expect(fcp).toBeLessThan(1000);

    // Also verify the product name (MirDB) is visible
    const heroTitle = page.locator('#hero-title');
    await expect(heroTitle).toBeVisible();
    await expect(heroTitle).toHaveText('MirDB');
  });

  test('Total page size is under 500KB uncompressed for fast loading', async ({ page }) => {
    let totalBytes = 0;
    const resourceSizes = [];

    // Listen to all network responses and sum up their sizes
    page.on('response', async (response) => {
      try {
        const headers = response.headers();
        // Get content-length if available, or get body size
        let size = parseInt(headers['content-length'] || '0', 10);

        if (size === 0) {
          try {
            const body = await response.body();
            size = body.length;
          } catch {
            // Response body might not be available for some resources
            size = 0;
          }
        }

        resourceSizes.push({
          url: response.url(),
          size: size,
        });
        totalBytes += size;
      } catch {
        // Ignore errors from responses that can't be read
      }
    });

    await page.goto('/');
    await page.waitForLoadState('load');

    // Wait a moment for all responses to be processed
    await page.waitForTimeout(500);

    console.log('Resource sizes:');
    resourceSizes.forEach(r => {
      console.log(`  ${r.url}: ${(r.size / 1024).toFixed(2)} KB`);
    });
    console.log(`Total page size: ${(totalBytes / 1024).toFixed(2)} KB`);

    // Total page size should be under 500KB
    expect(totalBytes).toBeLessThan(500 * 1024);
  });
});
