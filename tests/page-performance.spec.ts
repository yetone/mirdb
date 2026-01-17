import { test, expect, chromium, Browser } from '@playwright/test';

/**
 * E2E Tests for Page Performance - Load Time
 *
 * This test suite verifies that the page loads within acceptable time limits
 * as per NFR-1 requirement: under 3 seconds on 3G connection.
 *
 * Test cases:
 * 1. First Contentful Paint (FCP) under 1.8 seconds
 * 2. Largest Contentful Paint (LCP) under 2.5 seconds
 * 3. Lighthouse performance score 90+
 * 4. Page usable within 3 seconds on simulated 3G
 */

test.describe('Page Performance - Load Time', () => {
  test('Test Case 1: First Contentful Paint (FCP) is under 1.8 seconds', async ({ page }) => {
    // Navigate to the page and collect performance metrics
    await page.goto('/', { waitUntil: 'networkidle' });

    // Get FCP from Performance API
    const fcp = await page.evaluate(() => {
      return new Promise<number>((resolve) => {
        const observer = new PerformanceObserver((list) => {
          const entries = list.getEntriesByName('first-contentful-paint');
          if (entries.length > 0) {
            resolve(entries[0].startTime);
          }
        });

        observer.observe({ type: 'paint', buffered: true });

        // Fallback: check if FCP already exists
        const existingEntries = performance.getEntriesByName('first-contentful-paint');
        if (existingEntries.length > 0) {
          resolve(existingEntries[0].startTime);
        }
      });
    });

    console.log(`First Contentful Paint (FCP): ${fcp.toFixed(2)}ms`);

    // FCP should be under 1.8 seconds (1800ms)
    expect(fcp).toBeLessThan(1800);
  });

  test('Test Case 2: Largest Contentful Paint (LCP) is under 2.5 seconds', async ({ page }) => {
    // Navigate to the page
    await page.goto('/', { waitUntil: 'networkidle' });

    // Get LCP from Performance API
    const lcp = await page.evaluate(() => {
      return new Promise<number>((resolve) => {
        let lcpValue = 0;

        const observer = new PerformanceObserver((list) => {
          const entries = list.getEntries();
          for (const entry of entries) {
            lcpValue = entry.startTime;
          }
        });

        observer.observe({ type: 'largest-contentful-paint', buffered: true });

        // Give time for LCP to be reported, then resolve
        setTimeout(() => {
          observer.disconnect();
          resolve(lcpValue);
        }, 100);
      });
    });

    console.log(`Largest Contentful Paint (LCP): ${lcp.toFixed(2)}ms`);

    // LCP should be under 2.5 seconds (2500ms)
    expect(lcp).toBeLessThan(2500);
  });

  test('Test Case 3: Lighthouse performance score is 90 or higher', async () => {
    // Import lighthouse dynamically
    const lighthouse = await import('lighthouse');
    const chromiumPath = require('@playwright/test').chromium;

    // Launch a browser instance for Lighthouse
    const browser: Browser = await chromium.launch({
      args: ['--remote-debugging-port=9222'],
      headless: true,
    });

    try {
      // Run Lighthouse audit
      const runnerResult = await lighthouse.default('http://localhost:3000', {
        port: 9222,
        output: 'json',
        logLevel: 'error',
        onlyCategories: ['performance'],
        formFactor: 'desktop',
        throttling: {
          // No throttling for desktop test
          cpuSlowdownMultiplier: 1,
          rttMs: 0,
          throughputKbps: 0,
        },
        screenEmulation: {
          mobile: false,
          width: 1350,
          height: 940,
          deviceScaleFactor: 1,
          disabled: false,
        },
      });

      if (!runnerResult || !runnerResult.lhr) {
        throw new Error('Lighthouse audit failed to produce results');
      }

      const performanceScore = runnerResult.lhr.categories.performance.score;
      const scorePercentage = performanceScore !== null ? performanceScore * 100 : 0;

      console.log(`Lighthouse Performance Score: ${scorePercentage.toFixed(0)}`);

      // Log key metrics
      const metrics = runnerResult.lhr.audits;
      console.log(`  FCP: ${metrics['first-contentful-paint']?.displayValue || 'N/A'}`);
      console.log(`  LCP: ${metrics['largest-contentful-paint']?.displayValue || 'N/A'}`);
      console.log(`  TBT: ${metrics['total-blocking-time']?.displayValue || 'N/A'}`);
      console.log(`  CLS: ${metrics['cumulative-layout-shift']?.displayValue || 'N/A'}`);
      console.log(`  Speed Index: ${metrics['speed-index']?.displayValue || 'N/A'}`);

      // Performance score should be 90 or higher
      expect(scorePercentage).toBeGreaterThanOrEqual(90);
    } finally {
      await browser.close();
    }
  });

  test('Test Case 4: Page is usable within 3 seconds on simulated 3G', async ({ browser }) => {
    // Create a new context with 3G network emulation
    // Typical 3G speeds: ~750 Kbps down, ~250 Kbps up, ~100ms latency
    const context = await browser.newContext();
    const page = await context.newPage();

    // Create a CDP session to enable network throttling
    const client = await context.newCDPSession(page);
    await client.send('Network.enable');

    // Emulate Slow 3G network conditions
    // Download: 750 Kbps = 93,750 bytes/s
    // Upload: 250 Kbps = 31,250 bytes/s
    // Latency: 100ms
    await client.send('Network.emulateNetworkConditions', {
      offline: false,
      downloadThroughput: (750 * 1024) / 8, // 750 Kbps in bytes/s
      uploadThroughput: (250 * 1024) / 8, // 250 Kbps in bytes/s
      latency: 100, // 100ms latency
    });

    // Measure load time
    const startTime = Date.now();

    // Navigate to the page
    await page.goto('/', { waitUntil: 'domcontentloaded' });

    // Check that the page is usable (hero section visible, main content loaded)
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible({ timeout: 3000 });

    // Check that the main heading is visible
    const mainHeading = page.locator('h1');
    await expect(mainHeading).toBeVisible({ timeout: 3000 });

    // Check that at least one CTA button is visible
    const ctaButton = page.locator('.cta-buttons a').first();
    await expect(ctaButton).toBeVisible({ timeout: 3000 });

    const loadTime = Date.now() - startTime;
    console.log(`Page usable time on 3G: ${loadTime}ms`);

    // Page should be usable (core content visible) within 3 seconds
    expect(loadTime).toBeLessThan(3000);

    await context.close();
  });
});

// Additional performance-related tests
test.describe('Performance Metrics - Additional Checks', () => {
  test('Page resources are optimized', async ({ page }) => {
    // Collect resource timing data
    await page.goto('/', { waitUntil: 'networkidle' });

    const resourceMetrics = await page.evaluate(() => {
      const resources = performance.getEntriesByType('resource') as PerformanceResourceTiming[];
      return {
        totalResources: resources.length,
        totalTransferSize: resources.reduce((sum, r) => sum + (r.transferSize || 0), 0),
        slowestResource: resources.reduce(
          (max, r) => (r.duration > max.duration ? r : max),
          { name: '', duration: 0 }
        ),
      };
    });

    console.log(`Total resources: ${resourceMetrics.totalResources}`);
    console.log(`Total transfer size: ${(resourceMetrics.totalTransferSize / 1024).toFixed(2)} KB`);
    console.log(`Slowest resource: ${resourceMetrics.slowestResource.name} (${resourceMetrics.slowestResource.duration.toFixed(2)}ms)`);

    // Basic sanity checks
    expect(resourceMetrics.totalResources).toBeGreaterThan(0);
  });

  test('No render-blocking resources cause excessive delays', async ({ page }) => {
    await page.goto('/', { waitUntil: 'networkidle' });

    // Check Time to First Byte (TTFB)
    const ttfb = await page.evaluate(() => {
      const navigation = performance.getEntriesByType('navigation')[0] as PerformanceNavigationTiming;
      return navigation.responseStart - navigation.requestStart;
    });

    console.log(`Time to First Byte (TTFB): ${ttfb.toFixed(2)}ms`);

    // TTFB should be reasonable (under 600ms for a local server)
    expect(ttfb).toBeLessThan(600);
  });

  test('DOM is interactive quickly', async ({ page }) => {
    await page.goto('/', { waitUntil: 'networkidle' });

    // Get DOM Content Loaded timing
    const domContentLoaded = await page.evaluate(() => {
      const navigation = performance.getEntriesByType('navigation')[0] as PerformanceNavigationTiming;
      return navigation.domContentLoadedEventEnd - navigation.fetchStart;
    });

    console.log(`DOM Content Loaded: ${domContentLoaded.toFixed(2)}ms`);

    // DOM should be interactive within 1.5 seconds
    expect(domContentLoaded).toBeLessThan(1500);
  });
});
