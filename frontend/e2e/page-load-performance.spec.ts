import { test, expect } from '@playwright/test';

/**
 * Page Load Performance Tests for NFR-1
 *
 * Verifies that the homepage loads within 3 seconds on a standard
 * broadband connection, meeting the non-functional requirement.
 *
 * Standard broadband: 10 Mbps download, 1 Mbps upload, 20ms latency
 */

test.describe('Page Load Performance (NFR-1)', () => {
  test('homepage loads and becomes interactive within 3 seconds on broadband', async ({ page, context }) => {
    // Configure network throttling to simulate standard broadband connection
    // Standard broadband: ~10 Mbps download, ~1 Mbps upload, ~20ms latency
    const cdpSession = await context.newCDPSession(page);
    await cdpSession.send('Network.emulateNetworkConditions', {
      offline: false,
      downloadThroughput: (10 * 1024 * 1024) / 8, // 10 Mbps in bytes/s
      uploadThroughput: (1 * 1024 * 1024) / 8,    // 1 Mbps in bytes/s
      latency: 20, // 20ms latency
    });

    // Start performance measurement
    const startTime = Date.now();

    // Navigate to homepage
    await page.goto('/', { waitUntil: 'domcontentloaded' });

    // Wait for the page to be fully interactive
    // Check for key interactive elements
    await expect(page.locator('[data-testid="hero-section"]')).toBeVisible({ timeout: 3000 });
    await expect(page.locator('[data-testid="cta-register"]')).toBeVisible({ timeout: 3000 });
    await expect(page.locator('[data-testid="login-link"]')).toBeVisible({ timeout: 3000 });

    // Measure time to interactive
    const endTime = Date.now();
    const loadTime = endTime - startTime;

    // Verify load time is under 3 seconds
    expect(loadTime).toBeLessThan(3000);

    console.log(`Page load time: ${loadTime}ms`);
  });

  test('homepage DOMContentLoaded fires within 3 seconds', async ({ page, context }) => {
    // Configure network throttling to simulate standard broadband
    const cdpSession = await context.newCDPSession(page);
    await cdpSession.send('Network.emulateNetworkConditions', {
      offline: false,
      downloadThroughput: (10 * 1024 * 1024) / 8,
      uploadThroughput: (1 * 1024 * 1024) / 8,
      latency: 20,
    });

    // Collect performance metrics
    let domContentLoadedTime = 0;

    // Listen for performance timing
    page.on('domcontentloaded', () => {
      domContentLoadedTime = Date.now();
    });

    const navigationStart = Date.now();
    await page.goto('/', { waitUntil: 'domcontentloaded' });

    const domContentLoadedDuration = domContentLoadedTime - navigationStart;

    // DOMContentLoaded should fire within 3 seconds
    expect(domContentLoadedDuration).toBeLessThan(3000);

    console.log(`DOMContentLoaded time: ${domContentLoadedDuration}ms`);
  });

  test('homepage load event fires within 3 seconds', async ({ page, context }) => {
    // Configure network throttling to simulate standard broadband
    const cdpSession = await context.newCDPSession(page);
    await cdpSession.send('Network.emulateNetworkConditions', {
      offline: false,
      downloadThroughput: (10 * 1024 * 1024) / 8,
      uploadThroughput: (1 * 1024 * 1024) / 8,
      latency: 20,
    });

    // Navigate and wait for full load
    const navigationStart = Date.now();
    await page.goto('/', { waitUntil: 'load' });
    const loadDuration = Date.now() - navigationStart;

    // Full page load should complete within 3 seconds
    expect(loadDuration).toBeLessThan(3000);

    console.log(`Full page load time: ${loadDuration}ms`);
  });

  test('Core Web Vitals metrics are within acceptable ranges', async ({ page }) => {
    await page.goto('/');

    // Wait for page to stabilize
    await page.waitForLoadState('networkidle');

    // Collect performance metrics using Performance API
    const metrics = await page.evaluate(() => {
      const navigation = performance.getEntriesByType('navigation')[0] as PerformanceNavigationTiming;
      const paint = performance.getEntriesByType('paint');

      const fcpEntry = paint.find(entry => entry.name === 'first-contentful-paint');

      return {
        domContentLoaded: navigation.domContentLoadedEventEnd - navigation.fetchStart,
        loadEvent: navigation.loadEventEnd - navigation.fetchStart,
        firstContentfulPaint: fcpEntry ? fcpEntry.startTime : null,
        domInteractive: navigation.domInteractive - navigation.fetchStart,
        responseEnd: navigation.responseEnd - navigation.fetchStart,
      };
    });

    console.log('Performance Metrics:', metrics);

    // Verify core metrics are within acceptable ranges for fast page load
    // DOM Interactive (page is interactive) should be under 3 seconds
    expect(metrics.domInteractive).toBeLessThan(3000);

    // DOMContentLoaded (DOM fully parsed, deferred scripts executed)
    expect(metrics.domContentLoaded).toBeLessThan(3000);

    // First Contentful Paint should be under 1.8 seconds (good threshold)
    if (metrics.firstContentfulPaint !== null) {
      expect(metrics.firstContentfulPaint).toBeLessThan(1800);
    }
  });

  test('all critical resources load within 3 seconds', async ({ page, context }) => {
    // Configure network throttling to simulate standard broadband
    const cdpSession = await context.newCDPSession(page);
    await cdpSession.send('Network.emulateNetworkConditions', {
      offline: false,
      downloadThroughput: (10 * 1024 * 1024) / 8,
      uploadThroughput: (1 * 1024 * 1024) / 8,
      latency: 20,
    });

    // Track all network requests
    const resources: { url: string; duration: number }[] = [];

    page.on('response', async (response) => {
      const timing = response.request().timing();
      if (timing) {
        resources.push({
          url: response.url(),
          duration: timing.responseEnd,
        });
      }
    });

    const startTime = Date.now();
    await page.goto('/', { waitUntil: 'networkidle' });
    const totalLoadTime = Date.now() - startTime;

    // All critical resources should load within 3 seconds
    expect(totalLoadTime).toBeLessThan(3000);

    // Verify no individual resource takes too long
    const slowResources = resources.filter(r => r.duration > 2000);
    expect(slowResources.length).toBe(0);

    console.log(`Total resources loaded: ${resources.length}`);
    console.log(`Total load time: ${totalLoadTime}ms`);
  });
});
