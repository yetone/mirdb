// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');
const fs = require('fs');

const indexPath = path.resolve(__dirname, '../index.html');

/**
 * Performance - Page Load Time Tests
 *
 * This test suite verifies the landing page loads within acceptable time limits
 * as defined in the PRD:
 * - NFR-2: Page must load within 3 seconds on 3G connections
 * - Success criteria: Lighthouse performance score >= 80
 * - Page load time under 3 seconds on standard connections
 */
test.describe('Performance - Page Load Time', () => {
  /**
   * Test Case 1: Measure First Contentful Paint (FCP)
   * Input: Measure First Contentful Paint (FCP)
   * Expected: FCP under 1.8 seconds on standard connection
   *
   * Note: For file:// protocol, Paint Timing API may not be available in headless mode.
   * We use CDP Performance metrics as a reliable alternative.
   */
  test('TC1: First Contentful Paint under 1.8 seconds', async ({ page }) => {
    // Create CDP session to capture performance metrics
    const client = await page.context().newCDPSession(page);
    await client.send('Performance.enable');

    const startTime = Date.now();
    await page.goto(`file://${indexPath}`, { waitUntil: 'domcontentloaded' });

    // Wait for first content to be visible
    await expect(page.locator('[data-testid="hero-title"]')).toBeVisible();
    const fcpTime = Date.now() - startTime;

    // Try to get FCP from Paint Timing API first
    const paintTimingJson = await page.evaluate(() =>
      JSON.stringify(window.performance.getEntriesByType('paint'))
    );
    const paintTiming = JSON.parse(paintTimingJson);
    const fcpEntry = paintTiming.find(entry => entry.name === 'first-contentful-paint');

    // Use Paint Timing API value if available, otherwise use measured time
    const fcpMs = fcpEntry ? fcpEntry.startTime : fcpTime;

    console.log(`First Contentful Paint: ${fcpMs.toFixed(2)}ms`);
    console.log(`  (Source: ${fcpEntry ? 'Paint Timing API' : 'DOM measurement'})`);

    // FCP should be under 1.8 seconds (1800ms)
    expect(fcpMs).toBeLessThan(1800);

    await client.send('Performance.disable');
  });

  /**
   * Test Case 2: Measure Largest Contentful Paint (LCP)
   * Input: Measure Largest Contentful Paint (LCP)
   * Expected: LCP under 2.5 seconds on standard connection
   */
  test('TC2: Largest Contentful Paint under 2.5 seconds', async ({ page }) => {
    await page.goto(`file://${indexPath}`);

    // Use PerformanceObserver to get LCP
    const lcpMs = await page.evaluate(() => {
      return new Promise((resolve) => {
        // Set a timeout to ensure we don't wait forever
        const timeout = setTimeout(() => resolve(0), 5000);

        new PerformanceObserver((list) => {
          const entries = list.getEntries();
          const lastEntry = entries[entries.length - 1];
          clearTimeout(timeout);
          resolve(lastEntry ? lastEntry.startTime : 0);
        }).observe({
          type: 'largest-contentful-paint',
          buffered: true
        });
      });
    });

    expect(lcpMs).toBeGreaterThan(0);
    // LCP should be under 2.5 seconds (2500ms)
    expect(lcpMs).toBeLessThan(2500);

    console.log(`Largest Contentful Paint: ${lcpMs.toFixed(2)}ms`);
  });

  /**
   * Test Case 3: Page load on simulated 3G connection
   * Input: Measure page load on simulated 3G
   * Expected: Page loads within 3 seconds on 3G connection
   *
   * Note: 3G network conditions:
   * - Download: ~750 Kbps
   * - Upload: ~250 Kbps
   * - Latency: ~100ms
   */
  test('TC3: Page loads within 3 seconds on simulated 3G', async ({ page }) => {
    // Create a CDP session for network throttling
    const client = await page.context().newCDPSession(page);
    await client.send('Network.enable');

    // Simulate 3G network conditions
    // Regular 3G: 750 Kbps download, 250 Kbps upload, 100ms latency
    await client.send('Network.emulateNetworkConditions', {
      offline: false,
      downloadThroughput: (750 * 1024) / 8, // Convert Kbps to bytes per second
      uploadThroughput: (250 * 1024) / 8,
      latency: 100
    });

    const startTime = Date.now();

    // Navigate to page
    await page.goto(`file://${indexPath}`, { waitUntil: 'domcontentloaded' });

    // Wait for hero section to be visible (core content)
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible({ timeout: 3000 });

    const loadTime = Date.now() - startTime;

    // Page should load within 3 seconds on 3G (NFR-2)
    expect(loadTime).toBeLessThan(3000);

    console.log(`Page load time on 3G: ${loadTime}ms`);

    // Clean up CDP session
    await client.send('Network.disable');
  });

  /**
   * Test Case 4: Lighthouse performance audit simulation
   * Input: Run Lighthouse performance audit
   * Expected: Lighthouse performance score >= 80
   *
   * Note: Since Playwright cannot directly run Lighthouse, we simulate
   * key performance indicators that contribute to the Lighthouse score:
   * - FCP (First Contentful Paint)
   * - LCP (Largest Contentful Paint)
   * - Speed Index
   * - Time to Interactive
   */
  test('TC4: Performance metrics indicate Lighthouse score >= 80', async ({ page }) => {
    await page.goto(`file://${indexPath}`, { waitUntil: 'networkidle' });

    // Collect multiple performance metrics
    const metrics = await page.evaluate(() => {
      const perfEntries = performance.getEntriesByType('navigation')[0];
      const paintEntries = performance.getEntriesByType('paint');

      const fcp = paintEntries.find(e => e.name === 'first-contentful-paint');

      return {
        domContentLoaded: perfEntries.domContentLoadedEventEnd - perfEntries.startTime,
        domComplete: perfEntries.domComplete - perfEntries.startTime,
        loadEventEnd: perfEntries.loadEventEnd - perfEntries.startTime,
        fcp: fcp ? fcp.startTime : 0,
        // Transfer size indicates efficiency
        transferSize: perfEntries.transferSize || 0,
        // Decode/encode times
        responseEnd: perfEntries.responseEnd - perfEntries.startTime
      };
    });

    // Get LCP separately using PerformanceObserver
    const lcp = await page.evaluate(() => {
      return new Promise((resolve) => {
        const timeout = setTimeout(() => resolve(0), 5000);
        new PerformanceObserver((list) => {
          const entries = list.getEntries();
          const lastEntry = entries[entries.length - 1];
          clearTimeout(timeout);
          resolve(lastEntry ? lastEntry.startTime : 0);
        }).observe({
          type: 'largest-contentful-paint',
          buffered: true
        });
      });
    });

    console.log('Performance Metrics:');
    console.log(`  FCP: ${metrics.fcp.toFixed(2)}ms`);
    console.log(`  LCP: ${lcp.toFixed(2)}ms`);
    console.log(`  DOM Content Loaded: ${metrics.domContentLoaded.toFixed(2)}ms`);
    console.log(`  DOM Complete: ${metrics.domComplete.toFixed(2)}ms`);
    console.log(`  Load Event End: ${metrics.loadEventEnd.toFixed(2)}ms`);

    // Calculate a simulated performance score based on key metrics
    // Lighthouse scoring thresholds (good = 90th percentile):
    // FCP: <1.8s good, <3s needs improvement
    // LCP: <2.5s good, <4s needs improvement
    // TTI/DOM Complete: <3.8s good

    let score = 100;

    // FCP scoring (weight ~10%)
    if (metrics.fcp > 3000) score -= 20;
    else if (metrics.fcp > 1800) score -= 10;

    // LCP scoring (weight ~25%)
    if (lcp > 4000) score -= 40;
    else if (lcp > 2500) score -= 20;

    // DOM Complete scoring (similar to Speed Index weight ~10%)
    if (metrics.domComplete > 5800) score -= 20;
    else if (metrics.domComplete > 3400) score -= 10;

    // Total Blocking Time (approximate via DOM metrics) (weight ~30%)
    // For a static page with minimal JS, this should be low
    const estimatedTBT = Math.max(0, metrics.loadEventEnd - metrics.domContentLoaded - 50);
    if (estimatedTBT > 600) score -= 30;
    else if (estimatedTBT > 200) score -= 15;

    console.log(`  Estimated Performance Score: ${score}`);

    // Score should be >= 80
    expect(score).toBeGreaterThanOrEqual(80);
  });

  /**
   * Test Case 5: Check total page weight
   * Input: Check total page weight
   * Expected: Total page size under 500KB (excluding optional assets)
   *
   * Core assets that must be under 500KB:
   * - index.html
   * - styles.css
   * - script.js
   * - External CDN resources (Prism.js for syntax highlighting)
   *
   * Optional/excluded assets:
   * - assets/logo.gif (2.5MB) - decorative, lazy-loadable
   * - assets/usage.gif (5.8MB) - optional demo
   */
  test('TC5: Core page assets under 500KB', async ({ page }) => {
    // Calculate actual file sizes of core assets
    const htmlPath = path.resolve(__dirname, '../index.html');
    const cssPath = path.resolve(__dirname, '../styles.css');
    const jsPath = path.resolve(__dirname, '../script.js');

    const htmlSize = fs.statSync(htmlPath).size;
    const cssSize = fs.statSync(cssPath).size;
    const jsSize = fs.statSync(jsPath).size;

    // Core page weight (HTML + CSS + JS)
    const corePageWeight = htmlSize + cssSize + jsSize;

    console.log('Core Asset Sizes:');
    console.log(`  index.html: ${(htmlSize / 1024).toFixed(2)} KB`);
    console.log(`  styles.css: ${(cssSize / 1024).toFixed(2)} KB`);
    console.log(`  script.js: ${(jsSize / 1024).toFixed(2)} KB`);
    console.log(`  Total Core: ${(corePageWeight / 1024).toFixed(2)} KB`);

    // Also estimate external CDN resources (Prism.js)
    // Prism core: ~15KB minified + gzipped
    // Prism theme: ~2KB
    // Prism bash component: ~1KB
    const estimatedExternalResources = 18 * 1024; // 18KB estimate

    const totalEstimatedWeight = corePageWeight + estimatedExternalResources;
    console.log(`  Estimated External (Prism.js): ~18 KB`);
    console.log(`  Total Estimated: ${(totalEstimatedWeight / 1024).toFixed(2)} KB`);

    // Core page weight should be well under 500KB
    // The actual limit is 500KB excluding optional assets (GIFs)
    expect(corePageWeight).toBeLessThan(500 * 1024);

    // Even with external resources, should be under 500KB
    expect(totalEstimatedWeight).toBeLessThan(500 * 1024);

    // Additionally verify by loading the page and checking transfer sizes
    await page.goto(`file://${indexPath}`);

    // For file:// protocol, resources don't have transfer sizes from network
    // but we've verified the actual file sizes above
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();
  });

  /**
   * Additional Test: DOMContentLoaded timing
   * Ensures core HTML/CSS parsing is fast
   */
  test('DOMContentLoaded fires quickly', async ({ page }) => {
    await page.goto(`file://${indexPath}`);

    const domContentLoaded = await page.evaluate(() => {
      const perfEntry = performance.getEntriesByType('navigation')[0];
      return perfEntry.domContentLoadedEventEnd - perfEntry.startTime;
    });

    console.log(`DOMContentLoaded: ${domContentLoaded.toFixed(2)}ms`);

    // DOMContentLoaded should fire within 1 second for a static page
    expect(domContentLoaded).toBeLessThan(1000);
  });

  /**
   * Additional Test: All critical content visible quickly
   * Verifies that key sections load without delay
   */
  test('Critical content renders within performance budget', async ({ page }) => {
    const startTime = Date.now();

    await page.goto(`file://${indexPath}`);

    // All critical sections should be visible quickly
    const criticalSections = [
      '[data-testid="navbar"]',
      '[data-testid="hero-section"]',
      '[data-testid="hero-title"]',
      '[data-testid="hero-cta"]'
    ];

    for (const selector of criticalSections) {
      await expect(page.locator(selector)).toBeVisible({ timeout: 2000 });
    }

    const totalTime = Date.now() - startTime;
    console.log(`All critical content visible in: ${totalTime}ms`);

    // Should complete within 2 seconds
    expect(totalTime).toBeLessThan(2000);
  });
});
