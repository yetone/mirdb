const { test, expect, chromium } = require('@playwright/test');
const { playAudit } = require('playwright-lighthouse');

/**
 * Performance - Page Load Time Tests
 * Scenario: Validate the homepage loads within acceptable time limits
 *
 * Steps:
 * 1. Clear cache and load page - Testing uncached performance
 * 2. Measure load time - Target: under 3 seconds on standard broadband
 * 3. Verify content visibility - Above-the-fold content should load quickly
 *
 * Test Cases:
 * 1. Lighthouse performance score >= 80 (Run Lighthouse performance audit)
 * 2. First Contentful Paint (FCP) under 2 seconds on fast connection
 * 3. Time to Interactive (TTI) under 3 seconds on standard broadband
 */

// Lighthouse configuration to only run performance audits
const lighthouseConfig = {
  extends: 'lighthouse:default',
  settings: {
    onlyCategories: ['performance'],
  },
};

test.describe('Performance - Page Load Time', () => {
  // Configure longer timeout for performance tests
  test.setTimeout(60000);

  // Only run Lighthouse tests in Chromium as it requires Chrome DevTools Protocol
  test.skip(({ browserName }) => browserName !== 'chromium', 'Lighthouse only works with Chromium');

  let browser;
  let page;
  const PORT = 9222;

  test.beforeAll(async () => {
    // Launch browser with remote debugging port for Lighthouse
    browser = await chromium.launch({
      args: [`--remote-debugging-port=${PORT}`],
    });
  });

  test.afterAll(async () => {
    if (browser) {
      await browser.close();
    }
  });

  test.beforeEach(async () => {
    const context = await browser.newContext();
    page = await context.newPage();
  });

  test.afterEach(async () => {
    if (page) {
      await page.close();
    }
  });

  test('Test Case 1: Lighthouse performance score >= 80', async () => {
    // Navigate to homepage (fresh page load simulates cleared cache)
    await page.goto('http://localhost:3000/');

    // Run Lighthouse audit with performance threshold
    const result = await playAudit({
      page: page,
      port: PORT,
      thresholds: {
        performance: 80,
      },
      config: lighthouseConfig,
      reports: {
        formats: {
          html: false,
          json: false,
        },
      },
    });

    // Get the actual performance score
    const performanceScore = result.lhr.categories.performance.score * 100;
    console.log(`Lighthouse Performance Score: ${performanceScore}`);

    // Verify performance score meets threshold
    expect(performanceScore).toBeGreaterThanOrEqual(80);
  });

  test('Test Case 2: First Contentful Paint (FCP) under 2 seconds', async () => {
    // Navigate to homepage (fresh page load simulates cleared cache)
    await page.goto('http://localhost:3000/');

    // Run Lighthouse audit to measure FCP
    const result = await playAudit({
      page: page,
      port: PORT,
      thresholds: {
        performance: 50, // Set a low threshold since we're testing FCP specifically
      },
      config: lighthouseConfig,
      reports: {
        formats: {
          html: false,
          json: false,
        },
      },
    });

    // Get FCP metric in milliseconds
    const fcpAudit = result.lhr.audits['first-contentful-paint'];
    const fcpMs = fcpAudit.numericValue;

    console.log(`First Contentful Paint: ${fcpMs}ms`);

    // FCP should be under 2000ms (2 seconds) on fast connection
    expect(fcpMs).toBeLessThan(2000);
  });

  test('Test Case 3: Time to Interactive (TTI) under 3 seconds', async () => {
    // Navigate to homepage (fresh page load simulates cleared cache)
    await page.goto('http://localhost:3000/');

    // Run Lighthouse audit to measure TTI
    const result = await playAudit({
      page: page,
      port: PORT,
      thresholds: {
        performance: 50, // Set a low threshold since we're testing TTI specifically
      },
      config: lighthouseConfig,
      reports: {
        formats: {
          html: false,
          json: false,
        },
      },
    });

    // Get TTI metric in milliseconds
    const ttiAudit = result.lhr.audits['interactive'];
    const ttiMs = ttiAudit.numericValue;

    console.log(`Time to Interactive: ${ttiMs}ms`);

    // TTI should be under 3000ms (3 seconds) on standard broadband
    expect(ttiMs).toBeLessThan(3000);
  });

  // Additional verification tests

  test('Verify above-the-fold content is visible quickly', async () => {
    const startTime = Date.now();

    await page.goto('http://localhost:3000/');

    // Wait for hero section to be visible (above-the-fold content)
    const heroSection = page.locator('.hero');
    await heroSection.waitFor({ state: 'visible', timeout: 2000 });

    const heroVisibleTime = Date.now() - startTime;
    console.log(`Hero section visible after: ${heroVisibleTime}ms`);

    // Verify hero content is visible within 2 seconds
    expect(heroVisibleTime).toBeLessThan(2000);

    // Verify headline text is rendered
    const headline = page.locator('.hero-headline');
    await expect(headline).toBeVisible();
    await expect(headline).toContainText('MirDB');
  });

  test('Page resources are optimized for performance', async () => {
    await page.goto('http://localhost:3000/', { waitUntil: 'networkidle' });

    // Check for inline styles (no external CSS files to block rendering)
    const hasInlineStyles = await page.evaluate(() => {
      const styleElements = document.querySelectorAll('style');
      return styleElements.length > 0;
    });
    expect(hasInlineStyles).toBe(true);

    // Verify no external render-blocking CSS
    const externalStylesheets = await page.locator('link[rel="stylesheet"]').count();
    expect(externalStylesheets).toBe(0);

    // Verify image resources have lazy loading
    const lazyImages = await page.locator('img[loading="lazy"]').count();
    expect(lazyImages).toBeGreaterThanOrEqual(2); // logo.gif and usage.gif
  });
});
