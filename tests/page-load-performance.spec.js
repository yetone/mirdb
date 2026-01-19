// @ts-check
const { test, expect, chromium } = require('@playwright/test');
const lighthouse = require('lighthouse').default;

/**
 * E2E tests for Page Load Performance (NFR-1)
 * Scenario: Verify that page load time is under 3 seconds and meets performance metrics
 *
 * Test Cases:
 * 1. Page loads completely in under 3 seconds
 * 2. Lighthouse performance score is 90 or higher
 * 3. First Contentful Paint (FCP) occurs within 1.8 seconds
 * 4. Largest Contentful Paint (LCP) occurs within 2.5 seconds
 */

test.describe('Page Load Performance (NFR-1)', () => {

  /**
   * Test Case 1: Page loads completely in under 3 seconds
   * Input: Measure page load time on broadband
   * Expected: Page loads completely in under 3 seconds
   */
  test('TC1: Page loads completely in under 3 seconds', async ({ page }) => {
    // Start timing
    const startTime = Date.now();

    // Navigate to the homepage and wait for network to be idle
    await page.goto('/', { waitUntil: 'networkidle' });

    // Calculate load time
    const loadTime = Date.now() - startTime;

    // Verify page loaded in under 3 seconds (3000ms)
    expect(loadTime).toBeLessThan(3000);

    // Also verify key content is visible
    await expect(page.locator('[data-testid="hero-section"]')).toBeVisible();
  });

  /**
   * Test Case 2: Lighthouse performance score is 90 or higher
   * Input: Run Lighthouse performance audit
   * Expected: Lighthouse performance score is 90 or higher
   */
  test('TC2: Lighthouse performance score is 90 or higher', async () => {
    // Launch browser with remote debugging for Lighthouse
    const browser = await chromium.launch({
      args: ['--remote-debugging-port=9222'],
      headless: true
    });

    try {
      // Run Lighthouse audit
      const { lhr } = await lighthouse('http://localhost:3000', {
        port: 9222,
        output: 'json',
        logLevel: 'error',
        onlyCategories: ['performance'],
        throttling: {
          // Simulate 10Mbps broadband connection
          throughputKbps: 10240,
          cpuSlowdownMultiplier: 1,
          requestLatencyMs: 40
        },
        formFactor: 'desktop',
        screenEmulation: {
          mobile: false,
          width: 1350,
          height: 940,
          deviceScaleFactor: 1,
          disabled: false
        }
      });

      // Get performance score (0-1, multiply by 100 to get percentage)
      const performanceScore = lhr.categories.performance.score * 100;

      // Verify performance score is 90 or higher
      expect(performanceScore).toBeGreaterThanOrEqual(90);
    } finally {
      await browser.close();
    }
  });

  /**
   * Test Case 3: First Contentful Paint (FCP) occurs within 1.8 seconds
   * Input: Check First Contentful Paint (FCP)
   * Expected: FCP occurs within 1.8 seconds
   */
  test('TC3: First Contentful Paint (FCP) occurs within 1.8 seconds', async () => {
    // Launch browser with remote debugging for Lighthouse
    const browser = await chromium.launch({
      args: ['--remote-debugging-port=9223'],
      headless: true
    });

    try {
      // Run Lighthouse audit
      const { lhr } = await lighthouse('http://localhost:3000', {
        port: 9223,
        output: 'json',
        logLevel: 'error',
        onlyCategories: ['performance'],
        throttling: {
          // Simulate 10Mbps broadband connection
          throughputKbps: 10240,
          cpuSlowdownMultiplier: 1,
          requestLatencyMs: 40
        },
        formFactor: 'desktop',
        screenEmulation: {
          mobile: false,
          width: 1350,
          height: 940,
          deviceScaleFactor: 1,
          disabled: false
        }
      });

      // Get FCP metric in milliseconds
      const fcpAudit = lhr.audits['first-contentful-paint'];
      const fcpMs = fcpAudit.numericValue;

      // Verify FCP is within 1.8 seconds (1800ms)
      expect(fcpMs).toBeLessThanOrEqual(1800);
    } finally {
      await browser.close();
    }
  });

  /**
   * Test Case 4: Largest Contentful Paint (LCP) occurs within 2.5 seconds
   * Input: Check Largest Contentful Paint (LCP)
   * Expected: LCP occurs within 2.5 seconds
   */
  test('TC4: Largest Contentful Paint (LCP) occurs within 2.5 seconds', async () => {
    // Launch browser with remote debugging for Lighthouse
    const browser = await chromium.launch({
      args: ['--remote-debugging-port=9224'],
      headless: true
    });

    try {
      // Run Lighthouse audit
      const { lhr } = await lighthouse('http://localhost:3000', {
        port: 9224,
        output: 'json',
        logLevel: 'error',
        onlyCategories: ['performance'],
        throttling: {
          // Simulate 10Mbps broadband connection
          throughputKbps: 10240,
          cpuSlowdownMultiplier: 1,
          requestLatencyMs: 40
        },
        formFactor: 'desktop',
        screenEmulation: {
          mobile: false,
          width: 1350,
          height: 940,
          deviceScaleFactor: 1,
          disabled: false
        }
      });

      // Get LCP metric in milliseconds
      const lcpAudit = lhr.audits['largest-contentful-paint'];
      const lcpMs = lcpAudit.numericValue;

      // Verify LCP is within 2.5 seconds (2500ms)
      expect(lcpMs).toBeLessThanOrEqual(2500);
    } finally {
      await browser.close();
    }
  });

});
