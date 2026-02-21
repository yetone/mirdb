/**
 * Page Load Performance E2E Tests
 * Owner: Scenario 18 - Page Load Performance
 *
 * Tests:
 * - Page load time within 3 seconds
 * - Lighthouse performance audit (score >= 90)
 * - Lighthouse accessibility audit (score >= 90)
 */

const { test, expect, chromium } = require('@playwright/test');
const lighthouseModule = require('lighthouse');
const lighthouse = lighthouseModule.default || lighthouseModule;

// Lighthouse configuration for performance testing
const lighthouseConfig = {
  extends: 'lighthouse:default',
  settings: {
    onlyCategories: ['performance', 'accessibility'],
    formFactor: 'desktop',
    screenEmulation: {
      mobile: false,
      width: 1920,
      height: 1080,
      deviceScaleFactor: 1,
      disabled: false,
    },
    throttling: {
      rttMs: 40,
      throughputKbps: 10240,
      cpuSlowdownMultiplier: 1,
      requestLatencyMs: 0,
      downloadThroughputKbps: 10240,
      uploadThroughputKbps: 5120,
    },
  },
};

test.describe('Page Load Performance', () => {
  test('Test Case 1: Homepage fully loads within 3 seconds', async ({ page }) => {
    // NFR-1: The homepage must load completely within 3 seconds on standard broadband
    const startTime = Date.now();

    // Navigate to homepage and wait for load event
    await page.goto('/', { waitUntil: 'load' });

    const loadTime = Date.now() - startTime;

    // Verify page is fully loaded
    await expect(page.locator('#hero')).toBeVisible();
    await expect(page.locator('#features')).toBeVisible();
    await expect(page.locator('#quickstart')).toBeVisible();

    // Assert load time is under 3 seconds (3000ms)
    expect(loadTime).toBeLessThan(3000);

    console.log(`Page load time: ${loadTime}ms`);
  });

  test('Test Case 2: Lighthouse performance audit scores 90 or higher', async () => {
    // Launch a separate Chrome instance for Lighthouse
    const browser = await chromium.launch({
      args: ['--remote-debugging-port=9222'],
    });

    try {
      // Run Lighthouse performance audit
      const result = await lighthouse('http://localhost:3000/', {
        port: 9222,
        output: 'json',
        logLevel: 'error',
        onlyCategories: ['performance'],
      }, lighthouseConfig);

      const performanceScore = result.lhr.categories.performance.score * 100;

      console.log(`Lighthouse Performance Score: ${performanceScore}`);

      // Assert performance score is 90 or higher
      expect(performanceScore).toBeGreaterThanOrEqual(90);
    } finally {
      await browser.close();
    }
  });

  test('Test Case 3: Lighthouse accessibility audit scores 90 or higher', async () => {
    // Launch a separate Chrome instance for Lighthouse
    const browser = await chromium.launch({
      args: ['--remote-debugging-port=9223'],
    });

    try {
      // Run Lighthouse accessibility audit
      const result = await lighthouse('http://localhost:3000/', {
        port: 9223,
        output: 'json',
        logLevel: 'error',
        onlyCategories: ['accessibility'],
      }, lighthouseConfig);

      const accessibilityScore = result.lhr.categories.accessibility.score * 100;

      console.log(`Lighthouse Accessibility Score: ${accessibilityScore}`);

      // Assert accessibility score is 90 or higher
      expect(accessibilityScore).toBeGreaterThanOrEqual(90);
    } finally {
      await browser.close();
    }
  });
});
