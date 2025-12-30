// @ts-check
const { test, expect, chromium } = require('@playwright/test');
const { playAudit } = require('playwright-lighthouse');

/**
 * Performance tests for MirDB homepage
 * Verifies page load performance against NFR-1 requirements
 *
 * Test cases:
 * TC1: Page loads within 3 seconds (NFR-1)
 * TC2: Lighthouse performance score >= 80
 * TC3: First Contentful Paint (FCP) under 1.8 seconds
 * TC4: Largest Contentful Paint (LCP) under 2.5 seconds
 */

// Use a single worker for Lighthouse tests to avoid port conflicts
test.describe.configure({ mode: 'serial' });

test.describe('Page Load Performance', () => {
  // Performance thresholds from requirements
  const THRESHOLDS = {
    pageLoadTime: 3000, // 3 seconds in ms (NFR-1)
    lighthouseScore: 80, // Lighthouse performance score >= 80
    fcp: 1800, // First Contentful Paint under 1.8 seconds
    lcp: 2500, // Largest Contentful Paint under 2.5 seconds
  };

  test('TC1: Page loads within 3 seconds (NFR-1)', async ({ page }) => {
    // Measure page load time from navigation start to load event
    const startTime = Date.now();

    await page.goto('/', { waitUntil: 'load' });

    const loadTime = Date.now() - startTime;

    // Verify page loaded successfully
    await expect(page.locator('body')).toBeVisible();

    // Also check Performance API for more accurate timing
    const performanceTiming = await page.evaluate(() => {
      const timing = performance.timing;
      return {
        loadEventEnd: timing.loadEventEnd,
        navigationStart: timing.navigationStart,
        domContentLoadedEventEnd: timing.domContentLoadedEventEnd,
      };
    });

    const pageLoadDuration = performanceTiming.loadEventEnd - performanceTiming.navigationStart;

    // Use the measurement that captures the actual load time
    // (Performance API timing is more accurate but may be 0 if load event hasn't fired)
    const actualLoadTime = pageLoadDuration > 0 ? pageLoadDuration : loadTime;

    console.log(`Page load time: ${actualLoadTime}ms (threshold: ${THRESHOLDS.pageLoadTime}ms)`);

    expect(actualLoadTime).toBeLessThan(THRESHOLDS.pageLoadTime);
  });

  test('TC2: Lighthouse performance score >= 80', async () => {
    // Launch browser with remote debugging port for Lighthouse
    const browser = await chromium.launch({
      args: ['--remote-debugging-port=9222'],
    });

    const page = await browser.newPage();
    await page.goto('http://localhost:3000/', { waitUntil: 'networkidle' });

    try {
      // Run Lighthouse audit with performance threshold
      const auditResult = await playAudit({
        page,
        port: 9222,
        thresholds: {
          performance: THRESHOLDS.lighthouseScore,
        },
      });

      console.log(`Lighthouse performance score: ${auditResult?.lhr?.categories?.performance?.score * 100 || 'N/A'}`);
    } finally {
      await browser.close();
    }
  });

  test('TC3: First Contentful Paint (FCP) under 1.8 seconds', async () => {
    // Launch browser with remote debugging port for Lighthouse
    const browser = await chromium.launch({
      args: ['--remote-debugging-port=9223'],
    });

    const page = await browser.newPage();
    await page.goto('http://localhost:3000/', { waitUntil: 'networkidle' });

    try {
      // Run Lighthouse audit with performance threshold (required, cannot be empty)
      // We'll use a low threshold of 1 to ensure it passes, then manually check FCP
      const auditResult = await playAudit({
        page,
        port: 9223,
        thresholds: {
          performance: 1, // Minimal threshold to get the audit results
        },
      });

      // Get FCP value from Lighthouse results
      const fcpAudit = auditResult?.lhr?.audits?.['first-contentful-paint'];
      const fcpValue = fcpAudit?.numericValue || 0;

      console.log(`First Contentful Paint: ${fcpValue}ms (threshold: ${THRESHOLDS.fcp}ms)`);

      expect(fcpValue).toBeLessThan(THRESHOLDS.fcp);
    } finally {
      await browser.close();
    }
  });

  test('TC4: Largest Contentful Paint (LCP) under 2.5 seconds', async () => {
    // Launch browser with remote debugging port for Lighthouse
    const browser = await chromium.launch({
      args: ['--remote-debugging-port=9224'],
    });

    const page = await browser.newPage();
    await page.goto('http://localhost:3000/', { waitUntil: 'networkidle' });

    try {
      // Run Lighthouse audit with performance threshold (required, cannot be empty)
      // We'll use a low threshold of 1 to ensure it passes, then manually check LCP
      const auditResult = await playAudit({
        page,
        port: 9224,
        thresholds: {
          performance: 1, // Minimal threshold to get the audit results
        },
      });

      // Get LCP value from Lighthouse results
      const lcpAudit = auditResult?.lhr?.audits?.['largest-contentful-paint'];
      const lcpValue = lcpAudit?.numericValue || 0;

      console.log(`Largest Contentful Paint: ${lcpValue}ms (threshold: ${THRESHOLDS.lcp}ms)`);

      expect(lcpValue).toBeLessThan(THRESHOLDS.lcp);
    } finally {
      await browser.close();
    }
  });
});
