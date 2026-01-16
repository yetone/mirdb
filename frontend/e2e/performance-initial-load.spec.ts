import { test, expect, Page, CDPSession } from '@playwright/test';

/**
 * Performance - Initial Load Time E2E Tests
 *
 * This test file covers the scenario: "Verify homepage loads initial content
 * within 2 seconds as specified in NFR-1"
 *
 * Test Cases:
 * 1. Measure First Contentful Paint (FCP) - Expected under 1.8 seconds
 * 2. Measure Largest Contentful Paint (LCP) - Expected under 2.5 seconds
 * 3. Measure Cumulative Layout Shift (CLS) - Expected under 0.1
 *
 * These metrics align with Google Core Web Vitals standards and NFR-1 requirements.
 */

interface PerformanceMetrics {
  fcp: number | null;
  lcp: number | null;
  cls: number;
}

/**
 * Collects Core Web Vitals metrics using CDP (Chrome DevTools Protocol)
 */
async function collectCoreWebVitals(page: Page): Promise<PerformanceMetrics> {
  const client = await page.context().newCDPSession(page);

  // Enable performance metrics
  await client.send('Performance.enable');

  // Track layout shifts for CLS
  let cumulativeLayoutShift = 0;

  // Set up listener for layout shift entries before navigation
  await page.addInitScript(() => {
    (window as any).__layoutShifts = [];
    const observer = new PerformanceObserver((list) => {
      for (const entry of list.getEntries()) {
        if (!(entry as any).hadRecentInput) {
          (window as any).__layoutShifts.push((entry as any).value);
        }
      }
    });
    observer.observe({ type: 'layout-shift', buffered: true });
  });

  // Navigate with clear cache to simulate cold start
  await page.goto('/', { waitUntil: 'networkidle' });

  // Wait for page to stabilize
  await page.waitForTimeout(2000);

  // Get FCP and LCP from Performance API
  const metrics = await page.evaluate(() => {
    const fcp = performance.getEntriesByType('paint')
      .find(entry => entry.name === 'first-contentful-paint')?.startTime ?? null;

    // Get LCP from PerformanceObserver entries
    const lcpEntries = performance.getEntriesByType('largest-contentful-paint');
    const lcp = lcpEntries.length > 0
      ? (lcpEntries[lcpEntries.length - 1] as any).startTime
      : null;

    // Get accumulated layout shifts
    const layoutShifts = (window as any).__layoutShifts || [];
    const cls = layoutShifts.reduce((sum: number, shift: number) => sum + shift, 0);

    return { fcp, lcp, cls };
  });

  await client.send('Performance.disable');

  return metrics;
}

/**
 * Alternative method to get LCP using PerformanceObserver with promise
 */
async function getLCPWithObserver(page: Page): Promise<number | null> {
  await page.addInitScript(() => {
    (window as any).__lcpValue = null;
    const observer = new PerformanceObserver((list) => {
      const entries = list.getEntries();
      if (entries.length > 0) {
        (window as any).__lcpValue = (entries[entries.length - 1] as any).startTime;
      }
    });
    observer.observe({ type: 'largest-contentful-paint', buffered: true });
  });

  await page.goto('/', { waitUntil: 'networkidle' });
  await page.waitForTimeout(2000);

  const lcp = await page.evaluate(() => (window as any).__lcpValue);
  return lcp;
}

test.describe('Performance - Initial Load Time E2E Tests', () => {
  test.describe.configure({ mode: 'serial' });

  /**
   * Test Case 1: Measure First Contentful Paint (FCP)
   * Input: Measure First Contentful Paint (FCP)
   * Expected: FCP under 1.8 seconds
   */
  test('FCP should be under 1.8 seconds', async ({ page }) => {
    // Clear browser context for cold start simulation
    await page.context().clearCookies();

    // Add FCP observer before navigation
    await page.addInitScript(() => {
      (window as any).__fcpValue = null;
      const observer = new PerformanceObserver((list) => {
        for (const entry of list.getEntries()) {
          if (entry.name === 'first-contentful-paint') {
            (window as any).__fcpValue = entry.startTime;
          }
        }
      });
      observer.observe({ type: 'paint', buffered: true });
    });

    // Navigate to homepage
    await page.goto('/', { waitUntil: 'domcontentloaded' });

    // Wait for paint events to be recorded
    await page.waitForTimeout(1000);

    // Get FCP value
    const fcp = await page.evaluate(() => {
      // Try from observer first
      if ((window as any).__fcpValue) {
        return (window as any).__fcpValue;
      }
      // Fallback to performance.getEntriesByType
      const entries = performance.getEntriesByType('paint');
      const fcpEntry = entries.find(e => e.name === 'first-contentful-paint');
      return fcpEntry?.startTime ?? null;
    });

    console.log(`First Contentful Paint (FCP): ${fcp}ms`);

    expect(fcp).not.toBeNull();
    expect(fcp).toBeLessThan(1800); // 1.8 seconds = 1800ms
  });

  /**
   * Test Case 2: Measure Largest Contentful Paint (LCP)
   * Input: Measure Largest Contentful Paint (LCP)
   * Expected: LCP under 2.5 seconds
   */
  test('LCP should be under 2.5 seconds', async ({ page }) => {
    // Clear browser context for cold start simulation
    await page.context().clearCookies();

    // Add LCP observer before navigation
    await page.addInitScript(() => {
      (window as any).__lcpValue = null;
      const observer = new PerformanceObserver((list) => {
        const entries = list.getEntries();
        if (entries.length > 0) {
          // LCP is the last entry (most recent largest paint)
          (window as any).__lcpValue = (entries[entries.length - 1] as any).startTime;
        }
      });
      observer.observe({ type: 'largest-contentful-paint', buffered: true });
    });

    // Navigate to homepage
    await page.goto('/', { waitUntil: 'networkidle' });

    // Wait for LCP to be measured (happens after load)
    await page.waitForTimeout(2000);

    // Get LCP value
    const lcp = await page.evaluate(() => (window as any).__lcpValue);

    console.log(`Largest Contentful Paint (LCP): ${lcp}ms`);

    expect(lcp).not.toBeNull();
    expect(lcp).toBeLessThan(2500); // 2.5 seconds = 2500ms
  });

  /**
   * Test Case 3: Measure Cumulative Layout Shift (CLS)
   * Input: Measure Cumulative Layout Shift (CLS)
   * Expected: CLS under 0.1
   */
  test('CLS should be under 0.1', async ({ page }) => {
    // Clear browser context for cold start simulation
    await page.context().clearCookies();

    // Add CLS observer before navigation
    await page.addInitScript(() => {
      (window as any).__clsValue = 0;
      const observer = new PerformanceObserver((list) => {
        for (const entry of list.getEntries()) {
          // Only count layout shifts without recent user input
          if (!(entry as any).hadRecentInput) {
            (window as any).__clsValue += (entry as any).value;
          }
        }
      });
      observer.observe({ type: 'layout-shift', buffered: true });
    });

    // Navigate to homepage
    await page.goto('/', { waitUntil: 'networkidle' });

    // Wait for page to stabilize and capture all layout shifts
    await page.waitForTimeout(3000);

    // Scroll down to trigger any lazy-loaded content shifts
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
    await page.waitForTimeout(1000);

    // Scroll back up
    await page.evaluate(() => window.scrollTo(0, 0));
    await page.waitForTimeout(1000);

    // Get CLS value
    const cls = await page.evaluate(() => (window as any).__clsValue);

    console.log(`Cumulative Layout Shift (CLS): ${cls}`);

    expect(cls).toBeLessThan(0.1);
  });

  /**
   * Additional test: Verify hero section content is visible within 2 seconds
   * This directly tests NFR-1 requirement
   */
  test('hero section content should be visible within 2 seconds', async ({ page }) => {
    const startTime = Date.now();

    // Navigate and wait for hero content
    await page.goto('/');

    // Check hero section visibility
    const heroSection = page.locator('section').first();
    await expect(heroSection).toBeVisible({ timeout: 2000 });

    // Check for main headline visibility
    const headline = page.getByRole('heading', { level: 1 });
    await expect(headline).toBeVisible({ timeout: 2000 });

    // Check for CTA buttons visibility
    const getStartedButton = page.getByTestId('cta-get-started');
    await expect(getStartedButton).toBeVisible({ timeout: 2000 });

    const elapsed = Date.now() - startTime;
    console.log(`Hero content visible in: ${elapsed}ms`);

    expect(elapsed).toBeLessThan(2000);
  });

  /**
   * Comprehensive Core Web Vitals test
   * Collects all metrics in a single test run
   */
  test('all Core Web Vitals should meet thresholds', async ({ page }) => {
    await page.context().clearCookies();

    // Set up all observers before navigation
    await page.addInitScript(() => {
      (window as any).__metrics = {
        fcp: null,
        lcp: null,
        cls: 0
      };

      // FCP Observer
      const fcpObserver = new PerformanceObserver((list) => {
        for (const entry of list.getEntries()) {
          if (entry.name === 'first-contentful-paint') {
            (window as any).__metrics.fcp = entry.startTime;
          }
        }
      });
      fcpObserver.observe({ type: 'paint', buffered: true });

      // LCP Observer
      const lcpObserver = new PerformanceObserver((list) => {
        const entries = list.getEntries();
        if (entries.length > 0) {
          (window as any).__metrics.lcp = (entries[entries.length - 1] as any).startTime;
        }
      });
      lcpObserver.observe({ type: 'largest-contentful-paint', buffered: true });

      // CLS Observer
      const clsObserver = new PerformanceObserver((list) => {
        for (const entry of list.getEntries()) {
          if (!(entry as any).hadRecentInput) {
            (window as any).__metrics.cls += (entry as any).value;
          }
        }
      });
      clsObserver.observe({ type: 'layout-shift', buffered: true });
    });

    // Navigate to homepage
    await page.goto('/', { waitUntil: 'networkidle' });

    // Wait for metrics to stabilize
    await page.waitForTimeout(3000);

    // Scroll to trigger any lazy content
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
    await page.waitForTimeout(500);
    await page.evaluate(() => window.scrollTo(0, 0));
    await page.waitForTimeout(500);

    // Collect all metrics
    const metrics = await page.evaluate(() => (window as any).__metrics);

    console.log('Core Web Vitals Summary:');
    console.log(`  FCP: ${metrics.fcp}ms (threshold: 1800ms)`);
    console.log(`  LCP: ${metrics.lcp}ms (threshold: 2500ms)`);
    console.log(`  CLS: ${metrics.cls} (threshold: 0.1)`);

    // Verify all thresholds
    expect(metrics.fcp, 'FCP should be measured').not.toBeNull();
    expect(metrics.fcp, 'FCP should be under 1.8s').toBeLessThan(1800);

    expect(metrics.lcp, 'LCP should be measured').not.toBeNull();
    expect(metrics.lcp, 'LCP should be under 2.5s').toBeLessThan(2500);

    expect(metrics.cls, 'CLS should be under 0.1').toBeLessThan(0.1);
  });
});
