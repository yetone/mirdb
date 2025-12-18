import { test, expect, chromium, Browser, Page, BrowserContext } from '@playwright/test';

/**
 * Performance Tests for MirDB Homepage
 *
 * These tests verify that the homepage meets performance requirements:
 * - First Contentful Paint (FCP) under 1.8 seconds
 * - Largest Contentful Paint (LCP) under 2.5 seconds
 * - Lighthouse Performance Score >= 90 (measured via core web vitals)
 * - Total page weight under 2MB
 */

test.describe('Performance - Page Load', () => {
  let browser: Browser;
  let context: BrowserContext;
  let page: Page;

  test.beforeAll(async () => {
    // Launch browser with specific configuration for performance testing
    browser = await chromium.launch({
      args: [
        '--no-sandbox',
        '--disable-dev-shm-usage',
        '--disable-gpu',
      ],
    });
  });

  test.afterAll(async () => {
    if (browser) {
      await browser.close();
    }
  });

  test.beforeEach(async () => {
    context = await browser.newContext();
    page = await context.newPage();
  });

  test.afterEach(async () => {
    if (page) {
      await page.close();
    }
    if (context) {
      await context.close();
    }
  });

  test('TC1: First Contentful Paint (FCP) is under 1.8 seconds', async () => {
    // Set up performance observer before navigation
    await page.addInitScript(() => {
      (window as any).__fcpMetric = 0;
      const observer = new PerformanceObserver((list) => {
        const entries = list.getEntries();
        const fcpEntry = entries.find(entry => entry.name === 'first-contentful-paint');
        if (fcpEntry) {
          (window as any).__fcpMetric = fcpEntry.startTime;
        }
      });
      observer.observe({ type: 'paint', buffered: true });
    });

    // Navigate to page and wait for load
    await page.goto('http://localhost:3000', { waitUntil: 'networkidle' });

    // Give a moment for the metric to be captured
    await page.waitForTimeout(500);

    // Get FCP from the global variable or performance API
    const fcp = await page.evaluate(() => {
      // Check the observer-captured value first
      if ((window as any).__fcpMetric > 0) {
        return (window as any).__fcpMetric;
      }
      // Fallback: Check performance entries directly
      const entries = performance.getEntriesByType('paint');
      const fcpEntry = entries.find(entry => entry.name === 'first-contentful-paint');
      return fcpEntry ? fcpEntry.startTime : 0;
    });

    console.log(`First Contentful Paint (FCP): ${fcp.toFixed(2)}ms`);

    // FCP should be under 1800ms (1.8 seconds)
    // If FCP is 0, it means the metric wasn't captured (common in headless)
    // In that case, we fall back to DOM Content Loaded timing
    if (fcp > 0) {
      expect(fcp).toBeLessThan(1800);
    } else {
      // Fallback: Use DOM Content Loaded as a proxy
      const dcl = await page.evaluate(() => {
        const nav = performance.getEntriesByType('navigation')[0] as PerformanceNavigationTiming;
        return nav.domContentLoadedEventEnd - nav.startTime;
      });
      console.log(`FCP not available, using DOM Content Loaded: ${dcl.toFixed(2)}ms`);
      // DOM Content Loaded should still be under our FCP threshold
      expect(dcl).toBeLessThan(1800);
    }
  });

  test('TC2: Largest Contentful Paint (LCP) is under 2.5 seconds', async () => {
    // Set up LCP observer before navigation
    await page.addInitScript(() => {
      (window as any).__lcpMetric = 0;
      const observer = new PerformanceObserver((list) => {
        const entries = list.getEntries();
        if (entries.length > 0) {
          // LCP is the last entry
          (window as any).__lcpMetric = entries[entries.length - 1].startTime;
        }
      });
      observer.observe({ type: 'largest-contentful-paint', buffered: true });
    });

    // Navigate to page
    await page.goto('http://localhost:3000', { waitUntil: 'networkidle' });

    // Wait for LCP to be captured (it finalizes after user interaction or load)
    await page.waitForTimeout(2000);

    // Trigger an interaction to finalize LCP
    await page.mouse.move(100, 100);
    await page.waitForTimeout(500);

    // Get LCP from the global variable
    const lcp = await page.evaluate(() => {
      return (window as any).__lcpMetric || 0;
    });

    console.log(`Largest Contentful Paint (LCP): ${lcp.toFixed(2)}ms`);

    // LCP should be under 2500ms (2.5 seconds)
    // If LCP is 0, use load event as fallback (common in headless mode)
    if (lcp > 0) {
      expect(lcp).toBeLessThan(2500);
    } else {
      // Fallback: Use load event timing as proxy for LCP
      const loadTime = await page.evaluate(() => {
        const nav = performance.getEntriesByType('navigation')[0] as PerformanceNavigationTiming;
        return nav.loadEventEnd - nav.startTime;
      });
      console.log(`LCP not available in headless mode, using Load Time: ${loadTime.toFixed(2)}ms`);
      // Load time should be under our LCP threshold
      expect(loadTime).toBeLessThan(2500);
    }
  });

  test('TC3: Lighthouse performance score is 90 or higher', async () => {
    // Since playwright-lighthouse has compatibility issues,
    // we'll measure core web vitals directly and calculate an equivalent score

    // Set up observers before navigation
    await page.addInitScript(() => {
      (window as any).__metrics = {
        fcp: 0,
        lcp: 0,
        cls: 0,
        tbt: 0,
      };

      // FCP Observer
      new PerformanceObserver((list) => {
        const entries = list.getEntries();
        const fcpEntry = entries.find(entry => entry.name === 'first-contentful-paint');
        if (fcpEntry) {
          (window as any).__metrics.fcp = fcpEntry.startTime;
        }
      }).observe({ type: 'paint', buffered: true });

      // LCP Observer
      new PerformanceObserver((list) => {
        const entries = list.getEntries();
        if (entries.length > 0) {
          (window as any).__metrics.lcp = entries[entries.length - 1].startTime;
        }
      }).observe({ type: 'largest-contentful-paint', buffered: true });

      // CLS Observer
      new PerformanceObserver((list) => {
        const entries = list.getEntries();
        for (const entry of entries) {
          if (!(entry as any).hadRecentInput) {
            (window as any).__metrics.cls += (entry as any).value;
          }
        }
      }).observe({ type: 'layout-shift', buffered: true });

      // Long Task Observer (for TBT approximation)
      (window as any).__longTasks = [];
      new PerformanceObserver((list) => {
        const entries = list.getEntries();
        for (const entry of entries) {
          (window as any).__longTasks.push(entry.duration);
        }
      }).observe({ type: 'longtask', buffered: true });
    });

    // Navigate to page
    await page.goto('http://localhost:3000', { waitUntil: 'networkidle' });

    // Wait for metrics to stabilize
    await page.waitForTimeout(2000);
    await page.mouse.move(100, 100);
    await page.waitForTimeout(500);

    // Get all metrics
    const metrics = await page.evaluate(() => {
      const nav = performance.getEntriesByType('navigation')[0] as PerformanceNavigationTiming;
      const longTasks = (window as any).__longTasks || [];
      const tbt = longTasks.reduce((sum: number, duration: number) => sum + Math.max(0, duration - 50), 0);

      return {
        fcp: (window as any).__metrics?.fcp || 0,
        lcp: (window as any).__metrics?.lcp || 0,
        cls: (window as any).__metrics?.cls || 0,
        tbt: tbt,
        loadTime: nav.loadEventEnd - nav.startTime,
        domContentLoaded: nav.domContentLoadedEventEnd - nav.startTime,
        ttfb: nav.responseStart - nav.requestStart,
      };
    });

    console.log('Core Web Vitals:');
    console.log(`  FCP: ${metrics.fcp > 0 ? metrics.fcp.toFixed(2) + 'ms' : 'Not captured (using DCL: ' + metrics.domContentLoaded.toFixed(2) + 'ms)'}`);
    console.log(`  LCP: ${metrics.lcp > 0 ? metrics.lcp.toFixed(2) + 'ms' : 'Not captured (using Load: ' + metrics.loadTime.toFixed(2) + 'ms)'}`);
    console.log(`  CLS: ${metrics.cls.toFixed(4)}`);
    console.log(`  TBT: ${metrics.tbt.toFixed(2)}ms`);
    console.log(`  TTFB: ${metrics.ttfb.toFixed(2)}ms`);

    // Calculate a performance score based on Lighthouse scoring methodology
    // Using simplified scoring based on Core Web Vitals thresholds
    // FCP: Good < 1800ms, Needs Improvement < 3000ms, Poor >= 3000ms
    // LCP: Good < 2500ms, Needs Improvement < 4000ms, Poor >= 4000ms
    // CLS: Good < 0.1, Needs Improvement < 0.25, Poor >= 0.25
    // TBT: Good < 200ms, Needs Improvement < 600ms, Poor >= 600ms

    const fcpValue = metrics.fcp > 0 ? metrics.fcp : metrics.domContentLoaded;
    const lcpValue = metrics.lcp > 0 ? metrics.lcp : metrics.loadTime;

    const fcpScore = fcpValue < 1800 ? 100 : fcpValue < 3000 ? 70 : 30;
    const lcpScore = lcpValue < 2500 ? 100 : lcpValue < 4000 ? 70 : 30;
    const clsScore = metrics.cls < 0.1 ? 100 : metrics.cls < 0.25 ? 70 : 30;
    const tbtScore = metrics.tbt < 200 ? 100 : metrics.tbt < 600 ? 70 : 30;
    const ttfbScore = metrics.ttfb < 200 ? 100 : metrics.ttfb < 500 ? 80 : 50;

    // Weighted average similar to Lighthouse (LCP and TBT are most weighted)
    // Lighthouse weights: FCP 10%, LCP 25%, TBT 30%, CLS 25%, Speed Index 10%
    // We'll approximate without Speed Index
    const performanceScore = Math.round(
      fcpScore * 0.15 +
      lcpScore * 0.30 +
      tbtScore * 0.35 +
      clsScore * 0.20
    );

    console.log(`\nPerformance Score Breakdown:`);
    console.log(`  FCP Score: ${fcpScore}`);
    console.log(`  LCP Score: ${lcpScore}`);
    console.log(`  TBT Score: ${tbtScore}`);
    console.log(`  CLS Score: ${clsScore}`);
    console.log(`  Overall Performance Score: ${performanceScore}`);

    // Performance score should be at least 90
    expect(performanceScore).toBeGreaterThanOrEqual(90);
  });

  test('TC4: Total page weight is under 2MB', async () => {
    // Create a list to track all network resources
    const resources: { url: string; size: number; type: string }[] = [];

    // Listen to all network responses
    page.on('response', async (response) => {
      try {
        const request = response.request();
        const resourceType = request.resourceType();
        const url = response.url();

        // Get content length from headers or body
        const headers = response.headers();
        let size = 0;

        if (headers['content-length']) {
          size = parseInt(headers['content-length'], 10);
        } else {
          try {
            const body = await response.body();
            size = body.length;
          } catch {
            // Some responses may not have a body
            size = 0;
          }
        }

        resources.push({
          url: url,
          size: size,
          type: resourceType,
        });
      } catch {
        // Ignore errors for redirects or failed requests
      }
    });

    // Navigate to page
    await page.goto('http://localhost:3000', { waitUntil: 'networkidle' });

    // Wait a bit for all resources to be tracked
    await page.waitForTimeout(1000);

    // Calculate total size
    const totalSize = resources.reduce((sum, r) => sum + r.size, 0);
    const totalSizeKB = totalSize / 1024;
    const totalSizeMB = totalSizeKB / 1024;

    console.log(`Total page weight: ${totalSizeMB.toFixed(2)}MB (${totalSizeKB.toFixed(2)}KB)`);
    console.log('\nResource breakdown:');

    // Group resources by type
    const byType: { [key: string]: number } = {};
    resources.forEach(r => {
      byType[r.type] = (byType[r.type] || 0) + r.size;
    });

    Object.entries(byType).forEach(([type, size]) => {
      console.log(`  ${type}: ${(size / 1024).toFixed(2)}KB`);
    });

    // Total transferred size should be under 2MB (2048KB)
    // Note: This is a generous limit; well-optimized pages are typically under 500KB
    expect(totalSizeMB).toBeLessThan(2);
  });
});

test.describe('Performance - Additional Metrics', () => {
  test('Page navigation metrics are within acceptable ranges', async ({ page }) => {
    // Use Playwright's built-in performance metrics
    await page.goto('http://localhost:3000', { waitUntil: 'networkidle' });

    // Get navigation timing metrics
    const metrics = await page.evaluate(() => {
      const navigation = performance.getEntriesByType('navigation')[0] as PerformanceNavigationTiming;

      return {
        // DNS lookup time
        dnsLookup: navigation.domainLookupEnd - navigation.domainLookupStart,
        // Connection time
        connection: navigation.connectEnd - navigation.connectStart,
        // Time to first byte
        ttfb: navigation.responseStart - navigation.requestStart,
        // DOM content loaded
        domContentLoaded: navigation.domContentLoadedEventEnd - navigation.startTime,
        // Full page load
        loadComplete: navigation.loadEventEnd - navigation.startTime,
        // DOM interactive
        domInteractive: navigation.domInteractive - navigation.startTime,
      };
    });

    console.log('Navigation Timing Metrics:');
    console.log(`  DNS Lookup: ${metrics.dnsLookup.toFixed(2)}ms`);
    console.log(`  Connection: ${metrics.connection.toFixed(2)}ms`);
    console.log(`  Time to First Byte (TTFB): ${metrics.ttfb.toFixed(2)}ms`);
    console.log(`  DOM Content Loaded: ${metrics.domContentLoaded.toFixed(2)}ms`);
    console.log(`  DOM Interactive: ${metrics.domInteractive.toFixed(2)}ms`);
    console.log(`  Full Load: ${metrics.loadComplete.toFixed(2)}ms`);

    // Basic assertions for reasonable load times
    // DOM Content Loaded should be under 3 seconds
    expect(metrics.domContentLoaded).toBeLessThan(3000);
    // Full load should be under 5 seconds
    expect(metrics.loadComplete).toBeLessThan(5000);
    // TTFB should be under 500ms for a static site
    expect(metrics.ttfb).toBeLessThan(500);
  });

  test('Critical resources load efficiently', async ({ page }) => {
    const resourceTimings: { name: string; duration: number; size: number }[] = [];

    page.on('response', async (response) => {
      const url = response.url();
      const request = response.request();
      const timing = request.timing();

      // Only track main resources (HTML, CSS, JS)
      const type = request.resourceType();
      if (['document', 'stylesheet', 'script'].includes(type)) {
        let size = 0;
        try {
          const body = await response.body();
          size = body.length;
        } catch {
          // Ignore
        }

        resourceTimings.push({
          name: url.split('/').pop() || url,
          duration: timing.responseEnd,
          size: size,
        });
      }
    });

    await page.goto('http://localhost:3000', { waitUntil: 'networkidle' });

    console.log('\nCritical Resource Load Times:');
    resourceTimings.forEach(r => {
      console.log(`  ${r.name}: ${r.duration.toFixed(2)}ms (${(r.size / 1024).toFixed(2)}KB)`);
    });

    // All critical resources should load within 2 seconds
    resourceTimings.forEach(r => {
      expect(r.duration).toBeLessThan(2000);
    });
  });
});
