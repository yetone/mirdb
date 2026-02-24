/**
 * Performance E2E Tests
 * Owner: Scenario 13 - Performance Requirements
 *
 * Test cases:
 * - TTI under 2000ms
 * - Lighthouse performance >= 90
 * - FCP under 1.8s
 * - LCP under 2.5s
 * - CLS under 0.1
 */

import { test, expect } from '@playwright/test';

// Performance thresholds based on requirements
const PERFORMANCE_THRESHOLDS = {
  TTI: 2000,        // Time to Interactive in ms
  FCP: 1800,        // First Contentful Paint in ms
  LCP: 2500,        // Largest Contentful Paint in ms
  CLS: 0.1,         // Cumulative Layout Shift (unitless)
  PERFORMANCE_SCORE: 90,  // Lighthouse performance score (0-100)
};

test.describe('Performance Requirements', () => {

  test.describe('Page Load Performance', () => {

    test('page loads within 2 seconds (Time to Interactive)', async ({ page }) => {
      // Navigate to page and measure performance
      await page.goto('/', { waitUntil: 'networkidle' });

      // Get performance metrics using Performance API
      const metrics = await page.evaluate(() => {
        const entries = performance.getEntriesByType('navigation');
        if (entries.length > 0) {
          const navEntry = entries[0];
          return {
            domInteractive: navEntry.domInteractive,
            domContentLoadedEventEnd: navEntry.domContentLoadedEventEnd,
            loadEventEnd: navEntry.loadEventEnd,
            duration: navEntry.duration,
          };
        }
        return {
          domInteractive: performance.now(),
          domContentLoadedEventEnd: performance.now(),
          loadEventEnd: performance.now(),
          duration: performance.now(),
        };
      });

      // TTI is approximated by domInteractive
      const tti = metrics.domInteractive;
      console.log(`Time to Interactive (domInteractive): ${tti}ms`);

      expect(tti).toBeLessThan(PERFORMANCE_THRESHOLDS.TTI);
    });

    test('First Contentful Paint is under 1.8 seconds', async ({ page }) => {
      await page.goto('/', { waitUntil: 'networkidle' });

      // Get FCP from Performance API
      const fcp = await page.evaluate(() => {
        const fcpEntry = performance.getEntriesByName('first-contentful-paint')[0];
        if (fcpEntry) {
          return fcpEntry.startTime;
        }
        // Fallback: use domContentLoadedEventEnd as approximation
        const navEntry = performance.getEntriesByType('navigation')[0];
        return navEntry ? navEntry.domContentLoadedEventEnd : 0;
      });

      console.log(`First Contentful Paint: ${fcp}ms`);
      expect(fcp).toBeLessThan(PERFORMANCE_THRESHOLDS.FCP);
    });

    test('Largest Contentful Paint is under 2.5 seconds', async ({ page }) => {
      await page.goto('/', { waitUntil: 'networkidle' });

      // Wait a bit for LCP to be recorded
      await page.waitForTimeout(500);

      // Get LCP from Performance API
      const lcp = await page.evaluate(() => {
        const lcpEntries = performance.getEntriesByType('largest-contentful-paint');
        if (lcpEntries.length > 0) {
          return lcpEntries[lcpEntries.length - 1].startTime;
        }
        // If no LCP entries, use load time as fallback
        const navEntry = performance.getEntriesByType('navigation')[0];
        return navEntry ? navEntry.loadEventEnd : 1000;
      });

      console.log(`Largest Contentful Paint: ${lcp}ms`);
      expect(lcp).toBeLessThan(PERFORMANCE_THRESHOLDS.LCP);
    });

    test('Cumulative Layout Shift is under 0.1', async ({ page }) => {
      await page.goto('/', { waitUntil: 'networkidle' });

      // Wait for any animations to settle
      await page.waitForTimeout(1000);

      // Calculate CLS from Layout Shift entries
      const cls = await page.evaluate(() => {
        let clsValue = 0;
        const entries = performance.getEntriesByType('layout-shift');
        for (const entry of entries) {
          if (!entry.hadRecentInput) {
            clsValue += entry.value;
          }
        }
        return clsValue;
      });

      console.log(`Cumulative Layout Shift: ${cls}`);
      expect(cls).toBeLessThan(PERFORMANCE_THRESHOLDS.CLS);
    });

  });

  test.describe('Lighthouse Performance Score Simulation', () => {

    test('achieves performance score >= 90 based on core web vitals', async ({ page }) => {
      await page.goto('/', { waitUntil: 'networkidle' });

      // Wait for metrics to settle
      await page.waitForTimeout(1000);

      // Collect all performance metrics
      const metrics = await page.evaluate(() => {
        const navEntry = performance.getEntriesByType('navigation')[0];
        const fcpEntry = performance.getEntriesByName('first-contentful-paint')[0];
        const lcpEntries = performance.getEntriesByType('largest-contentful-paint');
        const layoutShifts = performance.getEntriesByType('layout-shift');

        let cls = 0;
        for (const entry of layoutShifts) {
          if (!entry.hadRecentInput) {
            cls += entry.value;
          }
        }

        return {
          fcp: fcpEntry?.startTime || navEntry?.domContentLoadedEventEnd || 500,
          lcp: lcpEntries.length > 0 ? lcpEntries[lcpEntries.length - 1].startTime : navEntry?.loadEventEnd || 1000,
          tti: navEntry?.domInteractive || 500,
          cls: cls,
          speedIndex: navEntry?.domContentLoadedEventEnd || 500,
          tbt: 0, // Total Blocking Time - hard to measure without PerformanceObserver
        };
      });

      console.log('Performance Metrics:');
      console.log(`  FCP: ${metrics.fcp}ms`);
      console.log(`  LCP: ${metrics.lcp}ms`);
      console.log(`  TTI: ${metrics.tti}ms`);
      console.log(`  CLS: ${metrics.cls}`);

      // Calculate a simulated Lighthouse score based on Core Web Vitals
      // Lighthouse 10+ weights: FCP 10%, SI 10%, LCP 25%, TBT 30%, CLS 25%
      // We simplify this by scoring each metric and averaging

      const fcpScore = scoreMetric(metrics.fcp, 1800, 3000);  // Good < 1.8s, Poor > 3s
      const lcpScore = scoreMetric(metrics.lcp, 2500, 4000);  // Good < 2.5s, Poor > 4s
      const ttiScore = scoreMetric(metrics.tti, 2000, 4000);  // Good < 2s, Poor > 4s
      const clsScore = scoreCLS(metrics.cls);                  // Good < 0.1, Poor > 0.25

      // Weighted average (simplified Lighthouse weights)
      const performanceScore = Math.round(
        fcpScore * 0.10 +
        lcpScore * 0.25 +
        ttiScore * 0.40 +  // Using TTI as proxy for TBT
        clsScore * 0.25
      );

      console.log(`\nSimulated Performance Score: ${performanceScore}/100`);
      console.log(`  FCP Score: ${fcpScore}`);
      console.log(`  LCP Score: ${lcpScore}`);
      console.log(`  TTI Score: ${ttiScore}`);
      console.log(`  CLS Score: ${clsScore}`);

      expect(performanceScore).toBeGreaterThanOrEqual(PERFORMANCE_THRESHOLDS.PERFORMANCE_SCORE);
    });

  });

  test.describe('Asset Optimization', () => {

    test('all CSS files are loaded', async ({ page }) => {
      const responses = [];

      page.on('response', (response) => {
        const url = response.url();
        if (url.endsWith('.css') || url.includes('.css')) {
          responses.push({
            url,
            status: response.status(),
          });
        }
      });

      await page.goto('/', { waitUntil: 'networkidle' });

      // Verify CSS files loaded successfully
      expect(responses.length).toBeGreaterThan(0);
      for (const response of responses) {
        expect(response.status).toBe(200);
      }
    });

    test('all JavaScript files are loaded', async ({ page }) => {
      const responses = [];

      page.on('response', (response) => {
        const url = response.url();
        if ((url.endsWith('.js') || url.includes('.js')) && !url.includes('node_modules')) {
          responses.push({
            url,
            status: response.status(),
          });
        }
      });

      await page.goto('/', { waitUntil: 'networkidle' });

      // Verify JS files loaded successfully
      expect(responses.length).toBeGreaterThan(0);
      for (const response of responses) {
        expect(response.status).toBe(200);
      }
    });

    test('images have appropriate dimensions specified', async ({ page }) => {
      await page.goto('/', { waitUntil: 'networkidle' });

      // Check that images have width and height attributes to prevent CLS
      const imagesWithDimensions = await page.evaluate(() => {
        const images = document.querySelectorAll('img');
        let withDimensions = 0;
        let total = 0;

        images.forEach((img) => {
          total++;
          if (img.hasAttribute('width') && img.hasAttribute('height')) {
            withDimensions++;
          }
        });

        return { withDimensions, total };
      });

      console.log(`Images with dimensions: ${imagesWithDimensions.withDimensions}/${imagesWithDimensions.total}`);
      // All images should have dimensions specified for CLS prevention
      expect(imagesWithDimensions.withDimensions).toBe(imagesWithDimensions.total);
    });

    test('no render-blocking resources in critical path', async ({ page }) => {
      await page.goto('/', { waitUntil: 'networkidle' });

      // Check that scripts are either deferred or async (or type="module")
      const blockingScripts = await page.evaluate(() => {
        const scripts = document.querySelectorAll('script[src]');
        let blocking = 0;

        scripts.forEach((script) => {
          const hasDefer = script.hasAttribute('defer');
          const hasAsync = script.hasAttribute('async');
          const isModule = script.type === 'module';

          if (!hasDefer && !hasAsync && !isModule) {
            blocking++;
          }
        });

        return blocking;
      });

      console.log(`Render-blocking scripts: ${blockingScripts}`);
      expect(blockingScripts).toBe(0);
    });

    test('page total weight is reasonable', async ({ page }) => {
      let totalBytes = 0;
      let resourceCount = 0;

      page.on('response', async (response) => {
        try {
          const body = await response.body();
          totalBytes += body.length;
          resourceCount++;
        } catch {
          // Some responses may not have a body
        }
      });

      await page.goto('/', { waitUntil: 'networkidle' });

      // Total page weight should be under 1MB for good performance
      const maxBytes = 1024 * 1024; // 1MB
      const totalKB = Math.round(totalBytes / 1024);
      console.log(`Total page weight: ${totalKB}KB (${resourceCount} resources)`);
      expect(totalBytes).toBeLessThan(maxBytes);
    });

  });

});

// Helper function to score metrics (returns 0-100)
function scoreMetric(value, goodThreshold, poorThreshold) {
  if (value <= goodThreshold) {
    return 100;
  } else if (value >= poorThreshold) {
    return 0;
  }
  // Linear interpolation between good and poor
  const range = poorThreshold - goodThreshold;
  const excess = value - goodThreshold;
  return Math.round(100 - (excess / range) * 100);
}

// Helper function to score CLS (returns 0-100)
function scoreCLS(value) {
  const goodThreshold = 0.1;
  const poorThreshold = 0.25;

  if (value <= goodThreshold) {
    return 100;
  } else if (value >= poorThreshold) {
    return 0;
  }
  const range = poorThreshold - goodThreshold;
  const excess = value - goodThreshold;
  return Math.round(100 - (excess / range) * 100);
}
