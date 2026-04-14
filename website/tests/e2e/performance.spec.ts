/**
 * Performance Tests
 * Owner: Scenario 11 - Performance Requirements
 *
 * Tests for:
 * - Page load time (under 2 seconds)
 * - First Contentful Paint (FCP) (under 1.8 seconds)
 * - Largest Contentful Paint (LCP) (under 2.5 seconds)
 * - Cumulative Layout Shift (CLS) (under 0.1)
 * - Total page size (under 1MB excluding video/large images)
 */

import { test, expect, Page } from '@playwright/test';

// Performance thresholds based on requirements
const THRESHOLDS = {
  PAGE_LOAD_TIME_MS: 2000,
  FCP_MS: 1800,
  LCP_MS: 2500,
  CLS: 0.1,
  TOTAL_PAGE_SIZE_BYTES: 1024 * 1024, // 1MB
};

// Interface for performance metrics
interface PerformanceMetrics {
  fcp: number | null;
  lcp: number | null;
  cls: number;
  loadTime: number;
}

// Helper function to collect Web Vitals metrics
async function collectWebVitals(page: Page): Promise<PerformanceMetrics> {
  return await page.evaluate(() => {
    return new Promise<{
      fcp: number | null;
      lcp: number | null;
      cls: number;
      loadTime: number;
    }>((resolve) => {
      const metrics: {
        fcp: number | null;
        lcp: number | null;
        cls: number;
        loadTime: number;
      } = {
        fcp: null,
        lcp: null,
        cls: 0,
        loadTime: 0,
      };

      // Get load timing from Navigation Timing API
      const navTiming = performance.getEntriesByType('navigation')[0] as PerformanceNavigationTiming;
      if (navTiming) {
        metrics.loadTime = navTiming.loadEventEnd - navTiming.startTime;
      }

      // Get paint timing for FCP
      const paintEntries = performance.getEntriesByType('paint');
      for (const entry of paintEntries) {
        if (entry.name === 'first-contentful-paint') {
          metrics.fcp = entry.startTime;
        }
      }

      // Set up observers for LCP and CLS
      let lcpResolved = false;
      let clsResolved = false;

      const lcpObserver = new PerformanceObserver((entryList) => {
        const entries = entryList.getEntries();
        const lastEntry = entries[entries.length - 1] as PerformancePaintTiming;
        if (lastEntry) {
          metrics.lcp = lastEntry.startTime;
        }
      });

      const clsObserver = new PerformanceObserver((entryList) => {
        for (const entry of entryList.getEntries()) {
          // @ts-ignore - value is available on layout-shift entries
          if (!entry.hadRecentInput) {
            // @ts-ignore
            metrics.cls += entry.value;
          }
        }
      });

      try {
        lcpObserver.observe({ type: 'largest-contentful-paint', buffered: true });
      } catch (e) {
        lcpResolved = true;
      }

      try {
        clsObserver.observe({ type: 'layout-shift', buffered: true });
      } catch (e) {
        clsResolved = true;
      }

      // Wait for metrics to stabilize, then resolve
      setTimeout(() => {
        lcpObserver.disconnect();
        clsObserver.disconnect();
        resolve(metrics);
      }, 1000);
    });
  });
}

// Helper function to get total page size from network requests
async function getTotalPageSize(page: Page): Promise<number> {
  // Get all resource timing entries
  const resourceSizes = await page.evaluate(() => {
    const entries = performance.getEntriesByType('resource') as PerformanceResourceTiming[];
    let totalSize = 0;

    for (const entry of entries) {
      // transferSize gives actual bytes transferred (0 if cached)
      // If transferSize is 0 but encodedBodySize > 0, it was cached
      const size = entry.transferSize || entry.encodedBodySize || 0;
      totalSize += size;
    }

    // Add document size
    const navEntry = performance.getEntriesByType('navigation')[0] as PerformanceNavigationTiming;
    if (navEntry) {
      totalSize += navEntry.transferSize || navEntry.encodedBodySize || 0;
    }

    return totalSize;
  });

  return resourceSizes;
}

test.describe('Performance Requirements', () => {
  test.describe.configure({ mode: 'serial' });

  test('page fully loads within 2 seconds on standard connection', async ({ page }) => {
    // Navigate to the page and wait for it to fully load
    const startTime = Date.now();
    await page.goto('/');
    await page.waitForLoadState('load');
    const endTime = Date.now();

    // Calculate actual load time
    const actualLoadTime = endTime - startTime;

    // Also verify using Navigation Timing API
    const navLoadTime = await page.evaluate(() => {
      const navTiming = performance.getEntriesByType('navigation')[0] as PerformanceNavigationTiming;
      if (navTiming) {
        return navTiming.loadEventEnd - navTiming.startTime;
      }
      return 0;
    });

    // Use the maximum of both measurements
    const loadTime = Math.max(actualLoadTime, navLoadTime);

    console.log(`Page load time: ${loadTime}ms (threshold: ${THRESHOLDS.PAGE_LOAD_TIME_MS}ms)`);
    console.log(`  - Measured externally: ${actualLoadTime}ms`);
    console.log(`  - Navigation Timing API: ${navLoadTime}ms`);

    expect(loadTime).toBeLessThanOrEqual(THRESHOLDS.PAGE_LOAD_TIME_MS);
  });

  test('First Contentful Paint (FCP) is under 1.8 seconds', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('load');

    // Wait a bit for paint metrics to be recorded
    await page.waitForTimeout(500);

    const fcp = await page.evaluate(() => {
      const paintEntries = performance.getEntriesByType('paint');
      for (const entry of paintEntries) {
        if (entry.name === 'first-contentful-paint') {
          return entry.startTime;
        }
      }
      return null;
    });

    console.log(`FCP: ${fcp}ms (threshold: ${THRESHOLDS.FCP_MS}ms)`);

    // FCP should exist and be under threshold
    expect(fcp).not.toBeNull();
    expect(fcp).toBeLessThanOrEqual(THRESHOLDS.FCP_MS);
  });

  test('Largest Contentful Paint (LCP) is under 2.5 seconds', async ({ page }) => {
    // Set up LCP tracking before navigation
    await page.goto('/');
    await page.waitForLoadState('load');

    // Wait for LCP to be recorded (typically occurs within 2.5s of page load)
    await page.waitForTimeout(1000);

    const lcp = await page.evaluate(() => {
      return new Promise<number | null>((resolve) => {
        let lcpValue: number | null = null;

        // Check existing LCP entries
        const observer = new PerformanceObserver((entryList) => {
          const entries = entryList.getEntries();
          const lastEntry = entries[entries.length - 1];
          if (lastEntry) {
            lcpValue = lastEntry.startTime;
          }
        });

        try {
          observer.observe({ type: 'largest-contentful-paint', buffered: true });
        } catch (e) {
          // LCP not supported
        }

        // Give it a moment to collect, then resolve
        setTimeout(() => {
          observer.disconnect();
          resolve(lcpValue);
        }, 200);
      });
    });

    console.log(`LCP: ${lcp}ms (threshold: ${THRESHOLDS.LCP_MS}ms)`);

    // LCP should exist and be under threshold
    expect(lcp).not.toBeNull();
    expect(lcp).toBeLessThanOrEqual(THRESHOLDS.LCP_MS);
  });

  test('Cumulative Layout Shift (CLS) is under 0.1', async ({ page }) => {
    // Navigate and wait for full load
    await page.goto('/');
    await page.waitForLoadState('load');

    // Wait for any potential layout shifts to occur
    await page.waitForTimeout(2000);

    // Scroll down to trigger any lazy-loaded content shifts
    await page.evaluate(() => {
      window.scrollTo(0, document.body.scrollHeight / 2);
    });
    await page.waitForTimeout(500);

    await page.evaluate(() => {
      window.scrollTo(0, document.body.scrollHeight);
    });
    await page.waitForTimeout(500);

    // Scroll back to top
    await page.evaluate(() => {
      window.scrollTo(0, 0);
    });
    await page.waitForTimeout(500);

    // Collect CLS
    const cls = await page.evaluate(() => {
      return new Promise<number>((resolve) => {
        let clsValue = 0;

        const observer = new PerformanceObserver((entryList) => {
          for (const entry of entryList.getEntries()) {
            // @ts-ignore - hadRecentInput is available on layout-shift entries
            if (!entry.hadRecentInput) {
              // @ts-ignore - value is available on layout-shift entries
              clsValue += entry.value;
            }
          }
        });

        try {
          observer.observe({ type: 'layout-shift', buffered: true });
        } catch (e) {
          // Layout shift observer not supported
        }

        setTimeout(() => {
          observer.disconnect();
          resolve(clsValue);
        }, 200);
      });
    });

    console.log(`CLS: ${cls} (threshold: ${THRESHOLDS.CLS})`);

    // CLS should be under threshold
    expect(cls).toBeLessThanOrEqual(THRESHOLDS.CLS);
  });

  test('total page size is under 1MB (excluding video/large images)', async ({ page }) => {
    // Track network requests
    const resourceSizes: { url: string; size: number; type: string }[] = [];

    page.on('response', async (response) => {
      try {
        const url = response.url();
        const headers = response.headers();
        const contentLength = parseInt(headers['content-length'] || '0', 10);
        const contentType = headers['content-type'] || '';

        // Skip videos and large images (as per requirements)
        const isVideo = contentType.startsWith('video/');
        const isLargeImage = contentType.startsWith('image/') && contentLength > 500 * 1024;

        if (!isVideo && !isLargeImage) {
          resourceSizes.push({
            url: url.split('?')[0], // Remove query params for cleaner logging
            size: contentLength,
            type: contentType.split(';')[0],
          });
        }
      } catch {
        // Ignore errors from getting body size
      }
    });

    await page.goto('/');
    await page.waitForLoadState('load');

    // Calculate total size
    const totalSize = resourceSizes.reduce((sum, r) => sum + r.size, 0);

    console.log(`Total page size: ${(totalSize / 1024).toFixed(2)}KB (threshold: ${THRESHOLDS.TOTAL_PAGE_SIZE_BYTES / 1024}KB)`);
    console.log('Resources loaded:');
    resourceSizes
      .filter((r) => r.size > 1024) // Only log resources > 1KB
      .sort((a, b) => b.size - a.size)
      .forEach((r) => {
        console.log(`  - ${r.type}: ${(r.size / 1024).toFixed(2)}KB - ${r.url.substring(0, 80)}`);
      });

    // Verify total size is under 1MB
    expect(totalSize).toBeLessThanOrEqual(THRESHOLDS.TOTAL_PAGE_SIZE_BYTES);
  });
});

test.describe('Performance - Comprehensive Metrics', () => {
  test('all Web Vitals are within acceptable ranges', async ({ page }) => {
    // Navigate and collect all metrics
    await page.goto('/');
    await page.waitForLoadState('load');
    await page.waitForTimeout(1500);

    const metrics = await collectWebVitals(page);

    console.log('Web Vitals Summary:');
    console.log(`  - Load Time: ${metrics.loadTime}ms (threshold: ${THRESHOLDS.PAGE_LOAD_TIME_MS}ms)`);
    console.log(`  - FCP: ${metrics.fcp}ms (threshold: ${THRESHOLDS.FCP_MS}ms)`);
    console.log(`  - LCP: ${metrics.lcp}ms (threshold: ${THRESHOLDS.LCP_MS}ms)`);
    console.log(`  - CLS: ${metrics.cls} (threshold: ${THRESHOLDS.CLS})`);

    // All metrics should be within thresholds
    expect(metrics.loadTime).toBeLessThanOrEqual(THRESHOLDS.PAGE_LOAD_TIME_MS);
    if (metrics.fcp !== null) {
      expect(metrics.fcp).toBeLessThanOrEqual(THRESHOLDS.FCP_MS);
    }
    if (metrics.lcp !== null) {
      expect(metrics.lcp).toBeLessThanOrEqual(THRESHOLDS.LCP_MS);
    }
    expect(metrics.cls).toBeLessThanOrEqual(THRESHOLDS.CLS);
  });
});

test.describe('Performance - Asset Optimization', () => {
  test('CSS files are optimized and not excessively large', async ({ page }) => {
    const cssFiles: { url: string; size: number }[] = [];

    page.on('response', async (response) => {
      const url = response.url();
      const contentType = response.headers()['content-type'] || '';
      if (contentType.includes('text/css') || url.endsWith('.css')) {
        const contentLength = parseInt(response.headers()['content-length'] || '0', 10);
        cssFiles.push({ url, size: contentLength });
      }
    });

    await page.goto('/');
    await page.waitForLoadState('load');

    const totalCssSize = cssFiles.reduce((sum, f) => sum + f.size, 0);
    console.log(`Total CSS size: ${(totalCssSize / 1024).toFixed(2)}KB`);

    // CSS should be reasonable (under 200KB total)
    expect(totalCssSize).toBeLessThan(200 * 1024);
  });

  test('JavaScript files are optimized and not excessively large', async ({ page }) => {
    const jsFiles: { url: string; size: number }[] = [];

    page.on('response', async (response) => {
      const url = response.url();
      const contentType = response.headers()['content-type'] || '';
      if (contentType.includes('javascript') || url.endsWith('.js')) {
        const contentLength = parseInt(response.headers()['content-length'] || '0', 10);
        jsFiles.push({ url, size: contentLength });
      }
    });

    await page.goto('/');
    await page.waitForLoadState('load');

    const totalJsSize = jsFiles.reduce((sum, f) => sum + f.size, 0);
    console.log(`Total JavaScript size: ${(totalJsSize / 1024).toFixed(2)}KB`);

    // JS should be reasonable (under 500KB total, including Mermaid)
    expect(totalJsSize).toBeLessThan(500 * 1024);
  });

  test('HTML document is not excessively large', async ({ page }) => {
    const response = await page.goto('/');
    const htmlSize = parseInt(response?.headers()['content-length'] || '0', 10);

    console.log(`HTML document size: ${(htmlSize / 1024).toFixed(2)}KB`);

    // HTML should be under 100KB
    expect(htmlSize).toBeLessThan(100 * 1024);
  });
});
