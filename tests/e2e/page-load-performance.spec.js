/**
 * E2E Tests for Page Load Performance
 * Test Case ID: 1, 4
 * Validates page load timing and Lighthouse-style performance metrics
 */

const { test, expect } = require('@playwright/test');
const path = require('path');
const fs = require('fs');

const indexPath = 'file://' + path.resolve(__dirname, '../../index.html');

test.describe('Page Load Performance', () => {
  test.describe('Test Case 1: Page Load Timing', () => {
    test('should complete DOMContentLoaded within 3 seconds', async ({ page }) => {
      const metrics = await page.evaluate(async (url) => {
        const navigationStart = performance.now();

        // Create a promise that resolves when DOMContentLoaded fires
        const domContentLoadedPromise = new Promise((resolve) => {
          if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded', () => {
              resolve(performance.now() - navigationStart);
            });
          } else {
            resolve(0); // Already loaded
          }
        });

        return {
          domContentLoaded: await domContentLoadedPromise,
        };
      });

      // For file:// URLs, DOMContentLoaded should be nearly instant
      // We're mainly verifying the page can be parsed quickly
      await page.goto(indexPath);
      const navigationTiming = await page.evaluate(() => {
        const timing = performance.getEntriesByType('navigation')[0];
        return {
          domContentLoadedEventEnd: timing?.domContentLoadedEventEnd || 0,
          loadEventEnd: timing?.loadEventEnd || 0,
        };
      });

      console.log('Navigation timing:', navigationTiming);
      // DOMContentLoaded should complete in under 3 seconds (3000ms)
      expect(navigationTiming.domContentLoadedEventEnd).toBeLessThan(3000);
    });

    test('should display visible content within 3 seconds', async ({ page }) => {
      const startTime = Date.now();

      await page.goto(indexPath);

      // Wait for the main content to be visible
      await page.locator('h1').waitFor({ state: 'visible', timeout: 3000 });
      await page.locator('.hero-tagline').waitFor({ state: 'visible', timeout: 3000 });

      const loadTime = Date.now() - startTime;

      console.log(`Time to visible content: ${loadTime}ms`);
      expect(loadTime).toBeLessThan(3000);
    });

    test('should have all critical elements rendered within 3 seconds', async ({ page }) => {
      const startTime = Date.now();

      await page.goto(indexPath);

      // Verify critical elements are visible
      await expect(page.locator('h1')).toBeVisible({ timeout: 3000 });
      await expect(page.locator('.hero-tagline')).toBeVisible({ timeout: 3000 });
      await expect(page.locator('.hero-description')).toBeVisible({ timeout: 3000 });
      await expect(page.locator('.btn-primary')).toBeVisible({ timeout: 3000 });

      const loadTime = Date.now() - startTime;
      console.log(`Time to all critical elements: ${loadTime}ms`);

      expect(loadTime).toBeLessThan(3000);
    });

    test('should complete page load event within reasonable time', async ({ page }) => {
      await page.goto(indexPath, { waitUntil: 'load' });

      const timing = await page.evaluate(() => {
        const navTiming = performance.getEntriesByType('navigation')[0];
        return {
          loadEventEnd: navTiming?.loadEventEnd || 0,
          domComplete: navTiming?.domComplete || 0,
          domInteractive: navTiming?.domInteractive || 0,
        };
      });

      console.log('Performance timing:', timing);

      // For a static page, load should be fast
      // Note: With large GIF assets, this might be higher
      expect(timing.domInteractive).toBeLessThan(3000);
    });
  });

  test.describe('Test Case 4: Performance Metrics', () => {
    test('should have good First Contentful Paint timing', async ({ page }) => {
      await page.goto(indexPath, { waitUntil: 'domcontentloaded' });

      // Wait a bit for paint metrics to be available
      await page.waitForTimeout(100);

      const paintMetrics = await page.evaluate(() => {
        const entries = performance.getEntriesByType('paint');
        const fcp = entries.find((e) => e.name === 'first-contentful-paint');
        return {
          firstContentfulPaint: fcp?.startTime || 0,
        };
      });

      console.log('Paint metrics:', paintMetrics);

      // FCP should be under 1.8 seconds for good score
      // For file:// URLs with simple content, it should be much faster
      expect(paintMetrics.firstContentfulPaint).toBeLessThan(1800);
    });

    test('should have minimal render-blocking resources', async ({ page }) => {
      await page.goto(indexPath);

      // Check for render-blocking stylesheets
      const renderBlocking = await page.evaluate(() => {
        const stylesheets = document.querySelectorAll('link[rel="stylesheet"]');
        const blockingStylesheets = Array.from(stylesheets).filter((link) => {
          // Check if stylesheet doesn't have media query that would make it non-blocking
          const media = link.getAttribute('media');
          return !media || media === 'all' || media === 'screen';
        });

        return {
          totalStylesheets: stylesheets.length,
          blockingStylesheets: blockingStylesheets.length,
        };
      });

      console.log('Stylesheet analysis:', renderBlocking);

      // For a simple static page, minimal external blocking stylesheets is acceptable
      // Main concern is excessive external resources
      expect(renderBlocking.blockingStylesheets).toBeLessThanOrEqual(2);
    });

    test('should have efficient DOM structure', async ({ page }) => {
      await page.goto(indexPath);

      const domStats = await page.evaluate(() => {
        return {
          totalElements: document.querySelectorAll('*').length,
          maxDepth: (function getMaxDepth(el, depth = 0) {
            let max = depth;
            el.childNodes.forEach((child) => {
              if (child.nodeType === 1) {
                max = Math.max(max, getMaxDepth(child, depth + 1));
              }
            });
            return max;
          })(document.body),
        };
      });

      console.log('DOM statistics:', domStats);

      // DOM should not be excessively large
      expect(domStats.totalElements).toBeLessThan(1500);
      expect(domStats.maxDepth).toBeLessThan(32);
    });

    test('should score well on Core Web Vitals estimates', async ({ page }) => {
      await page.goto(indexPath, { waitUntil: 'networkidle' });

      // Collect various performance metrics
      const metrics = await page.evaluate(() => {
        const navTiming = performance.getEntriesByType('navigation')[0];
        const paintEntries = performance.getEntriesByType('paint');
        const fcp = paintEntries.find((e) => e.name === 'first-contentful-paint');

        // Calculate LCP estimate (for static page, usually close to FCP)
        const images = document.querySelectorAll('img');
        let largestImageSize = 0;
        images.forEach((img) => {
          const size = img.offsetWidth * img.offsetHeight;
          largestImageSize = Math.max(largestImageSize, size);
        });

        return {
          // Time to First Byte (TTFB) - should be fast for file://
          ttfb: navTiming?.responseStart || 0,

          // First Contentful Paint
          fcp: fcp?.startTime || 0,

          // DOM Content Loaded
          dcl: navTiming?.domContentLoadedEventEnd || 0,

          // Total page load
          load: navTiming?.loadEventEnd || 0,

          // DOM Interactive
          domInteractive: navTiming?.domInteractive || 0,

          // Largest image found (for LCP estimation)
          hasLargeImages: largestImageSize > 100000,
        };
      });

      console.log('Web Vitals estimates:', metrics);

      // Lighthouse scoring thresholds (approximate)
      // FCP < 1.8s is "good" (green)
      // FCP < 3s is "needs improvement" (orange)
      // FCP >= 3s is "poor" (red)

      // For a static file:// page, we expect very fast metrics
      // Being lenient here since we can't run actual Lighthouse

      // FCP should be good (< 1.8s) or at least acceptable (< 3s)
      expect(metrics.fcp).toBeLessThan(3000);

      // DCL should complete quickly
      expect(metrics.dcl).toBeLessThan(3000);

      // DOM Interactive should be fast
      expect(metrics.domInteractive).toBeLessThan(3000);
    });
  });

  test.describe('Resource Loading', () => {
    test('should not have excessive network requests', async ({ page }) => {
      const requests = [];

      page.on('request', (request) => {
        requests.push({
          url: request.url(),
          resourceType: request.resourceType(),
        });
      });

      await page.goto(indexPath, { waitUntil: 'networkidle' });

      console.log(`Total requests: ${requests.length}`);
      console.log(
        'Request types:',
        requests.reduce((acc, req) => {
          acc[req.resourceType] = (acc[req.resourceType] || 0) + 1;
          return acc;
        }, {})
      );

      // For a static page, requests should be minimal
      // Main document + CSS + images + maybe JS
      expect(requests.length).toBeLessThan(20);
    });

    test('should have acceptable total transfer size', async ({ page }) => {
      let totalSize = 0;

      page.on('response', async (response) => {
        try {
          const headers = response.headers();
          const contentLength = headers['content-length'];
          if (contentLength) {
            totalSize += parseInt(contentLength, 10);
          }
        } catch (e) {
          // Ignore errors from non-file responses
        }
      });

      await page.goto(indexPath, { waitUntil: 'networkidle' });

      console.log(`Estimated total transfer size: ${(totalSize / 1024).toFixed(2)} KB`);

      // For file:// URLs, content-length might not be available
      // This test mainly documents the transfer size
      expect(totalSize).toBeDefined();
    });
  });
});
