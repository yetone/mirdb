// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Page Load Performance Tests
 *
 * These tests verify that the MirDB homepage meets NFR-2 performance requirements:
 * - Page load time under 3 seconds on standard connections
 * - First Contentful Paint (FCP) under 1.8 seconds
 * - Total page size under 1MB
 * - Lighthouse performance score of 90+
 * - Images use modern formats (WebP) with fallbacks
 */

test.describe('Page Load Performance', () => {
  test.describe('DOMContentLoaded Performance', () => {
    test('DOMContentLoaded fires in under 3 seconds', async ({ page }) => {
      // Navigate and wait for the page to fully load
      await page.goto('/', { waitUntil: 'load' });

      // Get timing metrics using Performance API
      const performanceMetrics = await page.evaluate(() => {
        const timing = performance.timing;
        const navigationStart = timing.navigationStart;
        return {
          domContentLoadedTime: timing.domContentLoadedEventEnd - navigationStart,
          loadEventTime: timing.loadEventEnd - navigationStart,
          responseEnd: timing.responseEnd - navigationStart,
        };
      });

      // DOMContentLoaded should be under 3000ms (3 seconds)
      expect(performanceMetrics.domContentLoadedTime).toBeLessThan(3000);

      console.log(`DOMContentLoaded: ${performanceMetrics.domContentLoadedTime}ms`);
      console.log(`Full Load: ${performanceMetrics.loadEventTime}ms`);
    });
  });

  test.describe('First Contentful Paint', () => {
    test('First Contentful Paint occurs in under 1.8 seconds', async ({ page }) => {
      await page.goto('/');

      // Wait for page to be fully loaded
      await page.waitForLoadState('load');

      // Get FCP using Performance Observer API
      const fcpMetric = await page.evaluate(() => {
        return new Promise((resolve) => {
          // Check if paint entries are already available
          const paintEntries = performance.getEntriesByType('paint');
          const fcpEntry = paintEntries.find(entry => entry.name === 'first-contentful-paint');

          if (fcpEntry) {
            resolve(fcpEntry.startTime);
          } else {
            // If not available, use a reasonable fallback based on DOM timing
            const timing = performance.timing;
            const domInteractive = timing.domInteractive - timing.navigationStart;
            resolve(domInteractive);
          }
        });
      });

      // FCP should be under 1800ms (1.8 seconds)
      expect(fcpMetric).toBeLessThan(1800);

      console.log(`First Contentful Paint: ${fcpMetric}ms`);
    });
  });

  test.describe('Total Page Size', () => {
    test('Total transfer size is under 1MB', async ({ page }) => {
      const resourceSizes = [];

      // Listen for all network responses
      page.on('response', async (response) => {
        try {
          const headers = response.headers();
          const contentLength = headers['content-length'];

          if (contentLength) {
            resourceSizes.push({
              url: response.url(),
              size: parseInt(contentLength, 10),
            });
          } else {
            // For responses without content-length, try to get body size
            try {
              const body = await response.body();
              resourceSizes.push({
                url: response.url(),
                size: body.length,
              });
            } catch {
              // Ignore errors for responses we can't read
            }
          }
        } catch {
          // Ignore errors
        }
      });

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Calculate total size
      const totalSize = resourceSizes.reduce((sum, resource) => sum + resource.size, 0);
      const totalSizeKB = totalSize / 1024;
      const totalSizeMB = totalSize / (1024 * 1024);

      console.log(`Total transfer size: ${totalSizeKB.toFixed(2)} KB (${totalSizeMB.toFixed(2)} MB)`);
      console.log('Resources loaded:');
      resourceSizes.forEach(r => {
        console.log(`  - ${r.url}: ${(r.size / 1024).toFixed(2)} KB`);
      });

      // Total size should be under 1MB (1,048,576 bytes)
      expect(totalSize).toBeLessThan(1048576);
    });
  });

  test.describe('Asset Optimization', () => {
    test('HTML file is reasonably sized', async ({ page }) => {
      let htmlSize = 0;

      page.on('response', async (response) => {
        if (response.url().endsWith('/') || response.url().endsWith('/index.html')) {
          try {
            const body = await response.body();
            htmlSize = body.length;
          } catch {
            // Ignore errors
          }
        }
      });

      await page.goto('/');
      await page.waitForLoadState('load');

      // HTML should be under 50KB (reasonable for a static homepage)
      expect(htmlSize).toBeLessThan(51200);
      console.log(`HTML size: ${(htmlSize / 1024).toFixed(2)} KB`);
    });

    test('CSS file is reasonably sized', async ({ page }) => {
      let cssSize = 0;

      page.on('response', async (response) => {
        if (response.url().includes('.css')) {
          try {
            const body = await response.body();
            cssSize += body.length;
          } catch {
            // Ignore errors
          }
        }
      });

      await page.goto('/');
      await page.waitForLoadState('load');

      // CSS should be under 50KB (reasonable for a static homepage)
      expect(cssSize).toBeLessThan(51200);
      console.log(`CSS size: ${(cssSize / 1024).toFixed(2)} KB`);
    });

    test('JavaScript bundle size is minimal', async ({ page }) => {
      let jsSize = 0;

      page.on('response', async (response) => {
        if (response.url().includes('.js') && !response.url().includes('playwright')) {
          try {
            const body = await response.body();
            jsSize += body.length;
          } catch {
            // Ignore errors
          }
        }
      });

      await page.goto('/');
      await page.waitForLoadState('load');

      // JS should be minimal for a static site (under 50KB)
      // Static sites shouldn't require much JS
      expect(jsSize).toBeLessThan(51200);
      console.log(`JavaScript size: ${(jsSize / 1024).toFixed(2)} KB`);
    });
  });

  test.describe('Image Optimization', () => {
    test('Images use modern formats (WebP) or are SVG', async ({ page }) => {
      const imageInfo = [];

      page.on('response', async (response) => {
        const url = response.url();
        const contentType = response.headers()['content-type'] || '';

        if (contentType.includes('image') ||
            url.match(/\.(png|jpg|jpeg|gif|webp|svg|avif)$/i)) {
          try {
            const body = await response.body();
            imageInfo.push({
              url,
              contentType,
              size: body.length,
              isOptimized: contentType.includes('webp') ||
                           contentType.includes('svg') ||
                           contentType.includes('avif') ||
                           url.includes('.webp') ||
                           url.includes('.svg') ||
                           url.includes('.avif'),
            });
          } catch {
            // Ignore errors
          }
        }
      });

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Check if any images are loaded
      if (imageInfo.length > 0) {
        console.log('Images loaded:');
        imageInfo.forEach(img => {
          console.log(`  - ${img.url}: ${img.contentType}, ${(img.size / 1024).toFixed(2)} KB, optimized: ${img.isOptimized}`);
        });

        // All images should be in optimized formats
        const allOptimized = imageInfo.every(img => img.isOptimized);
        expect(allOptimized).toBe(true);
      } else {
        // No external images - the page uses inline SVG which is optimal
        console.log('No external images loaded - page uses inline SVG (optimal)');

        // Verify that the architecture diagram is an inline SVG
        const hasSvgDiagram = await page.locator('svg.architecture-diagram').count();
        expect(hasSvgDiagram).toBeGreaterThan(0);
      }
    });

    test('No unnecessarily large images', async ({ page }) => {
      const largeImages = [];

      page.on('response', async (response) => {
        const contentType = response.headers()['content-type'] || '';

        if (contentType.includes('image')) {
          try {
            const body = await response.body();
            // Flag images over 200KB as potentially too large
            if (body.length > 204800) {
              largeImages.push({
                url: response.url(),
                size: body.length,
              });
            }
          } catch {
            // Ignore errors
          }
        }
      });

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      if (largeImages.length > 0) {
        console.log('Large images detected:');
        largeImages.forEach(img => {
          console.log(`  - ${img.url}: ${(img.size / 1024).toFixed(2)} KB`);
        });
      }

      // No images should be unnecessarily large (over 200KB)
      expect(largeImages.length).toBe(0);
    });
  });

  test.describe('Lighthouse Performance Metrics (Simulated)', () => {
    test('Performance metrics indicate Lighthouse score 90+ potential', async ({ page }) => {
      // Note: This is a simulated Lighthouse check. For actual Lighthouse audits,
      // manual testing or CI integration with lighthouse-ci is recommended.
      // This test validates key metrics that contribute to a high Lighthouse score.

      await page.goto('/');
      await page.waitForLoadState('load');

      // Get comprehensive performance metrics
      const metrics = await page.evaluate(() => {
        const timing = performance.timing;
        const navStart = timing.navigationStart;

        // Get paint entries
        const paintEntries = performance.getEntriesByType('paint');
        const fcpEntry = paintEntries.find(entry => entry.name === 'first-contentful-paint');

        // Get resource timing
        const resourceEntries = performance.getEntriesByType('resource');
        const totalResources = resourceEntries.length;
        const totalResourceDuration = resourceEntries.reduce((sum, entry) => sum + entry.duration, 0);

        return {
          // Core Web Vitals approximations
          fcp: fcpEntry ? fcpEntry.startTime : timing.domInteractive - navStart,
          domContentLoaded: timing.domContentLoadedEventEnd - navStart,
          loadComplete: timing.loadEventEnd - navStart,

          // Additional metrics
          ttfb: timing.responseStart - navStart, // Time to First Byte
          domInteractive: timing.domInteractive - navStart,

          // Resource metrics
          totalResources,
          avgResourceDuration: totalResources > 0 ? totalResourceDuration / totalResources : 0,
        };
      });

      console.log('Performance Metrics:');
      console.log(`  FCP: ${metrics.fcp.toFixed(0)}ms`);
      console.log(`  TTFB: ${metrics.ttfb.toFixed(0)}ms`);
      console.log(`  DOM Interactive: ${metrics.domInteractive.toFixed(0)}ms`);
      console.log(`  DOMContentLoaded: ${metrics.domContentLoaded.toFixed(0)}ms`);
      console.log(`  Load Complete: ${metrics.loadComplete.toFixed(0)}ms`);
      console.log(`  Total Resources: ${metrics.totalResources}`);
      console.log(`  Avg Resource Duration: ${metrics.avgResourceDuration.toFixed(0)}ms`);

      // Validate metrics that indicate a high Lighthouse score:
      // - FCP under 1.8s (good)
      // - TTFB under 600ms (good)
      // - DOM Interactive under 2s
      // - Load Complete under 3s

      expect(metrics.fcp).toBeLessThan(1800);
      expect(metrics.ttfb).toBeLessThan(600);
      expect(metrics.domInteractive).toBeLessThan(2000);
      expect(metrics.loadComplete).toBeLessThan(3000);

      // Estimate Lighthouse score based on metrics
      // This is a rough approximation - actual Lighthouse uses more complex calculations
      const estimatedScore = calculateEstimatedLighthouseScore(metrics);
      console.log(`  Estimated Lighthouse Score: ${estimatedScore}`);

      // We expect the score to be at least 90
      expect(estimatedScore).toBeGreaterThanOrEqual(90);
    });
  });

  test.describe('Resource Loading Optimization', () => {
    test('Critical resources load efficiently', async ({ page }) => {
      const resourceLoadOrder = [];

      page.on('response', (response) => {
        resourceLoadOrder.push({
          url: response.url(),
          status: response.status(),
          timing: Date.now(),
        });
      });

      const startTime = Date.now();
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');
      const domContentLoadedTime = Date.now() - startTime;

      console.log(`Resources loaded before DOMContentLoaded: ${resourceLoadOrder.length}`);
      console.log(`Time to DOMContentLoaded: ${domContentLoadedTime}ms`);

      // All critical resources (HTML, CSS) should load within 2 seconds
      expect(domContentLoadedTime).toBeLessThan(2000);
    });

    test('No render-blocking resources delay paint significantly', async ({ page }) => {
      await page.goto('/');

      // Check that the page is visible quickly
      const heroVisible = await page.locator('[data-testid="hero"]').isVisible();
      expect(heroVisible).toBe(true);

      // Verify there are no external blocking scripts
      const scriptTags = await page.locator('script:not([async]):not([defer])').count();
      // The page should have minimal or no blocking scripts
      console.log(`Blocking script tags: ${scriptTags}`);
      expect(scriptTags).toBeLessThanOrEqual(1);

      // Verify stylesheet is loaded
      const stylesheetLoaded = await page.evaluate(() => {
        const styles = document.styleSheets;
        return styles.length > 0;
      });
      expect(stylesheetLoaded).toBe(true);
    });
  });
});

/**
 * Calculate an estimated Lighthouse performance score based on key metrics.
 * This is a simplified approximation of the actual Lighthouse scoring algorithm.
 *
 * @param {Object} metrics - Performance metrics
 * @returns {number} Estimated Lighthouse score (0-100)
 */
function calculateEstimatedLighthouseScore(metrics) {
  // Lighthouse scoring thresholds (simplified)
  // FCP: Good < 1800ms, Needs Improvement < 3000ms
  // TTFB: Good < 600ms, Needs Improvement < 1800ms

  let score = 100;

  // FCP impact (weight: 10%)
  if (metrics.fcp > 1800) {
    score -= Math.min(15, (metrics.fcp - 1800) / 100);
  }

  // TTFB impact (weight: 10%)
  if (metrics.ttfb > 600) {
    score -= Math.min(10, (metrics.ttfb - 600) / 100);
  }

  // DOM Interactive impact (weight: 20%)
  if (metrics.domInteractive > 2000) {
    score -= Math.min(20, (metrics.domInteractive - 2000) / 100);
  }

  // Load Complete impact (weight: 15%)
  if (metrics.loadComplete > 3000) {
    score -= Math.min(20, (metrics.loadComplete - 3000) / 100);
  }

  // Bonus for fast loading (under thresholds)
  if (metrics.fcp < 1000) score += 2;
  if (metrics.ttfb < 300) score += 2;
  if (metrics.loadComplete < 1500) score += 3;

  return Math.max(0, Math.min(100, Math.round(score)));
}
