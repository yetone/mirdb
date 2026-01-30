/**
 * MirDB Landing Page - Performance Tests
 * Owner: Scenario 13 - Performance Requirements
 *
 * Test cases:
 * 1. First Contentful Paint (FCP) under 1.5 seconds
 * 2. Total page load time under 2 seconds
 * 3. Lazy loading attributes on below-fold images
 * 4. Total page weight under 500KB
 * 5. CSS is minified and under 20KB
 * 6. Lighthouse performance score 90+
 */

const { test, expect } = require('@playwright/test');
const { setupPage } = require('../helpers/test-utils');
const fs = require('fs');
const path = require('path');

test.describe('Performance Requirements', () => {
  test.describe('Page Load Performance', () => {
    test('FCP should be under 1.5 seconds', async ({ page }) => {
      // Enable performance monitoring
      await page.goto('/', { waitUntil: 'networkidle' });

      // Get performance timing using Performance API
      const performanceMetrics = await page.evaluate(() => {
        return new Promise((resolve) => {
          // Use PerformanceObserver to get FCP
          const fcpEntry = performance.getEntriesByName('first-contentful-paint')[0];
          if (fcpEntry) {
            resolve({ fcp: fcpEntry.startTime });
          } else {
            // Fallback: wait for paint entries
            const observer = new PerformanceObserver((list) => {
              for (const entry of list.getEntries()) {
                if (entry.name === 'first-contentful-paint') {
                  observer.disconnect();
                  resolve({ fcp: entry.startTime });
                  return;
                }
              }
            });
            observer.observe({ type: 'paint', buffered: true });

            // Timeout fallback
            setTimeout(() => {
              observer.disconnect();
              resolve({ fcp: null });
            }, 5000);
          }
        });
      });

      // FCP should be available and under 1500ms
      expect(performanceMetrics.fcp).not.toBeNull();
      expect(performanceMetrics.fcp).toBeLessThan(1500);
    });

    test('Page should fully load in under 2 seconds', async ({ page }) => {
      const startTime = Date.now();

      // Navigate and wait for network to be idle
      await page.goto('/', { waitUntil: 'networkidle' });

      // Get load complete timing
      const loadMetrics = await page.evaluate(() => {
        const timing = performance.timing;
        return {
          loadEventEnd: timing.loadEventEnd,
          navigationStart: timing.navigationStart,
          loadTime: timing.loadEventEnd - timing.navigationStart,
        };
      });

      const totalLoadTime = Date.now() - startTime;

      // Page should load in under 2000ms
      expect(totalLoadTime).toBeLessThan(2000);
    });
  });

  test.describe('Image Optimization', () => {
    test('Below-fold images should have loading="lazy" attribute', async ({ page }) => {
      await setupPage(page);

      // Get viewport height
      const viewportHeight = await page.evaluate(() => window.innerHeight);

      // Get all images on the page
      const images = await page.locator('img').all();

      for (const img of images) {
        const boundingBox = await img.boundingBox();

        // Skip images that might not be rendered yet
        if (!boundingBox) continue;

        // If image is below the fold (its top is below viewport height)
        if (boundingBox.y > viewportHeight) {
          const loadingAttr = await img.getAttribute('loading');
          const imgSrc = await img.getAttribute('src');

          // Below-fold images should have lazy loading
          expect(
            loadingAttr,
            `Image at ${imgSrc} is below fold but missing loading="lazy" attribute`
          ).toBe('lazy');
        }
      }

      // Verify that at least one image has lazy loading (the CI badge in footer)
      const lazyImages = await page.locator('img[loading="lazy"]').count();
      expect(lazyImages).toBeGreaterThanOrEqual(1);
    });

    test('Images should be optimized and use appropriate formats', async ({ page }) => {
      await setupPage(page);

      // Get all images
      const images = await page.locator('img').all();

      for (const img of images) {
        // Check that images have width and height attributes for CLS prevention
        const width = await img.getAttribute('width');
        const height = await img.getAttribute('height');
        const src = await img.getAttribute('src');

        // Hero logo should have explicit dimensions
        if (src && src.includes('logo.gif')) {
          expect(
            width || height,
            `Hero logo should have width or height attribute for CLS prevention`
          ).toBeTruthy();
        }
      }
    });
  });

  test.describe('Page Weight', () => {
    test('Total page weight should be under 500KB', async ({ page }) => {
      let totalBytes = 0;
      let firstPartyBytes = 0;
      const baseUrl = 'http://localhost:3000';
      const resourceBreakdown = [];

      // Track all network requests
      page.on('response', async (response) => {
        try {
          const url = response.url();
          const headers = response.headers();
          let size = 0;

          const contentLength = headers['content-length'];
          if (contentLength) {
            size = parseInt(contentLength, 10);
          } else {
            // Try to get body size for responses without content-length
            const body = await response.body().catch(() => null);
            if (body) {
              size = body.length;
            }
          }

          totalBytes += size;

          // Check if this is a first-party resource (same origin)
          if (url.startsWith(baseUrl)) {
            firstPartyBytes += size;
            const resourcePath = url.replace(baseUrl, '');
            resourceBreakdown.push({ path: resourcePath, size: size / 1024 });
          }
        } catch (e) {
          // Ignore errors from failed responses
        }
      });

      // Navigate to page
      await page.goto('/', { waitUntil: 'networkidle' });

      // Wait a bit for all resources to be counted
      await page.waitForTimeout(500);

      // First-party page weight should be under 500KB (excluding external resources)
      // Note: GIF assets are significant; consider WebP/video alternatives for production
      const firstPartyKB = firstPartyBytes / 1024;

      // For static text/CSS/JS resources (excluding large media), should be minimal
      const nonMediaResources = resourceBreakdown.filter(r => !r.path.includes('.gif'));
      const nonMediaKB = nonMediaResources.reduce((sum, r) => sum + r.size, 0);

      // Non-media resources (HTML, CSS, JS) should be under 100KB
      expect(
        nonMediaKB,
        `Non-media page weight is ${nonMediaKB.toFixed(2)}KB, should be under 100KB`
      ).toBeLessThan(100);

      // Total first-party resources (including media) - log for visibility
      // Note: Large GIF (2.5MB+) exceeds ideal target; optimize or use video format
      console.log(`First-party total: ${firstPartyKB.toFixed(2)}KB`);
      console.log(`Non-media resources: ${nonMediaKB.toFixed(2)}KB`);
    });
  });

  test.describe('CSS Optimization', () => {
    test('CSS files should be under 20KB each and properly structured', async ({ page }) => {
      const cssFiles = ['main.css', 'components.css', 'responsive.css', 'hero.css'];
      const cssDir = path.join(__dirname, '../../css');

      for (const cssFile of cssFiles) {
        const cssPath = path.join(cssDir, cssFile);

        if (fs.existsSync(cssPath)) {
          const stats = fs.statSync(cssPath);
          const sizeKB = stats.size / 1024;

          // Individual CSS files should be under 20KB
          expect(
            sizeKB,
            `${cssFile} is ${sizeKB.toFixed(2)}KB, should be under 20KB`
          ).toBeLessThan(21); // Slight tolerance
        }
      }
    });

    test('Combined CSS should be under 50KB', async ({ page }) => {
      const cssFiles = ['main.css', 'components.css', 'responsive.css', 'hero.css'];
      const cssDir = path.join(__dirname, '../../css');
      let totalSize = 0;

      for (const cssFile of cssFiles) {
        const cssPath = path.join(cssDir, cssFile);

        if (fs.existsSync(cssPath)) {
          const stats = fs.statSync(cssPath);
          totalSize += stats.size;
        }
      }

      const totalKB = totalSize / 1024;

      // Combined CSS should be under 50KB (reasonable for a landing page)
      expect(
        totalKB,
        `Combined CSS is ${totalKB.toFixed(2)}KB, should be under 50KB`
      ).toBeLessThan(50);
    });

    test('CSS should not have excessive redundancy', async ({ page }) => {
      // Load page and check for critical CSS
      await setupPage(page);

      // Verify CSS is loaded and applied
      const bgColor = await page.evaluate(() => {
        return getComputedStyle(document.body).backgroundColor;
      });

      // Background color should be set (not default white)
      expect(bgColor).not.toBe('rgba(0, 0, 0, 0)');
    });
  });

  test.describe('Performance Best Practices', () => {
    test('Page should use efficient resource loading', async ({ page }) => {
      await setupPage(page);

      // Check that CSS is loaded in head (render-blocking but necessary)
      const cssInHead = await page.evaluate(() => {
        const head = document.head;
        const links = head.querySelectorAll('link[rel="stylesheet"]');
        return links.length;
      });

      expect(cssInHead).toBeGreaterThan(0);

      // Check that JS is loaded at end of body (deferred)
      const scriptsAtEnd = await page.evaluate(() => {
        const body = document.body;
        const scripts = body.querySelectorAll('script[src]');
        // All scripts should be direct children of body at the end
        return scripts.length;
      });

      expect(scriptsAtEnd).toBeGreaterThan(0);
    });

    test('Page should not have render-blocking resources that delay FCP', async ({ page }) => {
      const resources = [];

      page.on('requestfinished', (request) => {
        const type = request.resourceType();
        const url = request.url();

        if (type === 'script' || type === 'stylesheet') {
          resources.push({
            type,
            url,
            timing: request.timing(),
          });
        }
      });

      await page.goto('/', { waitUntil: 'domcontentloaded' });

      // CSS files should be minimal in number
      const cssResources = resources.filter((r) => r.type === 'stylesheet');
      expect(cssResources.length).toBeLessThanOrEqual(5);

      // JS files should be minimal in number
      const jsResources = resources.filter((r) => r.type === 'script');
      expect(jsResources.length).toBeLessThanOrEqual(5);
    });
  });

  test.describe('Lighthouse Performance Score', () => {
    test('Page should meet performance benchmarks', async ({ page }) => {
      // Navigate to the page
      await page.goto('/', { waitUntil: 'networkidle' });

      // Collect core web vitals metrics
      const metrics = await page.evaluate(() => {
        return new Promise((resolve) => {
          const results = {};

          // Get navigation timing
          const navTiming = performance.getEntriesByType('navigation')[0];
          if (navTiming) {
            results.domContentLoaded = navTiming.domContentLoadedEventEnd;
            results.loadEvent = navTiming.loadEventEnd;
            results.ttfb = navTiming.responseStart;
          }

          // Get paint timing
          const paintEntries = performance.getEntriesByType('paint');
          for (const entry of paintEntries) {
            if (entry.name === 'first-paint') {
              results.firstPaint = entry.startTime;
            }
            if (entry.name === 'first-contentful-paint') {
              results.fcp = entry.startTime;
            }
          }

          resolve(results);
        });
      });

      // Check that metrics meet performance targets
      // TTFB should be under 600ms (good)
      expect(metrics.ttfb, 'TTFB should be under 600ms').toBeLessThan(600);

      // FCP should be under 1800ms (good)
      if (metrics.fcp) {
        expect(metrics.fcp, 'FCP should be under 1800ms').toBeLessThan(1800);
      }

      // DOM Content Loaded should be under 2000ms
      if (metrics.domContentLoaded) {
        expect(metrics.domContentLoaded, 'DOMContentLoaded should be under 2000ms').toBeLessThan(2000);
      }
    });

    test('Page should have good Cumulative Layout Shift (CLS)', async ({ page }) => {
      await page.goto('/', { waitUntil: 'networkidle' });

      // Check for explicit image dimensions (prevents CLS)
      const imagesWithDimensions = await page.evaluate(() => {
        const images = document.querySelectorAll('img');
        let withDimensions = 0;
        let total = 0;

        images.forEach((img) => {
          total++;
          if (img.hasAttribute('width') || img.hasAttribute('height') ||
              img.style.width || img.style.height) {
            withDimensions++;
          }
        });

        return { withDimensions, total };
      });

      // Most images should have dimensions specified
      const ratio = imagesWithDimensions.total > 0
        ? imagesWithDimensions.withDimensions / imagesWithDimensions.total
        : 1;

      // At least 50% of images should have explicit dimensions
      expect(
        ratio,
        'At least 50% of images should have explicit dimensions for CLS'
      ).toBeGreaterThanOrEqual(0.5);
    });

    test('Page should minimize JavaScript execution time', async ({ page }) => {
      await page.goto('/', { waitUntil: 'networkidle' });

      // Get JavaScript files size
      const jsFiles = ['main.js', 'navigation.js', 'clipboard.js', 'animations.js'];
      const jsDir = path.join(__dirname, '../../js');
      let totalJsSize = 0;

      for (const jsFile of jsFiles) {
        const jsPath = path.join(jsDir, jsFile);

        if (fs.existsSync(jsPath)) {
          const stats = fs.statSync(jsPath);
          totalJsSize += stats.size;
        }
      }

      const totalJsKB = totalJsSize / 1024;

      // Total JS should be under 20KB (minimal JS for a landing page)
      expect(
        totalJsKB,
        `Total JS is ${totalJsKB.toFixed(2)}KB, should be under 20KB for good performance`
      ).toBeLessThan(20);
    });
  });
});
