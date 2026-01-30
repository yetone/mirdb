/**
 * Performance Tests
 * Owner: Scenario 11 - Performance and Load Time
 *
 * Tests:
 * - Page load time
 * - Asset sizes
 * - Lighthouse score (simulated via performance metrics)
 * - Third-party scripts absence
 */

const { test, expect } = require('@playwright/test');
const { BASE_URL, waitForPageLoad } = require('./test-utils');

test.describe('Performance and Load Time', () => {
  test.describe('Page Load Performance', () => {
    test('page DOMContentLoaded in under 2 seconds on fast connection', async ({ page }) => {
      // Measure DOMContentLoaded timing
      const startTime = Date.now();

      await page.goto('/', { waitUntil: 'domcontentloaded' });

      const loadTime = Date.now() - startTime;

      // DOMContentLoaded should be under 2 seconds on fast connection
      expect(loadTime).toBeLessThan(2000);
    });

    test('page fully loads within acceptable time', async ({ page }) => {
      const startTime = Date.now();

      await page.goto('/', { waitUntil: 'load' });

      const loadTime = Date.now() - startTime;

      // Full page load should be under 3 seconds
      expect(loadTime).toBeLessThan(3000);
    });

    test('measures First Contentful Paint', async ({ page }) => {
      await page.goto('/');

      // Get performance metrics
      const fcp = await page.evaluate(() => {
        return new Promise((resolve) => {
          const observer = new PerformanceObserver((list) => {
            for (const entry of list.getEntries()) {
              if (entry.name === 'first-contentful-paint') {
                resolve(entry.startTime);
              }
            }
          });
          observer.observe({ type: 'paint', buffered: true });

          // Fallback if FCP already happened
          setTimeout(() => {
            const entries = performance.getEntriesByName('first-contentful-paint');
            if (entries.length > 0) {
              resolve(entries[0].startTime);
            } else {
              resolve(0);
            }
          }, 100);
        });
      });

      // FCP should be under 1.5 seconds for good performance
      expect(fcp).toBeLessThan(1500);
    });
  });

  test.describe('No Third-Party Tracking Scripts', () => {
    test('no analytics or tracking scripts present', async ({ page }) => {
      await page.goto('/');
      await waitForPageLoad(page);

      // Check for common tracking/analytics scripts
      const trackingPatterns = [
        'google-analytics',
        'googletagmanager',
        'gtag',
        'analytics.js',
        'ga.js',
        'fbq',
        'facebook.net',
        'hotjar',
        'mixpanel',
        'segment',
        'amplitude',
        'heap',
        'fullstory',
        'clarity',
        'mouseflow',
        'crazyegg',
        'optimizely',
        'adobedtm',
        'omtrdc',
      ];

      // Check all script sources
      const scripts = await page.$$eval('script[src]', (elements) =>
        elements.map((el) => el.src)
      );

      for (const pattern of trackingPatterns) {
        for (const src of scripts) {
          expect(src.toLowerCase()).not.toContain(pattern.toLowerCase());
        }
      }

      // Check inline scripts for tracking code
      const inlineScripts = await page.$$eval('script:not([src])', (elements) =>
        elements.map((el) => el.textContent || '')
      );

      const inlineTrackingPatterns = [
        'gtag(',
        'ga(',
        'fbq(',
        '_gaq',
        'GoogleAnalyticsObject',
        'dataLayer',
      ];

      for (const pattern of inlineTrackingPatterns) {
        for (const script of inlineScripts) {
          expect(script).not.toContain(pattern);
        }
      }
    });

    test('no external tracking pixels or beacons', async ({ page }) => {
      const trackingRequests = [];

      // Monitor network requests
      page.on('request', (request) => {
        const url = request.url().toLowerCase();
        if (
          url.includes('analytics') ||
          url.includes('tracking') ||
          url.includes('pixel') ||
          url.includes('beacon') ||
          url.includes('collect')
        ) {
          trackingRequests.push(url);
        }
      });

      await page.goto('/');
      await waitForPageLoad(page);

      // Allow CircleCI badge (legitimate external resource)
      const filteredRequests = trackingRequests.filter(
        (url) => !url.includes('circleci')
      );

      expect(filteredRequests).toHaveLength(0);
    });
  });

  test.describe('Total Page Weight', () => {
    test('total page size is under 1MB (excluding cached assets)', async ({ page }) => {
      let totalSize = 0;

      // Monitor all network requests
      page.on('response', async (response) => {
        try {
          const headers = response.headers();
          const contentLength = headers['content-length'];
          if (contentLength) {
            totalSize += parseInt(contentLength, 10);
          }
        } catch (e) {
          // Ignore errors for responses that can't be measured
        }
      });

      await page.goto('/');
      await waitForPageLoad(page);

      // Wait for all network activity to settle
      await page.waitForTimeout(500);

      // Total page size should be under 1MB (1,048,576 bytes)
      // Allow some margin for external badge image
      expect(totalSize).toBeLessThan(1048576);
    });

    test('HTML document size is reasonable', async ({ page }) => {
      await page.goto('/');

      const htmlSize = await page.evaluate(() => {
        return document.documentElement.outerHTML.length;
      });

      // HTML should be under 100KB
      expect(htmlSize).toBeLessThan(102400);
    });
  });

  test.describe('Performance Metrics (Lighthouse-style)', () => {
    test('page has good performance characteristics', async ({ page }) => {
      await page.goto('/');

      // Get performance timing data
      const timing = await page.evaluate(() => {
        const perf = performance.timing || performance.getEntriesByType('navigation')[0];

        if (performance.timing) {
          return {
            domContentLoaded: perf.domContentLoadedEventEnd - perf.navigationStart,
            domInteractive: perf.domInteractive - perf.navigationStart,
            loadComplete: perf.loadEventEnd - perf.navigationStart,
          };
        } else {
          // Navigation Timing Level 2
          const nav = perf;
          return {
            domContentLoaded: nav.domContentLoadedEventEnd,
            domInteractive: nav.domInteractive,
            loadComplete: nav.loadEventEnd,
          };
        }
      });

      // DOM should be interactive quickly
      expect(timing.domInteractive).toBeLessThan(2000);

      // DOMContentLoaded should fire quickly
      expect(timing.domContentLoaded).toBeLessThan(2000);
    });

    test('no render-blocking resources that cause excessive delay', async ({ page }) => {
      // Track blocking resources
      const blockingResources = [];

      page.on('response', async (response) => {
        const request = response.request();
        const resourceType = request.resourceType();
        const url = request.url();

        // External scripts can be render-blocking
        if (resourceType === 'script' && !url.includes('localhost')) {
          blockingResources.push({ type: 'script', url });
        }

        // External stylesheets can be render-blocking
        if (resourceType === 'stylesheet' && !url.includes('localhost')) {
          blockingResources.push({ type: 'stylesheet', url });
        }
      });

      await page.goto('/');
      await waitForPageLoad(page);

      // Should have no external render-blocking resources
      expect(blockingResources).toHaveLength(0);
    });

    test('images have proper dimensions specified', async ({ page }) => {
      await page.goto('/');
      await waitForPageLoad(page);

      // Check that images have width and height or CSS dimensions
      const imagesWithoutDimensions = await page.$$eval('img', (images) => {
        return images.filter((img) => {
          const hasHtmlDimensions = img.hasAttribute('width') && img.hasAttribute('height');
          const style = window.getComputedStyle(img);
          const hasCssDimensions =
            (style.width !== 'auto' && style.width !== '0px') ||
            (style.height !== 'auto' && style.height !== '0px');

          // Either HTML attributes or CSS dimensions should be present
          return !hasHtmlDimensions && !hasCssDimensions;
        }).map((img) => img.src);
      });

      // All images should have dimensions to prevent layout shift
      // Allow for badge images which are external
      const localImages = imagesWithoutDimensions.filter(
        (src) => src.includes('localhost') || src.startsWith('/')
      );

      expect(localImages).toHaveLength(0);
    });
  });

  test.describe('Resource Optimization', () => {
    test('CSS is loaded efficiently', async ({ page }) => {
      const cssFiles = [];

      page.on('response', async (response) => {
        if (response.request().resourceType() === 'stylesheet') {
          cssFiles.push(response.url());
        }
      });

      await page.goto('/');
      await waitForPageLoad(page);

      // Should have CSS files loaded
      expect(cssFiles.length).toBeGreaterThan(0);

      // All CSS should be local (no external dependencies)
      for (const cssUrl of cssFiles) {
        expect(cssUrl).toContain('localhost');
      }
    });

    test('JavaScript is loaded efficiently', async ({ page }) => {
      const jsFiles = [];

      page.on('response', async (response) => {
        if (response.request().resourceType() === 'script') {
          jsFiles.push({
            url: response.url(),
            size: response.headers()['content-length'] || 0,
          });
        }
      });

      await page.goto('/');
      await waitForPageLoad(page);

      // Should have local JS files
      const localJsFiles = jsFiles.filter((f) => f.url.includes('localhost'));
      expect(localJsFiles.length).toBeGreaterThan(0);

      // No external JS dependencies for tracking
      const externalJsFiles = jsFiles.filter((f) => !f.url.includes('localhost'));
      expect(externalJsFiles).toHaveLength(0);
    });
  });
});
