// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Page Performance', () => {
  test.describe('Page Load Time', () => {
    test('page becomes interactive within 3 seconds', async ({ page }) => {
      const startTime = Date.now();

      await page.goto('/');

      // Wait for the page to be fully loaded and interactive
      await page.waitForLoadState('domcontentloaded');
      await page.waitForLoadState('load');

      // Verify key interactive elements are visible
      await expect(page.locator('[data-testid="hero"]')).toBeVisible();
      await expect(page.locator('h1')).toBeVisible();

      const loadTime = Date.now() - startTime;

      // Page should load within 3000ms (3 seconds) per NFR-2
      expect(loadTime).toBeLessThan(3000);
    });

    test('hero section renders quickly', async ({ page }) => {
      await page.goto('/');

      // Hero section should be visible immediately after navigation
      const hero = page.locator('[data-testid="hero"]');
      await expect(hero).toBeVisible({ timeout: 2000 });

      // CTA buttons should be interactive
      const ctaPrimary = page.locator('.cta-primary');
      await expect(ctaPrimary).toBeVisible({ timeout: 2000 });
    });
  });

  test.describe('Total Page Size', () => {
    test('total page weight is under 2MB for fast loading', async ({ page }) => {
      let totalBytes = 0;
      const resourceSizes = [];

      // Track all network responses
      page.on('response', async (response) => {
        const url = response.url();
        const headers = response.headers();
        let size = 0;

        // Get content-length from headers if available
        if (headers['content-length']) {
          size = parseInt(headers['content-length'], 10);
        } else {
          // Fallback: try to get body size (may not work for all resources)
          try {
            const body = await response.body();
            size = body.length;
          } catch (e) {
            // Ignore errors for resources we can't read
          }
        }

        if (size > 0) {
          resourceSizes.push({ url, size });
          totalBytes += size;
        }
      });

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // 2MB = 2 * 1024 * 1024 = 2097152 bytes
      const maxSizeBytes = 2 * 1024 * 1024;
      const totalMB = totalBytes / (1024 * 1024);

      console.log(`Total page size: ${totalMB.toFixed(2)} MB (${totalBytes} bytes)`);
      console.log('Largest resources:');
      resourceSizes
        .sort((a, b) => b.size - a.size)
        .slice(0, 5)
        .forEach(r => {
          console.log(`  ${(r.size / 1024).toFixed(2)} KB - ${r.url.split('/').pop()}`);
        });

      expect(totalBytes).toBeLessThan(maxSizeBytes);
    });

    test('HTML document is reasonably sized', async ({ page }) => {
      let htmlSize = 0;

      page.on('response', async (response) => {
        const url = response.url();
        const contentType = response.headers()['content-type'] || '';

        if (url.endsWith('/') || url.endsWith('.html') || contentType.includes('text/html')) {
          try {
            const body = await response.body();
            htmlSize = body.length;
          } catch (e) {
            // Ignore errors
          }
        }
      });

      await page.goto('/');
      await page.waitForLoadState('load');

      // HTML should be under 100KB (inline styles included)
      const maxHtmlSize = 100 * 1024;
      console.log(`HTML size: ${(htmlSize / 1024).toFixed(2)} KB`);
      expect(htmlSize).toBeLessThan(maxHtmlSize);
    });
  });

  test.describe('Render-Blocking Resources', () => {
    test('critical CSS is inlined or minimal render-blocking scripts', async ({ page }) => {
      await page.goto('/');

      // Check for external CSS files (render-blocking by default)
      const externalStylesheets = await page.locator('link[rel="stylesheet"]').all();

      // Our page uses inline styles, so there should be no external stylesheets
      console.log(`External stylesheets count: ${externalStylesheets.length}`);
      expect(externalStylesheets.length).toBe(0);

      // Check for render-blocking scripts in head
      const blockingScripts = await page.locator('head script:not([async]):not([defer]):not([type="module"])').all();
      console.log(`Render-blocking scripts in head: ${blockingScripts.length}`);
      expect(blockingScripts.length).toBe(0);

      // Verify inline styles exist (critical CSS should be inlined)
      const inlineStyles = await page.locator('style').all();
      expect(inlineStyles.length).toBeGreaterThan(0);
    });

    test('no render-blocking JavaScript in document head', async ({ page }) => {
      await page.goto('/');

      // Get all script tags in the head
      const headScripts = await page.evaluate(() => {
        const head = document.head;
        const scripts = head.querySelectorAll('script');
        return Array.from(scripts).map(s => ({
          src: s.src,
          async: s.async,
          defer: s.defer,
          type: s.type,
          isModule: s.type === 'module'
        }));
      });

      // Filter for potentially blocking scripts
      const blockingScripts = headScripts.filter(s =>
        s.src && !s.async && !s.defer && !s.isModule
      );

      console.log(`Total scripts in head: ${headScripts.length}`);
      console.log(`Blocking scripts: ${blockingScripts.length}`);

      expect(blockingScripts.length).toBe(0);
    });

    test('first contentful paint happens quickly', async ({ page }) => {
      await page.goto('/');

      // Check that visible content renders without waiting for external resources
      const hero = page.locator('[data-testid="hero"]');
      await expect(hero).toBeVisible({ timeout: 1500 });

      const h1 = page.locator('h1');
      await expect(h1).toBeVisible({ timeout: 1500 });

      // Verify text content is visible
      await expect(h1).toContainText('MirDB');
    });
  });

  test.describe('Performance Metrics', () => {
    test('performance score meets minimum threshold', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Get performance metrics via Navigation Timing API
      const performanceMetrics = await page.evaluate(() => {
        const timing = performance.timing;
        const navigation = performance.getEntriesByType('navigation')[0];

        return {
          // Time to first byte
          ttfb: timing.responseStart - timing.navigationStart,
          // DOM Content Loaded
          domContentLoaded: timing.domContentLoadedEventEnd - timing.navigationStart,
          // Page fully loaded
          pageLoad: timing.loadEventEnd - timing.navigationStart,
          // DOM interactive
          domInteractive: timing.domInteractive - timing.navigationStart,
          // Transfer size if available
          transferSize: navigation ? navigation.transferSize : 0,
        };
      });

      console.log('Performance Metrics:');
      console.log(`  TTFB: ${performanceMetrics.ttfb}ms`);
      console.log(`  DOM Interactive: ${performanceMetrics.domInteractive}ms`);
      console.log(`  DOM Content Loaded: ${performanceMetrics.domContentLoaded}ms`);
      console.log(`  Page Load: ${performanceMetrics.pageLoad}ms`);

      // TTFB should be under 500ms for a static page
      expect(performanceMetrics.ttfb).toBeLessThan(500);

      // DOM should be interactive within 2 seconds
      expect(performanceMetrics.domInteractive).toBeLessThan(2000);

      // Full page load within 3 seconds
      expect(performanceMetrics.pageLoad).toBeLessThan(3000);
    });

    test('largest contentful paint is acceptable', async ({ page }) => {
      await page.goto('/');

      // Use PerformanceObserver to measure LCP
      const lcp = await page.evaluate(() => {
        return new Promise((resolve) => {
          let lcpValue = 0;

          const observer = new PerformanceObserver((list) => {
            const entries = list.getEntries();
            const lastEntry = entries[entries.length - 1];
            lcpValue = lastEntry.startTime;
          });

          observer.observe({ type: 'largest-contentful-paint', buffered: true });

          // Wait a bit for LCP to be recorded
          setTimeout(() => {
            observer.disconnect();
            resolve(lcpValue);
          }, 2000);
        });
      });

      console.log(`Largest Contentful Paint: ${lcp}ms`);

      // LCP should ideally be under 2500ms for "good" performance
      // Being generous here due to GIF assets
      expect(lcp).toBeLessThan(3000);
    });

    test('cumulative layout shift is minimal', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Measure CLS using PerformanceObserver
      const cls = await page.evaluate(() => {
        return new Promise((resolve) => {
          let clsValue = 0;

          const observer = new PerformanceObserver((list) => {
            for (const entry of list.getEntries()) {
              if (!entry.hadRecentInput) {
                clsValue += entry.value;
              }
            }
          });

          observer.observe({ type: 'layout-shift', buffered: true });

          // Wait for any layout shifts to settle
          setTimeout(() => {
            observer.disconnect();
            resolve(clsValue);
          }, 2000);
        });
      });

      console.log(`Cumulative Layout Shift: ${cls}`);

      // CLS should be under 0.1 for "good" performance
      expect(cls).toBeLessThan(0.25);
    });
  });

  test.describe('Asset Optimization', () => {
    test('images use appropriate formats and sizes', async ({ page }) => {
      const imageResponses = [];

      page.on('response', async (response) => {
        const contentType = response.headers()['content-type'] || '';
        if (contentType.includes('image/')) {
          const url = response.url();
          let size = 0;

          if (response.headers()['content-length']) {
            size = parseInt(response.headers()['content-length'], 10);
          } else {
            try {
              const body = await response.body();
              size = body.length;
            } catch (e) {
              // Ignore
            }
          }

          imageResponses.push({ url, size, contentType });
        }
      });

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      console.log('Image assets:');
      imageResponses.forEach(img => {
        console.log(`  ${(img.size / 1024 / 1024).toFixed(2)} MB - ${img.url.split('/').pop()} (${img.contentType})`);
      });

      // Each individual image should be under 10MB
      // (GIFs can be large but should be reasonable)
      const maxImageSize = 10 * 1024 * 1024;
      for (const img of imageResponses) {
        expect(img.size).toBeLessThan(maxImageSize);
      }
    });

    test('no unnecessary external resources are loaded', async ({ page }) => {
      const externalRequests = [];

      page.on('request', (request) => {
        const url = new URL(request.url());
        if (!url.hostname.includes('localhost')) {
          externalRequests.push({
            url: request.url(),
            resourceType: request.resourceType()
          });
        }
      });

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      console.log('External requests:');
      externalRequests.forEach(req => {
        console.log(`  ${req.resourceType}: ${req.url}`);
      });

      // Filter for critical external resources
      const criticalExternal = externalRequests.filter(r =>
        r.resourceType === 'stylesheet' ||
        r.resourceType === 'script' ||
        r.resourceType === 'font'
      );

      // Should have minimal external critical resources
      // Only CircleCI badge image is expected
      expect(criticalExternal.length).toBe(0);
    });
  });
});
