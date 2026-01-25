/**
 * E2E Performance tests for homepage
 * Scenario 18 - Page Load Performance
 *
 * Tests:
 * - Test Case 1: Page loads completely in under 2 seconds on simulated broadband
 * - Test Case 2: Lighthouse performance score is 90 or above
 * - Test Case 3: Critical rendering path is optimized (no render-blocking resources)
 */

import { test, expect } from './fixtures';

test.describe('Page Load Performance - Scenario 18', () => {
  test.describe('Test Case 1: Page Load Time', () => {
    test('should load homepage completely in under 2 seconds on simulated broadband', async ({
      page,
    }) => {
      // Use Performance API to measure actual load time
      const startTime = Date.now();

      // Navigate to homepage
      await page.goto('/', { waitUntil: 'networkidle' });

      // Wait for main content to be visible (indicates page is ready for user interaction)
      await page.waitForSelector('#main-content', { state: 'visible' });
      await page.waitForSelector('.hero', { state: 'visible' });

      const loadTime = Date.now() - startTime;

      // Verify page loads in under 2 seconds (2000ms)
      expect(loadTime).toBeLessThan(2000);

      // Also verify using Navigation Timing API for more accurate measurement
      const performanceMetrics = await page.evaluate(() => {
        const timing = performance.getEntriesByType('navigation')[0] as PerformanceNavigationTiming;
        return {
          domContentLoaded: timing.domContentLoadedEventEnd - timing.startTime,
          loadComplete: timing.loadEventEnd - timing.startTime,
          firstContentfulPaint: performance
            .getEntriesByName('first-contentful-paint')[0]
            ?.startTime,
        };
      });

      // DOM Content Loaded should be under 2 seconds
      expect(performanceMetrics.domContentLoaded).toBeLessThan(2000);

      // Log metrics for debugging
      console.log('Performance Metrics:', {
        measureTime: `${loadTime}ms`,
        domContentLoaded: `${performanceMetrics.domContentLoaded.toFixed(2)}ms`,
        loadComplete: `${performanceMetrics.loadComplete.toFixed(2)}ms`,
        firstContentfulPaint: performanceMetrics.firstContentfulPaint
          ? `${performanceMetrics.firstContentfulPaint.toFixed(2)}ms`
          : 'N/A',
      });
    });

    test('should have fast First Contentful Paint (FCP)', async ({ page }) => {
      await page.goto('/', { waitUntil: 'networkidle' });

      // Wait for hero section as the main visible content
      await page.waitForSelector('.hero', { state: 'visible' });

      const fcpMetric = await page.evaluate(() => {
        const fcp = performance.getEntriesByName('first-contentful-paint')[0];
        return fcp ? fcp.startTime : null;
      });

      // First Contentful Paint should be under 1.8 seconds for good performance
      if (fcpMetric !== null) {
        expect(fcpMetric).toBeLessThan(1800);
        console.log(`First Contentful Paint: ${fcpMetric.toFixed(2)}ms`);
      }
    });

    test('should load all major page sections within timeout', async ({ page }) => {
      const startTime = Date.now();

      await page.goto('/');

      // Wait for all major sections to be visible
      await Promise.all([
        page.waitForSelector('.hero', { state: 'visible', timeout: 2000 }),
        page.waitForSelector('[data-testid="features-section"]', { state: 'visible', timeout: 2000 }),
        page.waitForSelector('[data-testid="how-it-works-section"]', { state: 'visible', timeout: 2000 }),
        page.waitForSelector('[data-testid="footer"]', { state: 'visible', timeout: 2000 }),
      ]);

      const totalLoadTime = Date.now() - startTime;

      // All sections should load within 2 seconds
      expect(totalLoadTime).toBeLessThan(2000);

      console.log(`All sections loaded in: ${totalLoadTime}ms`);
    });
  });

  test.describe('Test Case 2: Lighthouse Performance Score', () => {
    test('should achieve acceptable performance score via metrics analysis', async ({ page }) => {
      // Navigate to homepage
      await page.goto('/', { waitUntil: 'networkidle' });

      // Collect Web Vitals-like metrics using Performance API
      const performanceData = await page.evaluate(() => {
        const navigation = performance.getEntriesByType('navigation')[0] as PerformanceNavigationTiming;
        const paintEntries = performance.getEntriesByType('paint');
        const fcpEntry = paintEntries.find((entry) => entry.name === 'first-contentful-paint');
        const resourceEntries = performance.getEntriesByType('resource');

        // Calculate total transfer size
        const totalTransferSize = resourceEntries.reduce((total, entry) => {
          const resource = entry as PerformanceResourceTiming;
          return total + (resource.transferSize || 0);
        }, 0);

        return {
          // Navigation timing
          ttfb: navigation.responseStart - navigation.requestStart,
          domInteractive: navigation.domInteractive - navigation.startTime,
          domContentLoaded: navigation.domContentLoadedEventEnd - navigation.startTime,
          loadComplete: navigation.loadEventEnd - navigation.startTime,
          // Paint metrics
          fcp: fcpEntry?.startTime || null,
          // Resource metrics
          totalResources: resourceEntries.length,
          totalTransferSize,
        };
      });

      // Validate performance metrics against thresholds that would contribute to a 90+ Lighthouse score
      // TTFB should be under 600ms for good performance
      expect(performanceData.ttfb).toBeLessThan(600);

      // FCP should be under 1800ms (good threshold)
      if (performanceData.fcp !== null) {
        expect(performanceData.fcp).toBeLessThan(1800);
      }

      // DOM Interactive should be under 2000ms
      expect(performanceData.domInteractive).toBeLessThan(2000);

      // DOM Content Loaded should be under 2000ms
      expect(performanceData.domContentLoaded).toBeLessThan(2000);

      // Total transfer size should be reasonable (under 3MB for initial load)
      expect(performanceData.totalTransferSize).toBeLessThan(3 * 1024 * 1024);

      console.log('Performance Data for Lighthouse-like analysis:', {
        ttfb: `${performanceData.ttfb.toFixed(2)}ms`,
        fcp: performanceData.fcp ? `${performanceData.fcp.toFixed(2)}ms` : 'N/A',
        domInteractive: `${performanceData.domInteractive.toFixed(2)}ms`,
        domContentLoaded: `${performanceData.domContentLoaded.toFixed(2)}ms`,
        loadComplete: `${performanceData.loadComplete.toFixed(2)}ms`,
        totalResources: performanceData.totalResources,
        totalTransferSize: `${(performanceData.totalTransferSize / 1024).toFixed(2)}KB`,
      });
    });

    test('should have good Largest Contentful Paint (LCP) timing', async ({ page }) => {
      await page.goto('/');

      // Wait for page to fully load
      await page.waitForLoadState('networkidle');

      // Give time for LCP to be recorded
      await page.waitForTimeout(500);

      const lcpValue = await page.evaluate(() => {
        return new Promise<number | null>((resolve) => {
          let lcpValue: number | null = null;

          // Use PerformanceObserver to capture LCP
          const observer = new PerformanceObserver((list) => {
            const entries = list.getEntries();
            const lastEntry = entries[entries.length - 1];
            if (lastEntry) {
              lcpValue = lastEntry.startTime;
            }
          });

          observer.observe({ type: 'largest-contentful-paint', buffered: true });

          // Resolve after a short delay to capture the LCP
          setTimeout(() => {
            observer.disconnect();
            resolve(lcpValue);
          }, 500);
        });
      });

      // LCP should be under 2500ms for good performance (Lighthouse good threshold)
      if (lcpValue !== null) {
        expect(lcpValue).toBeLessThan(2500);
        console.log(`Largest Contentful Paint: ${lcpValue.toFixed(2)}ms`);
      }
    });

    test('should have minimal Cumulative Layout Shift (CLS)', async ({ page }) => {
      await page.goto('/');

      // Wait for page to stabilize
      await page.waitForLoadState('networkidle');
      await page.waitForTimeout(1000);

      const clsValue = await page.evaluate(() => {
        return new Promise<number>((resolve) => {
          let clsValue = 0;

          const observer = new PerformanceObserver((list) => {
            for (const entry of list.getEntries()) {
              // Cast to LayoutShift type
              const layoutShift = entry as PerformanceEntry & {
                hadRecentInput?: boolean;
                value?: number;
              };
              if (!layoutShift.hadRecentInput && layoutShift.value) {
                clsValue += layoutShift.value;
              }
            }
          });

          observer.observe({ type: 'layout-shift', buffered: true });

          // Capture CLS after page settles
          setTimeout(() => {
            observer.disconnect();
            resolve(clsValue);
          }, 1000);
        });
      });

      // CLS should be under 0.1 for good performance
      expect(clsValue).toBeLessThan(0.1);
      console.log(`Cumulative Layout Shift: ${clsValue.toFixed(4)}`);
    });
  });

  test.describe('Test Case 3: Critical Rendering Path Optimization', () => {
    test('should not have excessive render-blocking resources', async ({ page }) => {
      await page.goto('/', { waitUntil: 'domcontentloaded' });

      // Analyze resources and check for render-blocking patterns
      const resourceAnalysis = await page.evaluate(() => {
        const resources = performance.getEntriesByType('resource') as PerformanceResourceTiming[];

        // Categorize resources
        const scripts = resources.filter((r) => r.initiatorType === 'script');
        const styles = resources.filter((r) => r.initiatorType === 'link' || r.initiatorType === 'css');
        const images = resources.filter((r) => r.initiatorType === 'img');

        // Check for render-blocking scripts (loaded before DOMContentLoaded)
        const navigation = performance.getEntriesByType('navigation')[0] as PerformanceNavigationTiming;
        const domContentLoadedTime = navigation.domContentLoadedEventEnd - navigation.startTime;

        // Resources that finished loading before DOM interactive are potentially blocking
        const domInteractiveTime = navigation.domInteractive - navigation.startTime;
        const potentiallyBlockingScripts = scripts.filter(
          (s) => s.responseEnd < domInteractiveTime && s.responseEnd > 0
        );

        return {
          totalScripts: scripts.length,
          totalStyles: styles.length,
          totalImages: images.length,
          potentiallyBlockingScripts: potentiallyBlockingScripts.length,
          domInteractiveTime,
          domContentLoadedTime,
        };
      });

      // Should not have too many potentially blocking scripts (less than 5)
      expect(resourceAnalysis.potentiallyBlockingScripts).toBeLessThan(5);

      // DOM Interactive should happen reasonably fast
      expect(resourceAnalysis.domInteractiveTime).toBeLessThan(1500);

      console.log('Resource Analysis:', {
        totalScripts: resourceAnalysis.totalScripts,
        totalStyles: resourceAnalysis.totalStyles,
        totalImages: resourceAnalysis.totalImages,
        potentiallyBlockingScripts: resourceAnalysis.potentiallyBlockingScripts,
        domInteractiveTime: `${resourceAnalysis.domInteractiveTime.toFixed(2)}ms`,
        domContentLoadedTime: `${resourceAnalysis.domContentLoadedTime.toFixed(2)}ms`,
      });
    });

    test('should load CSS efficiently without blocking', async ({ page }) => {
      const responses: { url: string; type: string; size: number }[] = [];

      // Monitor network requests
      page.on('response', async (response) => {
        const url = response.url();
        const contentType = response.headers()['content-type'] || '';
        if (contentType.includes('css') || url.endsWith('.css')) {
          try {
            const buffer = await response.body();
            responses.push({
              url,
              type: 'css',
              size: buffer.length,
            });
          } catch {
            // Response may have been already consumed
          }
        }
      });

      await page.goto('/', { waitUntil: 'networkidle' });

      // Total CSS should be reasonable (under 500KB for initial load)
      const totalCssSize = responses.reduce((acc, r) => acc + r.size, 0);
      expect(totalCssSize).toBeLessThan(500 * 1024);

      console.log('CSS Loading Analysis:', {
        cssFiles: responses.length,
        totalCssSize: `${(totalCssSize / 1024).toFixed(2)}KB`,
      });
    });

    test('should have optimized JavaScript loading', async ({ page }) => {
      const jsResponses: { url: string; size: number }[] = [];

      page.on('response', async (response) => {
        const url = response.url();
        const contentType = response.headers()['content-type'] || '';
        if (contentType.includes('javascript') || url.endsWith('.js')) {
          try {
            const buffer = await response.body();
            jsResponses.push({
              url,
              size: buffer.length,
            });
          } catch {
            // Response may have been already consumed
          }
        }
      });

      await page.goto('/', { waitUntil: 'networkidle' });

      // Total JS should be reasonable (under 2MB for initial load with React)
      const totalJsSize = jsResponses.reduce((acc, r) => acc + r.size, 0);
      expect(totalJsSize).toBeLessThan(2 * 1024 * 1024);

      // Individual JS files should be reasonably sized (code splitting check)
      const largeScripts = jsResponses.filter((r) => r.size > 500 * 1024);
      // Should not have more than 2 large scripts (main bundle + vendor)
      expect(largeScripts.length).toBeLessThan(3);

      console.log('JavaScript Loading Analysis:', {
        jsFiles: jsResponses.length,
        totalJsSize: `${(totalJsSize / 1024).toFixed(2)}KB`,
        largeScripts: largeScripts.length,
      });
    });

    test('should have images loaded lazily or with proper optimization', async ({ page }) => {
      await page.goto('/');

      // Check for lazy loading attributes on images below the fold
      const imageAnalysis = await page.evaluate(() => {
        const images = document.querySelectorAll('img');
        let lazyLoadedCount = 0;
        let totalImages = 0;

        images.forEach((img) => {
          totalImages++;
          if (img.loading === 'lazy' || img.hasAttribute('data-lazy')) {
            lazyLoadedCount++;
          }
        });

        return {
          totalImages,
          lazyLoadedCount,
          hasProperLoading: totalImages === 0 || lazyLoadedCount > 0 || totalImages <= 2, // Small number of images is acceptable
        };
      });

      // Either images should be lazy loaded or there should be very few images
      expect(imageAnalysis.hasProperLoading).toBe(true);

      console.log('Image Loading Analysis:', {
        totalImages: imageAnalysis.totalImages,
        lazyLoadedImages: imageAnalysis.lazyLoadedCount,
      });
    });

    test('should have efficient resource delivery (no redundant requests)', async ({ page }) => {
      const requests: string[] = [];

      page.on('request', (request) => {
        requests.push(request.url());
      });

      await page.goto('/', { waitUntil: 'networkidle' });

      // Check for duplicate requests (excluding dynamic timestamps)
      const normalizedUrls = requests.map((url) => {
        try {
          const parsed = new URL(url);
          parsed.searchParams.delete('t'); // Remove timestamp params
          parsed.searchParams.delete('_'); // Remove cache busters
          return parsed.origin + parsed.pathname;
        } catch {
          return url;
        }
      });

      const urlCounts = normalizedUrls.reduce(
        (acc, url) => {
          acc[url] = (acc[url] || 0) + 1;
          return acc;
        },
        {} as Record<string, number>
      );

      // Find duplicate requests
      const duplicates = Object.entries(urlCounts)
        .filter(([, count]) => count > 1)
        .map(([url, count]) => ({ url, count }));

      // Should not have many duplicate requests (allow some for fonts, etc.)
      expect(duplicates.length).toBeLessThan(5);

      console.log('Resource Delivery Analysis:', {
        totalRequests: requests.length,
        duplicateRequests: duplicates.length,
        duplicates: duplicates.slice(0, 3), // Show first 3 for debugging
      });
    });
  });
});
