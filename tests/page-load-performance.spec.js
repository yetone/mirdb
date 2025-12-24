// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Page Load Performance E2E Tests (NFR-2)
 * Verifies the page loads within acceptable time limits
 * - Total page load time < 3 seconds
 * - First Contentful Paint (FCP) < 1.5 seconds
 */

test.describe('Page Load Performance', () => {
  /**
   * Test Case 1: Page fully loads within 3 seconds (E2E)
   * Measures total page load time using Navigation Timing API
   */
  test('TC1: Page fully loads within 3 seconds', async ({ page }) => {
    // Clear any potential caching by going to a blank page first
    await page.goto('about:blank');

    // Navigate to the page and wait for load event
    const startTime = Date.now();
    await page.goto('/', { waitUntil: 'load' });

    // Get detailed timing using Navigation Timing API
    const performanceTiming = await page.evaluate(() => {
      const timing = performance.getEntriesByType('navigation')[0];
      if (timing) {
        return {
          loadEventEnd: timing.loadEventEnd,
          startTime: timing.startTime,
          domContentLoadedEventEnd: timing.domContentLoadedEventEnd,
          responseEnd: timing.responseEnd
        };
      }
      // Fallback for older API
      const t = performance.timing;
      return {
        loadEventEnd: t.loadEventEnd - t.navigationStart,
        startTime: 0,
        domContentLoadedEventEnd: t.domContentLoadedEventEnd - t.navigationStart,
        responseEnd: t.responseEnd - t.navigationStart
      };
    });

    // Calculate total load time (loadEventEnd - startTime)
    const totalLoadTime = performanceTiming.loadEventEnd - performanceTiming.startTime;

    console.log(`Total page load time: ${totalLoadTime}ms`);
    console.log(`DOM Content Loaded: ${performanceTiming.domContentLoadedEventEnd}ms`);
    console.log(`Response End: ${performanceTiming.responseEnd}ms`);

    // Assert total load time is under 3 seconds (3000ms)
    expect(totalLoadTime).toBeLessThan(3000);
  });

  /**
   * Test Case 2: First Contentful Paint (FCP) is under 1.5 seconds (E2E)
   * Uses Paint Timing API to measure FCP
   */
  test('TC2: First Contentful Paint (FCP) is under 1.5 seconds', async ({ page }) => {
    // Clear browser state
    await page.goto('about:blank');

    // Navigate to the page
    await page.goto('/', { waitUntil: 'domcontentloaded' });

    // Wait for FCP to be recorded (give browser time to paint)
    await page.waitForTimeout(500);

    // Get FCP using Paint Timing API
    const fcpTime = await page.evaluate(() => {
      return new Promise((resolve) => {
        // Check if FCP is already available
        const entries = performance.getEntriesByType('paint');
        const fcpEntry = entries.find(entry => entry.name === 'first-contentful-paint');

        if (fcpEntry) {
          resolve(fcpEntry.startTime);
          return;
        }

        // If not available yet, use PerformanceObserver
        const observer = new PerformanceObserver((list) => {
          const fcpEntries = list.getEntriesByName('first-contentful-paint');
          if (fcpEntries.length > 0) {
            resolve(fcpEntries[0].startTime);
            observer.disconnect();
          }
        });

        observer.observe({ type: 'paint', buffered: true });

        // Timeout fallback - if no FCP recorded in 3 seconds, fail
        setTimeout(() => {
          const fallbackEntries = performance.getEntriesByType('paint');
          const fallbackFcp = fallbackEntries.find(e => e.name === 'first-contentful-paint');
          resolve(fallbackFcp ? fallbackFcp.startTime : null);
        }, 3000);
      });
    });

    console.log(`First Contentful Paint (FCP): ${fcpTime}ms`);

    // Assert FCP exists and is under 1.5 seconds (1500ms)
    expect(fcpTime).not.toBeNull();
    expect(fcpTime).toBeLessThan(1500);
  });

  /**
   * Test Case 3: Lighthouse performance audit - manual test placeholder
   * Note: Running actual Lighthouse requires additional setup
   * This test documents the manual testing requirement
   */
  test.skip('TC3: Lighthouse performance score is 90 or above (manual)', async ({ page }) => {
    // This test is marked as manual in the scenario
    // Actual Lighthouse testing requires:
    // 1. lighthouse npm package
    // 2. Running lighthouse CLI or programmatic API
    // 3. Real browser environment (not headless for accurate results)

    // Manual testing steps:
    // 1. Open Chrome DevTools
    // 2. Navigate to Lighthouse tab
    // 3. Select "Performance" category
    // 4. Run audit for Desktop
    // 5. Verify Performance score >= 90

    console.log('Manual Test: Run Lighthouse audit and verify Performance score >= 90');
    expect(true).toBe(true);
  });

  /**
   * Test Case 4: Total page size is under 500KB (excluding images) - Unit test
   * Validates the total size of HTML, CSS, JS, and fonts
   */
  test('TC4: Total page size is under 500KB (excluding images)', async ({ page }) => {
    // Track all network requests
    const resources = [];

    page.on('response', async (response) => {
      const url = response.url();
      const resourceType = response.request().resourceType();

      // Exclude images from the count
      if (resourceType === 'image' || resourceType === 'media') {
        return;
      }

      try {
        const headers = response.headers();
        const contentLength = headers['content-length'];
        const contentType = headers['content-type'] || '';

        // Get actual body size if content-length not available
        let size = 0;
        if (contentLength) {
          size = parseInt(contentLength, 10);
        } else {
          // Try to get body for size estimation
          try {
            const body = await response.body();
            size = body.length;
          } catch (e) {
            // Some responses may not have bodies
            size = 0;
          }
        }

        resources.push({
          url: url,
          type: resourceType,
          contentType: contentType,
          size: size
        });
      } catch (e) {
        // Ignore failed requests
      }
    });

    // Navigate to the page
    await page.goto('/', { waitUntil: 'networkidle' });

    // Calculate total size (excluding images)
    const totalSize = resources.reduce((sum, r) => sum + r.size, 0);
    const totalSizeKB = totalSize / 1024;

    console.log('\n=== Resource Breakdown (excluding images) ===');
    resources.forEach(r => {
      console.log(`${r.type}: ${r.url.substring(0, 80)}... - ${(r.size / 1024).toFixed(2)}KB`);
    });
    console.log(`\nTotal page size (excluding images): ${totalSizeKB.toFixed(2)}KB`);

    // Assert total size is under 500KB
    expect(totalSizeKB).toBeLessThan(500);
  });

  /**
   * Additional: Verify page reaches interactive state quickly
   */
  test('Page becomes interactive quickly', async ({ page }) => {
    await page.goto('about:blank');

    const startTime = Date.now();
    await page.goto('/', { waitUntil: 'domcontentloaded' });

    // Verify key interactive elements are present
    await expect(page.locator('nav')).toBeVisible();
    await expect(page.locator('#hero h1')).toBeVisible();
    await expect(page.locator('.cta-buttons')).toBeVisible();

    const interactiveTime = Date.now() - startTime;
    console.log(`Time to interactive elements visible: ${interactiveTime}ms`);

    // Interactive elements should be visible quickly (under 2 seconds)
    expect(interactiveTime).toBeLessThan(2000);
  });

  /**
   * Additional: Verify no render-blocking resources causing delays
   */
  test('Critical CSS is not excessively large', async ({ page }) => {
    await page.goto('/');

    // Get the inline CSS size
    const inlineCSSSize = await page.evaluate(() => {
      const styles = document.querySelectorAll('style');
      let totalSize = 0;
      styles.forEach(style => {
        totalSize += style.textContent?.length || 0;
      });
      return totalSize;
    });

    console.log(`Inline CSS size: ${(inlineCSSSize / 1024).toFixed(2)}KB`);

    // Inline CSS should be reasonable (under 50KB for critical CSS)
    expect(inlineCSSSize).toBeLessThan(50 * 1024);
  });
});
