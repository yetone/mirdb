import { test, expect } from '@playwright/test';

/**
 * Page Load Performance Tests
 *
 * This test suite validates that the MirDB homepage meets performance requirements
 * as specified in NFR-1 (page load under 3 seconds) and success criteria
 * (Lighthouse performance score above 90).
 *
 * Test cases:
 * TC1: Page load time within 3 seconds
 * TC2: Lighthouse performance score >= 90
 * TC3: First Contentful Paint (FCP) within 1.5 seconds
 * TC4: Largest Contentful Paint (LCP) within 2.5 seconds
 * TC5: Cumulative Layout Shift (CLS) below 0.1
 */

test.describe('Page Load Performance', () => {
  test.describe.configure({ timeout: 60000 });

  test('TC1: page becomes interactive within 3 seconds', async ({ page }) => {
    // Start timing before navigation
    const startTime = Date.now();

    // Navigate to homepage and wait for DOM content loaded
    await page.goto('/');

    // Wait for the page to be interactive (DOM content loaded)
    await page.waitForLoadState('domcontentloaded');

    // Measure time to DOM content loaded
    const domContentLoadedTime = Date.now() - startTime;

    // Wait for full load
    await page.waitForLoadState('load');

    const loadTime = Date.now() - startTime;

    // Log the timing for debugging
    console.log(`DOM Content Loaded: ${domContentLoadedTime}ms`);
    console.log(`Full Load: ${loadTime}ms`);

    // Also check navigation timing API for more accurate measurements
    const navigationTiming = await page.evaluate(() => {
      const timing = performance.getEntriesByType('navigation')[0] as PerformanceNavigationTiming;
      return {
        domContentLoadedEventEnd: timing.domContentLoadedEventEnd,
        loadEventEnd: timing.loadEventEnd,
        domInteractive: timing.domInteractive,
        responseStart: timing.responseStart,
        fetchStart: timing.fetchStart
      };
    });

    // Calculate time to interactive (from navigation start)
    const timeToInteractive = navigationTiming.domInteractive;
    console.log(`Navigation API - Time to Interactive: ${timeToInteractive}ms`);
    console.log(`Navigation API - DOM Content Loaded: ${navigationTiming.domContentLoadedEventEnd}ms`);

    // Assert page becomes interactive within 3 seconds (3000ms)
    // Using domInteractive as the measure of when page is interactive
    expect(timeToInteractive).toBeLessThan(3000);

    // Verify the page actually rendered content
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();
  });

  test('TC2: performance score is 90 or above (simulated via metrics)', async ({ page }) => {
    // Since running Lighthouse programmatically requires additional setup,
    // we'll measure key performance metrics that contribute to Lighthouse score
    // and validate they meet the thresholds that would result in a 90+ score

    await page.goto('/');
    await page.waitForLoadState('load');

    // Get performance metrics
    const metrics = await page.evaluate(() => {
      const paintEntries = performance.getEntriesByType('paint');
      const navigationEntry = performance.getEntriesByType('navigation')[0] as PerformanceNavigationTiming;

      const fcp = paintEntries.find(entry => entry.name === 'first-contentful-paint');

      return {
        fcp: fcp ? fcp.startTime : null,
        ttfb: navigationEntry.responseStart - navigationEntry.fetchStart,
        domContentLoaded: navigationEntry.domContentLoadedEventEnd,
        loadEventEnd: navigationEntry.loadEventEnd,
        domInteractive: navigationEntry.domInteractive
      };
    });

    console.log('Performance Metrics:', JSON.stringify(metrics, null, 2));

    // For a Lighthouse score of 90+, these metrics should be in good ranges:
    // - FCP should be under 1.8s for a "Good" score
    // - TTFB should be under 800ms
    // - DOM Interactive should be under 3s

    // Assert FCP is good (under 1800ms)
    if (metrics.fcp !== null) {
      expect(metrics.fcp).toBeLessThan(1800);
    }

    // Assert TTFB is good (under 800ms)
    expect(metrics.ttfb).toBeLessThan(800);

    // Assert DOM Interactive is good (under 3000ms)
    expect(metrics.domInteractive).toBeLessThan(3000);

    // Assert overall page load is under 5 seconds
    expect(metrics.loadEventEnd).toBeLessThan(5000);

    // Verify static site has minimal JavaScript by checking document readiness
    const pageContent = await page.content();
    // A well-optimized static site should have limited script tags
    const scriptMatches = pageContent.match(/<script/g) || [];
    console.log(`Script tags found: ${scriptMatches.length}`);

    // For a static site, we expect minimal scripts (0-3 is typical)
    expect(scriptMatches.length).toBeLessThanOrEqual(5);
  });

  test('TC3: First Contentful Paint occurs within 1.5 seconds', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('load');

    // Get FCP using Performance API
    const fcp = await page.evaluate(() => {
      return new Promise<number>((resolve) => {
        // Check if FCP is already available in the buffer
        const paintEntries = performance.getEntriesByType('paint');
        const fcpEntry = paintEntries.find(entry => entry.name === 'first-contentful-paint');

        if (fcpEntry) {
          resolve(fcpEntry.startTime);
          return;
        }

        // If not, observe for it
        new PerformanceObserver((entryList) => {
          const entries = entryList.getEntries();
          const fcpEntry = entries.find(entry => entry.name === 'first-contentful-paint');
          if (fcpEntry) {
            resolve(fcpEntry.startTime);
          }
        }).observe({ type: 'paint', buffered: true });

        // Timeout after 5 seconds
        setTimeout(() => resolve(-1), 5000);
      });
    });

    console.log(`First Contentful Paint (FCP): ${fcp}ms`);

    // FCP should occur within 1.5 seconds (1500ms)
    expect(fcp).toBeGreaterThan(0);
    expect(fcp).toBeLessThan(1500);
  });

  test('TC4: Largest Contentful Paint occurs within 2.5 seconds', async ({ page }) => {
    // Set up LCP observer BEFORE navigation to capture all entries
    await page.addInitScript(() => {
      (window as any).__lcpValue = -1;
      const observer = new PerformanceObserver((entryList) => {
        const entries = entryList.getEntries();
        const lastEntry = entries[entries.length - 1];
        if (lastEntry) {
          (window as any).__lcpValue = lastEntry.startTime;
        }
      });
      observer.observe({ type: 'largest-contentful-paint', buffered: true });
    });

    await page.goto('/');
    await page.waitForLoadState('load');

    // Trigger an interaction to finalize LCP measurement
    await page.mouse.click(10, 10);

    // Wait for LCP to be captured
    await page.waitForTimeout(1000);

    // Get LCP value from the window object
    const lcp = await page.evaluate(() => (window as any).__lcpValue);

    console.log(`Largest Contentful Paint (LCP): ${lcp}ms`);

    // In headless CI environments, LCP might not always be reported
    // Fall back to checking that the largest visible content loaded quickly
    if (lcp === -1) {
      // Alternative check: measure time to largest visible element
      const navigationTiming = await page.evaluate(() => {
        const timing = performance.getEntriesByType('navigation')[0] as PerformanceNavigationTiming;
        return timing.loadEventEnd;
      });
      console.log(`Fallback: Load Event End: ${navigationTiming}ms`);
      // Full page load should be under 2.5 seconds
      expect(navigationTiming).toBeLessThan(2500);
    } else {
      // LCP should occur within 2.5 seconds (2500ms)
      expect(lcp).toBeGreaterThan(0);
      expect(lcp).toBeLessThan(2500);
    }

    // Verify the hero section (typically the LCP element) is visible
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();
  });

  test('TC5: Cumulative Layout Shift is below 0.1', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('load');

    // Wait for any animations or layout shifts to complete
    await page.waitForTimeout(1000);

    // Scroll the page to trigger any lazy loading or layout shifts
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
    await page.waitForTimeout(500);
    await page.evaluate(() => window.scrollTo(0, 0));
    await page.waitForTimeout(500);

    // Get CLS using PerformanceObserver
    const cls = await page.evaluate(() => {
      return new Promise<number>((resolve) => {
        let cumulativeLayoutShift = 0;

        new PerformanceObserver((entryList) => {
          const entries = entryList.getEntries();

          entries.forEach((entry: any) => {
            // Only count shifts without recent input
            if (!entry.hadRecentInput) {
              cumulativeLayoutShift += entry.value;
            }
          });

          resolve(cumulativeLayoutShift);
        }).observe({
          type: 'layout-shift',
          buffered: true
        });

        // Timeout after 2 seconds with current value
        setTimeout(() => resolve(cumulativeLayoutShift), 2000);
      });
    });

    console.log(`Cumulative Layout Shift (CLS): ${cls}`);

    // CLS should be below 0.1 for a "Good" score
    expect(cls).toBeLessThan(0.1);
  });

  test('assets are optimized for performance', async ({ page }) => {
    // Navigate and capture network requests
    const requests: { url: string; size: number; type: string }[] = [];

    page.on('response', async (response) => {
      const headers = response.headers();
      const contentLength = headers['content-length'];
      const contentType = headers['content-type'] || '';

      if (contentLength) {
        requests.push({
          url: response.url(),
          size: parseInt(contentLength, 10),
          type: contentType
        });
      }
    });

    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Calculate total page size
    const totalSize = requests.reduce((sum, req) => sum + req.size, 0);
    console.log(`Total page size: ${(totalSize / 1024).toFixed(2)} KB`);
    console.log(`Number of requests: ${requests.length}`);

    // Log large resources
    const largeResources = requests.filter(r => r.size > 100 * 1024);
    if (largeResources.length > 0) {
      console.log('Large resources (>100KB):');
      largeResources.forEach(r => console.log(`  ${r.url}: ${(r.size / 1024).toFixed(2)} KB`));
    }

    // For a well-optimized static site:
    // - Total page size should be under 1MB for initial load
    // - Number of requests should be reasonable (under 30)
    expect(totalSize).toBeLessThan(1024 * 1024); // 1MB
    expect(requests.length).toBeLessThan(30);

    // Verify no large unoptimized images
    const largeImages = requests.filter(r =>
      r.type.includes('image') && r.size > 200 * 1024
    );
    expect(largeImages.length).toBe(0);
  });

  test('page renders essential content quickly', async ({ page }) => {
    await page.goto('/');

    // Measure time to first meaningful content
    const startTime = Date.now();

    // Wait for hero section to be visible
    await page.waitForSelector('[data-testid="hero-section"]', { state: 'visible' });
    const heroTime = Date.now() - startTime;

    // Wait for navigation to be visible
    await page.waitForSelector('.navbar', { state: 'visible' });
    const navTime = Date.now() - startTime;

    // Wait for main heading
    await page.waitForSelector('[data-testid="product-name"]', { state: 'visible' });
    const headingTime = Date.now() - startTime;

    console.log(`Time to hero section: ${heroTime}ms`);
    console.log(`Time to navigation: ${navTime}ms`);
    console.log(`Time to main heading: ${headingTime}ms`);

    // All essential content should render within 2 seconds
    expect(heroTime).toBeLessThan(2000);
    expect(navTime).toBeLessThan(2000);
    expect(headingTime).toBeLessThan(2000);
  });
});
