import { test, expect, chromium, Page } from '@playwright/test';
import { playAudit } from 'playwright-lighthouse';

/**
 * E2E Performance Tests for Page Load Time
 * Scenario: Verify that the website loads within acceptable time limits
 * NFR-2: Page load time must be under 3 seconds
 *
 * IMPORTANT: These tests should be run against a production build for accurate metrics.
 * Use: npm run test:performance (uses playwright.performance.config.ts)
 *
 * In development mode, Astro includes a dev toolbar that adds significant JavaScript,
 * which skews the performance metrics.
 */

// Helper to get navigation timing metrics
async function getNavigationTiming(page: Page): Promise<PerformanceNavigationTiming> {
  const navigationTimingJson = await page.evaluate(() =>
    JSON.stringify(performance.getEntriesByType('navigation')[0])
  );
  return JSON.parse(navigationTimingJson) as PerformanceNavigationTiming;
}

// Helper to get total page size from resources
async function getTotalPageSize(page: Page): Promise<{ totalSize: number; jsSize: number; resources: Array<{ name: string; size: number; type: string }> }> {
  const resourceTimingJson = await page.evaluate(() => {
    const resources = performance.getEntriesByType('resource') as PerformanceResourceTiming[];
    return JSON.stringify(resources.map(r => ({
      name: r.name,
      size: r.transferSize,
      type: r.initiatorType,
      encodedSize: r.encodedBodySize,
      decodedSize: r.decodedBodySize
    })));
  });

  const resources = JSON.parse(resourceTimingJson) as Array<{ name: string; size: number; type: string; encodedSize: number; decodedSize: number }>;

  // Calculate total size and JS size
  let totalSize = 0;
  let jsSize = 0;

  for (const resource of resources) {
    const size = resource.size || resource.encodedSize || 0;
    totalSize += size;

    if (resource.name.endsWith('.js') || resource.name.includes('.js?') || resource.type === 'script') {
      jsSize += size;
    }
  }

  // Also get document size
  const documentSizeJson = await page.evaluate(() => {
    const navEntries = performance.getEntriesByType('navigation') as PerformanceNavigationTiming[];
    if (navEntries.length > 0) {
      return JSON.stringify({
        size: navEntries[0].transferSize || navEntries[0].encodedBodySize || 0
      });
    }
    return JSON.stringify({ size: 0 });
  });

  const documentSize = JSON.parse(documentSizeJson) as { size: number };
  totalSize += documentSize.size;

  return {
    totalSize,
    jsSize,
    resources: resources.map(r => ({ name: r.name, size: r.size || r.encodedSize || 0, type: r.type }))
  };
}

// Helper to get Largest Contentful Paint
async function getLCP(page: Page): Promise<number> {
  const lcp = await page.evaluate(() => {
    return new Promise<number>((resolve) => {
      let lcpValue = 0;

      const observer = new PerformanceObserver((list) => {
        const entries = list.getEntries();
        const lastEntry = entries[entries.length - 1];
        if (lastEntry) {
          lcpValue = lastEntry.startTime;
        }
      });

      observer.observe({ type: 'largest-contentful-paint', buffered: true });

      // Wait a bit for LCP to be recorded, then resolve
      setTimeout(() => {
        observer.disconnect();
        resolve(lcpValue);
      }, 2000);
    });
  });

  return lcp;
}

test.describe('Performance - Page Load Time', () => {
  /**
   * Test Case 1: Page becomes interactive in under 3 seconds
   * Input: Load homepage on standard broadband connection
   * Expected: Page becomes interactive in under 3 seconds
   * NFR-2: Page load time must be under 3 seconds
   */
  test('TC1: Homepage should become interactive in under 3 seconds', async ({ page }) => {
    // Start timing
    const startTime = Date.now();

    // Navigate to homepage
    await page.goto('/', { waitUntil: 'domcontentloaded' });

    // Wait for interactive state (page is usable)
    await page.waitForLoadState('networkidle');

    const endTime = Date.now();
    const loadTime = endTime - startTime;

    // Get detailed navigation timing
    const navTiming = await getNavigationTiming(page);

    // domInteractive marks when the document has finished parsing
    const domInteractiveTime = navTiming.domInteractive - navTiming.startTime;

    // domContentLoadedEventEnd marks when DOMContentLoaded event completes
    const domContentLoadedTime = navTiming.domContentLoadedEventEnd - navTiming.startTime;

    // Full page load time
    const fullLoadTime = navTiming.loadEventEnd - navTiming.startTime;

    console.log(`Performance Metrics:`);
    console.log(`  - DOM Interactive: ${domInteractiveTime.toFixed(2)}ms`);
    console.log(`  - DOMContentLoaded: ${domContentLoadedTime.toFixed(2)}ms`);
    console.log(`  - Full Load: ${fullLoadTime.toFixed(2)}ms`);
    console.log(`  - Measured Load Time: ${loadTime}ms`);

    // Get LCP metric
    const lcp = await getLCP(page);
    console.log(`  - Largest Contentful Paint (LCP): ${lcp.toFixed(2)}ms`);

    // Assert that the page becomes interactive in under 3 seconds
    // Using domContentLoadedEventEnd as the measure of "interactive"
    expect(domContentLoadedTime).toBeLessThan(3000);

    // LCP should also be under 3 seconds for good user experience
    expect(lcp).toBeLessThan(3000);
  });

  /**
   * Test Case 2: Lighthouse performance score is 90 or higher
   * Input: Run Lighthouse performance audit
   * Expected: Lighthouse performance score is 90 or higher
   * Requirement: Design Specification - All pages pass Lighthouse performance score of 90+
   */
  test('TC2: Lighthouse performance score should be 90 or higher', async () => {
    // Use a unique port to avoid conflicts with other browser instances
    const debugPort = 9223;

    // Launch browser with remote debugging port for Lighthouse
    const browser = await chromium.launch({
      args: [`--remote-debugging-port=${debugPort}`],
    });

    const page = await browser.newPage();

    try {
      // Navigate to homepage
      await page.goto('http://localhost:4321/', { waitUntil: 'networkidle' });

      // Run Lighthouse audit with performance threshold of 90
      const result = await playAudit({
        page: page,
        port: debugPort,
        thresholds: {
          performance: 90,
        },
      });

      // The playAudit function already throws if thresholds are not met
      // If we get here, the performance threshold was met
      const performanceScore = result.lhr.categories.performance.score * 100;

      console.log('Lighthouse Audit Results:');
      console.log(`  - Performance Score: ${performanceScore}`);

      // Safely log other category scores if available
      if (result.lhr.categories.accessibility?.score !== undefined) {
        console.log(`  - Accessibility Score: ${result.lhr.categories.accessibility.score * 100}`);
      }
      if (result.lhr.categories['best-practices']?.score !== undefined) {
        console.log(`  - Best Practices Score: ${result.lhr.categories['best-practices'].score * 100}`);
      }
      if (result.lhr.categories.seo?.score !== undefined) {
        console.log(`  - SEO Score: ${result.lhr.categories.seo.score * 100}`);
      }

      // Log specific performance metrics (safely)
      const metrics = result.lhr.audits;
      console.log('Performance Metrics:');
      if (metrics['first-contentful-paint']?.displayValue) {
        console.log(`  - First Contentful Paint: ${metrics['first-contentful-paint'].displayValue}`);
      }
      if (metrics['largest-contentful-paint']?.displayValue) {
        console.log(`  - Largest Contentful Paint: ${metrics['largest-contentful-paint'].displayValue}`);
      }
      if (metrics['total-blocking-time']?.displayValue) {
        console.log(`  - Total Blocking Time: ${metrics['total-blocking-time'].displayValue}`);
      }
      if (metrics['cumulative-layout-shift']?.displayValue) {
        console.log(`  - Cumulative Layout Shift: ${metrics['cumulative-layout-shift'].displayValue}`);
      }
      if (metrics['speed-index']?.displayValue) {
        console.log(`  - Speed Index: ${metrics['speed-index'].displayValue}`);
      }

      // Assert performance score meets threshold
      expect(performanceScore).toBeGreaterThanOrEqual(90);

    } finally {
      await browser.close();
    }
  });

  /**
   * Test Case 3: Total page weight is reasonable (ideally under 1MB)
   * Input: Check total page size
   * Expected: Total page weight is reasonable for content (ideally under 1MB)
   * Requirement: Performance Considerations - Minimal JavaScript payload, Optimized images
   */
  test('TC3: Total page weight should be under 1MB', async ({ page }) => {
    // Navigate to homepage with all resources loaded
    await page.goto('/', { waitUntil: 'networkidle' });

    // Wait a bit for all resources to be captured
    await page.waitForTimeout(1000);

    const { totalSize, jsSize, resources } = await getTotalPageSize(page);

    // Convert to KB and MB for readability
    const totalSizeKB = totalSize / 1024;
    const totalSizeMB = totalSizeKB / 1024;
    const jsSizeKB = jsSize / 1024;

    console.log(`Page Size Analysis:`);
    console.log(`  - Total Page Size: ${totalSizeKB.toFixed(2)} KB (${totalSizeMB.toFixed(2)} MB)`);
    console.log(`  - JavaScript Size: ${jsSizeKB.toFixed(2)} KB`);
    console.log(`  - Number of Resources: ${resources.length}`);

    // Log largest resources
    const sortedResources = [...resources].sort((a, b) => b.size - a.size).slice(0, 5);
    console.log(`  - Top 5 Largest Resources:`);
    sortedResources.forEach((r, i) => {
      const sizeKB = (r.size / 1024).toFixed(2);
      const name = r.name.split('/').pop() || r.name;
      console.log(`      ${i + 1}. ${name} (${sizeKB} KB)`);
    });

    // Assert total page size is under 1MB
    // 1MB = 1024 * 1024 bytes = 1,048,576 bytes
    expect(totalSize).toBeLessThan(1024 * 1024);
  });

  /**
   * Test Case 4: JavaScript payload is minimal
   * Input: Check JavaScript bundle size
   * Expected: JavaScript payload is minimal (Astro ships minimal JS by default)
   * Requirement: Design Specification - Astro provides minimal JavaScript by default
   */
  test('TC4: JavaScript bundle size should be minimal', async ({ page }) => {
    // Navigate to homepage with all resources loaded
    await page.goto('/', { waitUntil: 'networkidle' });

    // Wait a bit for all resources to be captured
    await page.waitForTimeout(1000);

    const { jsSize, resources } = await getTotalPageSize(page);

    // Filter JavaScript resources
    const jsResources = resources.filter(r =>
      r.name.endsWith('.js') ||
      r.name.includes('.js?') ||
      r.type === 'script'
    );

    const jsSizeKB = jsSize / 1024;

    console.log(`JavaScript Bundle Analysis:`);
    console.log(`  - Total JavaScript Size: ${jsSizeKB.toFixed(2)} KB`);
    console.log(`  - Number of JS Files: ${jsResources.length}`);

    // Log individual JS files
    if (jsResources.length > 0) {
      console.log(`  - JavaScript Files:`);
      jsResources.forEach((r, i) => {
        const sizeKB = (r.size / 1024).toFixed(2);
        const name = r.name.split('/').pop() || r.name;
        console.log(`      ${i + 1}. ${name} (${sizeKB} KB)`);
      });
    } else {
      console.log(`  - No external JavaScript files loaded (excellent!)`);
    }

    // Astro ships minimal JavaScript by default
    // For a static marketing page with minimal interactivity, JS should be under 100KB
    // Being generous with 200KB to account for any interactive components
    // An ideal Astro static site should have very little to no JS
    expect(jsSize).toBeLessThan(200 * 1024);

    // Log a message about Astro's philosophy
    console.log(`\n  Note: Astro ships minimal JavaScript by default.`);
    console.log(`  Static sites should ideally have < 50KB of JS.`);
    console.log(`  A value under 200KB is acceptable for sites with some interactivity.`);
  });
});

/**
 * Additional helper tests for comprehensive performance validation
 */
test.describe('Performance - Additional Metrics', () => {
  test('Page should have reasonable Time to First Byte (TTFB)', async ({ page }) => {
    // Navigate to homepage
    await page.goto('/', { waitUntil: 'domcontentloaded' });

    const navTiming = await getNavigationTiming(page);

    // TTFB = responseStart - requestStart
    const ttfb = navTiming.responseStart - navTiming.requestStart;

    console.log(`TTFB (Time to First Byte): ${ttfb.toFixed(2)}ms`);

    // TTFB should be under 600ms for a good user experience
    // For a local dev server, this should be much faster
    expect(ttfb).toBeLessThan(600);
  });

  test('Page should not have excessive render-blocking resources', async ({ page }) => {
    await page.goto('/', { waitUntil: 'networkidle' });

    const navTiming = await getNavigationTiming(page);

    // Time from responseEnd to domInteractive indicates parsing and render-blocking time
    const renderBlockingTime = navTiming.domInteractive - navTiming.responseEnd;

    console.log(`Render Blocking Time: ${renderBlockingTime.toFixed(2)}ms`);
    console.log(`  (Time from response received to DOM interactive)`);

    // Render blocking time should be under 1 second
    expect(renderBlockingTime).toBeLessThan(1000);
  });
});
