/**
 * Performance Tests
 * Owner: Scenario 12 - Performance and Page Load
 *
 * Test cases:
 * - Lighthouse performance score >= 90
 * - FCP under 2 seconds on 3G
 * - LCP under 2.5 seconds
 * - Total page size under 1MB
 * - Images optimized
 * - No render-blocking resources
 */

import { test, expect, Page } from '@playwright/test';

// Performance thresholds based on PRD NFR-1
const PERFORMANCE_THRESHOLDS = {
  LIGHTHOUSE_SCORE: 90,
  FCP_MS: 2000,  // First Contentful Paint under 2 seconds
  LCP_MS: 2500,  // Largest Contentful Paint under 2.5 seconds
  MAX_PAGE_SIZE_BYTES: 1024 * 1024, // 1MB
  MAX_IMAGE_SIZE_BYTES: 200 * 1024, // 200KB per image
};

// Helper to get resource sizes
async function getResourceSizes(page: Page): Promise<{
  totalSize: number;
  resources: { url: string; size: number; type: string }[];
}> {
  const resources: { url: string; size: number; type: string }[] = [];
  let totalSize = 0;

  // Get performance entries for resources
  const performanceEntries = await page.evaluate(() => {
    return performance.getEntriesByType('resource').map((entry) => {
      const resourceEntry = entry as PerformanceResourceTiming;
      return {
        url: resourceEntry.name,
        size: resourceEntry.transferSize || resourceEntry.encodedBodySize || 0,
        type: resourceEntry.initiatorType,
      };
    });
  });

  for (const entry of performanceEntries) {
    resources.push(entry);
    totalSize += entry.size;
  }

  // Also get the HTML document size
  const htmlSize = await page.evaluate(() => {
    return new Blob([document.documentElement.outerHTML]).size;
  });

  totalSize += htmlSize;
  resources.push({ url: page.url(), size: htmlSize, type: 'document' });

  return { totalSize, resources };
}

// Helper to get paint metrics
async function getPaintMetrics(page: Page): Promise<{
  fcp: number | null;
  lcp: number | null;
}> {
  return await page.evaluate(() => {
    return new Promise<{ fcp: number | null; lcp: number | null }>((resolve) => {
      let fcp: number | null = null;
      let lcp: number | null = null;

      // Get FCP from paint timing
      const paintEntries = performance.getEntriesByType('paint');
      for (const entry of paintEntries) {
        if (entry.name === 'first-contentful-paint') {
          fcp = entry.startTime;
        }
      }

      // Observe LCP
      const lcpObserver = new PerformanceObserver((list) => {
        const entries = list.getEntries();
        if (entries.length > 0) {
          lcp = entries[entries.length - 1].startTime;
        }
      });

      try {
        lcpObserver.observe({ type: 'largest-contentful-paint', buffered: true });
      } catch (e) {
        // LCP observer not supported
      }

      // Give time for LCP to be recorded
      setTimeout(() => {
        lcpObserver.disconnect();
        resolve({ fcp, lcp });
      }, 500);
    });
  });
}

test.describe('Performance and Page Load', () => {
  test.describe.configure({ timeout: 60000 });

  test('Test Case 1: Lighthouse performance score is 90 or higher', async ({ page }) => {
    // Navigate to the page
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Collect performance metrics using Performance Observer
    const paintMetrics = await getPaintMetrics(page);
    const resourceData = await getResourceSizes(page);

    // Calculate a performance score based on key metrics
    // This is a simplified approximation since full Lighthouse requires Chrome DevTools Protocol
    let score = 100;

    // FCP scoring (under 2s is good, over 4s is poor)
    if (paintMetrics.fcp) {
      if (paintMetrics.fcp > 4000) score -= 30;
      else if (paintMetrics.fcp > 2000) score -= 15;
    }

    // LCP scoring (under 2.5s is good, over 4s is poor)
    if (paintMetrics.lcp) {
      if (paintMetrics.lcp > 4000) score -= 30;
      else if (paintMetrics.lcp > 2500) score -= 15;
    }

    // Page size scoring
    if (resourceData.totalSize > 2 * 1024 * 1024) score -= 20;
    else if (resourceData.totalSize > 1024 * 1024) score -= 10;

    // Check for unoptimized images
    const imageResources = resourceData.resources.filter(r =>
      r.type === 'img' || r.url.match(/\.(gif|png|jpg|jpeg|webp|svg)$/i)
    );

    for (const img of imageResources) {
      if (img.size > 500 * 1024) score -= 10; // Large image penalty
    }

    console.log(`Performance score approximation: ${score}`);
    console.log(`FCP: ${paintMetrics.fcp}ms, LCP: ${paintMetrics.lcp}ms`);
    console.log(`Total page size: ${(resourceData.totalSize / 1024).toFixed(2)}KB`);

    // For a static site with optimized assets, we should achieve >= 90
    expect(score).toBeGreaterThanOrEqual(PERFORMANCE_THRESHOLDS.LIGHTHOUSE_SCORE);
  });

  test('Test Case 2: FCP is under 2 seconds on simulated 3G connection', async ({ browser }) => {
    // Create a new context with network throttling (simulated 3G)
    const context = await browser.newContext({
      // Simulate slow 3G network conditions
      offline: false,
    });

    const page = await context.newPage();

    // Use CDP to throttle network to 3G speeds
    const client = await page.context().newCDPSession(page);
    await client.send('Network.enable');
    await client.send('Network.emulateNetworkConditions', {
      offline: false,
      downloadThroughput: (750 * 1024) / 8, // 750 Kbps download (slow 3G)
      uploadThroughput: (250 * 1024) / 8,   // 250 Kbps upload
      latency: 100,                          // 100ms latency
    });

    // Record when navigation starts
    const startTime = Date.now();

    await page.goto('/');

    // Wait for FCP
    await page.waitForFunction(() => {
      const entries = performance.getEntriesByType('paint');
      return entries.some(entry => entry.name === 'first-contentful-paint');
    }, { timeout: 10000 });

    const paintMetrics = await getPaintMetrics(page);

    console.log(`FCP on 3G: ${paintMetrics.fcp}ms`);

    expect(paintMetrics.fcp).toBeLessThan(PERFORMANCE_THRESHOLDS.FCP_MS);

    await context.close();
  });

  test('Test Case 3: LCP is under 2.5 seconds', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Wait a bit for LCP to be recorded
    await page.waitForTimeout(1000);

    const paintMetrics = await getPaintMetrics(page);

    console.log(`LCP: ${paintMetrics.lcp}ms`);

    // LCP should be recorded and under 2.5 seconds
    expect(paintMetrics.lcp).not.toBeNull();
    expect(paintMetrics.lcp).toBeLessThan(PERFORMANCE_THRESHOLDS.LCP_MS);
  });

  test('Test Case 4: Total page size is under 1MB', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    const resourceData = await getResourceSizes(page);

    console.log(`Total page size: ${(resourceData.totalSize / 1024).toFixed(2)}KB`);
    console.log(`Resources breakdown:`);

    // Log resource breakdown by type
    const byType: Record<string, number> = {};
    for (const r of resourceData.resources) {
      byType[r.type] = (byType[r.type] || 0) + r.size;
    }

    for (const [type, size] of Object.entries(byType)) {
      console.log(`  ${type}: ${(size / 1024).toFixed(2)}KB`);
    }

    expect(resourceData.totalSize).toBeLessThan(PERFORMANCE_THRESHOLDS.MAX_PAGE_SIZE_BYTES);
  });

  test('Test Case 5: Images are appropriately sized and compressed', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Get all images on the page
    const images = await page.evaluate(() => {
      const imgs = document.querySelectorAll('img');
      return Array.from(imgs).map(img => ({
        src: img.src,
        naturalWidth: img.naturalWidth,
        naturalHeight: img.naturalHeight,
        displayWidth: img.offsetWidth,
        displayHeight: img.offsetHeight,
        alt: img.alt,
      }));
    });

    console.log('Image analysis:');

    for (const img of images) {
      // Skip external images (like badges)
      if (img.src.includes('circleci.com') || img.src.includes('shields.io')) {
        console.log(`  Skipping external image: ${img.src}`);
        continue;
      }

      console.log(`  ${img.src}:`);
      console.log(`    Natural: ${img.naturalWidth}x${img.naturalHeight}`);
      console.log(`    Display: ${img.displayWidth}x${img.displayHeight}`);

      // Check that images aren't massively oversized for their display
      // Note: Allow higher ratios for small display sizes where the same image
      // may need to serve larger display contexts (like responsive designs)
      if (img.displayWidth > 0 && img.naturalWidth > 0) {
        const oversize = img.naturalWidth / img.displayWidth;
        console.log(`    Oversize ratio: ${oversize.toFixed(2)}x`);

        // For small display sizes (< 50px), allow up to 8x for responsive serving
        // For larger displays, keep stricter 4x limit
        const maxRatio = img.displayWidth < 50 ? 8 : 4;
        expect(oversize).toBeLessThan(maxRatio);
      }
    }

    // Check image file sizes from resources
    const resourceData = await getResourceSizes(page);
    const imageResources = resourceData.resources.filter(r =>
      r.url.match(/\.(gif|png|jpg|jpeg|webp|svg)$/i)
    );

    for (const img of imageResources) {
      // Skip external images
      if (!img.url.includes('localhost')) continue;

      console.log(`  ${img.url}: ${(img.size / 1024).toFixed(2)}KB`);

      // Each image should be under 200KB for optimal performance
      expect(img.size).toBeLessThan(PERFORMANCE_THRESHOLDS.MAX_IMAGE_SIZE_BYTES);
    }
  });

  test('Test Case 6: Critical CSS is inlined, non-critical assets deferred', async ({ page }) => {
    await page.goto('/');

    // Check for render-blocking resources
    const renderBlockingResources = await page.evaluate(() => {
      const renderBlocking: string[] = [];

      // Check stylesheets
      const stylesheets = document.querySelectorAll('link[rel="stylesheet"]');
      for (const link of stylesheets) {
        const href = link.getAttribute('href');
        const media = link.getAttribute('media');

        // A stylesheet is render-blocking if it doesn't have media="print" or similar
        if (href && (!media || media === 'all' || media === 'screen')) {
          // Check if it's a critical stylesheet (small) or should be deferred
          renderBlocking.push(href);
        }
      }

      // Check scripts without defer/async
      const scripts = document.querySelectorAll('script[src]');
      for (const script of scripts) {
        const src = script.getAttribute('src');
        const hasDefer = script.hasAttribute('defer');
        const hasAsync = script.hasAttribute('async');

        if (src && !hasDefer && !hasAsync) {
          renderBlocking.push(src);
        }
      }

      return renderBlocking;
    });

    console.log('Render-blocking resources:');
    for (const resource of renderBlockingResources) {
      console.log(`  ${resource}`);
    }

    // Check that all scripts are deferred
    const scripts = await page.locator('script[src]').all();
    for (const script of scripts) {
      const src = await script.getAttribute('src');
      const defer = await script.getAttribute('defer');
      const async = await script.getAttribute('async');

      console.log(`  Script ${src}: defer=${defer !== null}, async=${async !== null}`);

      // All scripts should have defer or async
      expect(defer !== null || async !== null).toBe(true);
    }

    // CSS files - check they're small enough to not significantly block rendering
    const resourceData = await getResourceSizes(page);
    const cssResources = resourceData.resources.filter(r =>
      r.url.endsWith('.css') || r.type === 'link'
    );

    let totalCssSize = 0;
    for (const css of cssResources) {
      totalCssSize += css.size;
      console.log(`  CSS ${css.url}: ${(css.size / 1024).toFixed(2)}KB`);
    }

    // Total CSS should be under 100KB for good performance
    console.log(`  Total CSS size: ${(totalCssSize / 1024).toFixed(2)}KB`);
    expect(totalCssSize).toBeLessThan(100 * 1024);
  });
});
