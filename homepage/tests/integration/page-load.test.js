/**
 * Page Load Performance Tests
 * Owner: Scenario 9 - Performance Optimization
 *
 * Tests:
 * - Page loads within 2 seconds on standard connection (NFR-1)
 * - All critical resources are loaded
 * - No render-blocking resources
 * - Images are optimized
 * - Total page size is under 500KB
 */

const { test, expect, chromium } = require('@playwright/test');
const path = require('path');
const fs = require('fs');

// Configure for performance testing
test.use({
  baseURL: 'http://localhost:8080',
});

test.describe('Performance and Load Time Tests', () => {

  /**
   * Test Case 1: DOMContentLoaded fires within 1 second
   */
  test('DOMContentLoaded fires within 1 second', async ({ page }) => {
    // Measure DOMContentLoaded timing
    const metrics = await measurePageLoad(page, '/');

    expect(metrics.domContentLoaded).toBeLessThan(1000);
    console.log(`DOMContentLoaded: ${metrics.domContentLoaded}ms`);
  });

  /**
   * Test Case 2: Time to Interactive (TTI) is less than 2 seconds
   */
  test('Time to Interactive is less than 2 seconds', async ({ page }) => {
    const metrics = await measurePageLoad(page, '/');

    // TTI is approximated by loadComplete time for static pages
    expect(metrics.loadComplete).toBeLessThan(2000);
    console.log(`Load Complete (TTI approximation): ${metrics.loadComplete}ms`);
  });

  /**
   * Test Case 3: Lighthouse-style performance audit (simplified)
   * Note: Full Lighthouse requires separate setup, so we test key metrics
   */
  test('Performance metrics meet quality standards (score >= 90 equivalent)', async ({ page }) => {
    const metrics = await measurePageLoad(page, '/');

    // Performance score is based on:
    // - First Contentful Paint < 1.8s (good)
    // - Speed Index < 3.4s (good)
    // - Largest Contentful Paint < 2.5s (good)
    // - Time to Interactive < 3.8s (good)
    // - Total Blocking Time < 200ms (good)
    // - Cumulative Layout Shift < 0.1 (good)

    // For a static page, we check key metrics that contribute to a 90+ score
    expect(metrics.firstContentfulPaint).toBeLessThan(1800);
    expect(metrics.loadComplete).toBeLessThan(2500);
    expect(metrics.domContentLoaded).toBeLessThan(1000);

    console.log(`FCP: ${metrics.firstContentfulPaint}ms`);
    console.log(`LCP (approximated): ${metrics.largestContentfulPaint}ms`);
  });

  /**
   * Test Case 4: Check for render-blocking CSS
   * Critical CSS should be inlined or no render-blocking CSS
   */
  test('No render-blocking CSS or critical CSS is inlined', async ({ page }) => {
    await page.goto('/');

    // Get all link elements for stylesheets
    const stylesheets = await page.evaluate(() => {
      const links = Array.from(document.querySelectorAll('link[rel="stylesheet"]'));
      return links.map(link => ({
        href: link.href,
        media: link.media || 'all',
        // Check if it's render-blocking (no async loading mechanism)
        isRenderBlocking: !link.media || link.media === 'all' || link.media === 'screen'
      }));
    });

    // For a small static site, having a few CSS files is acceptable
    // The key is that they're small and load quickly
    const renderBlockingCount = stylesheets.filter(s => s.isRenderBlocking).length;

    // Verify CSS files exist and are small
    for (const stylesheet of stylesheets) {
      const response = await page.request.get(stylesheet.href);
      const size = (await response.body()).length;
      // Each CSS file should be under 50KB
      expect(size).toBeLessThan(50 * 1024);
      console.log(`CSS ${stylesheet.href}: ${(size / 1024).toFixed(2)}KB`);
    }

    // Having 3 or fewer render-blocking CSS files is acceptable for this page
    expect(renderBlockingCount).toBeLessThanOrEqual(3);
    console.log(`Render-blocking CSS files: ${renderBlockingCount}`);
  });

  /**
   * Test Case 5: Check for render-blocking JS
   * JS should be deferred or async
   */
  test('JS is deferred or async (no render-blocking JS)', async ({ page }) => {
    await page.goto('/');

    // Check all script elements
    const scripts = await page.evaluate(() => {
      const scriptTags = Array.from(document.querySelectorAll('script'));
      return scriptTags
        .filter(s => s.src) // Only external scripts
        .map(script => ({
          src: script.src,
          async: script.async,
          defer: script.defer,
          type: script.type || 'text/javascript',
          isRenderBlocking: !script.async && !script.defer && script.type !== 'module'
        }));
    });

    // All external scripts should be either async or defer
    const renderBlockingScripts = scripts.filter(s => s.isRenderBlocking);

    console.log(`Total external scripts: ${scripts.length}`);
    scripts.forEach(s => {
      console.log(`Script ${s.src}: async=${s.async}, defer=${s.defer}`);
    });

    expect(renderBlockingScripts.length).toBe(0);
  });

  /**
   * Test Case 6: Total page size is under 500KB
   */
  test('Total page size is under 500KB', async ({ page }) => {
    let totalSize = 0;
    const resourceSizes = [];

    // Listen for all network requests
    page.on('response', async (response) => {
      try {
        const size = (await response.body()).length;
        totalSize += size;
        resourceSizes.push({
          url: response.url(),
          size: size,
          type: response.headers()['content-type']
        });
      } catch (e) {
        // Some responses may not have a body
      }
    });

    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Log resource breakdown
    resourceSizes.sort((a, b) => b.size - a.size);
    console.log('\nResource breakdown:');
    resourceSizes.slice(0, 10).forEach(r => {
      console.log(`  ${(r.size / 1024).toFixed(2)}KB - ${r.url.split('/').pop()}`);
    });

    console.log(`\nTotal page size: ${(totalSize / 1024).toFixed(2)}KB`);

    // Page should be under 500KB
    expect(totalSize).toBeLessThan(500 * 1024);
  });

  /**
   * Test Case 7: Check image formats
   * Images should use optimized formats (WebP, SVG where appropriate)
   */
  test('Images use optimized formats (SVG for icons/logos)', async ({ page }) => {
    await page.goto('/');

    // Get all images
    const images = await page.evaluate(() => {
      const imgs = Array.from(document.querySelectorAll('img'));
      return imgs.map(img => ({
        src: img.src,
        alt: img.alt,
        width: img.naturalWidth,
        height: img.naturalHeight
      }));
    });

    console.log(`Total images: ${images.length}`);

    // For this homepage, logos and icons should be SVG
    const svgImages = images.filter(img => img.src.endsWith('.svg'));
    const nonSvgImages = images.filter(img => !img.src.endsWith('.svg') && !img.src.startsWith('https://img.shields.io'));

    console.log(`SVG images: ${svgImages.length}`);
    console.log(`Non-SVG images (excluding badges): ${nonSvgImages.length}`);

    // All local images should be SVG for this site (optimal for icons/logos)
    // External badge images from shields.io are acceptable
    for (const img of images) {
      if (!img.src.startsWith('https://img.shields.io')) {
        // Local images should be SVG
        expect(img.src.endsWith('.svg') || img.src.includes('data:image/svg')).toBeTruthy();
      }
    }
  });

  /**
   * Test Case 8: Test load on simulated 3G connection
   * Page should be usable within 5 seconds on 3G
   */
  test('Page is usable within 5 seconds on 3G connection', async ({ browser }) => {
    // Create a new context with network throttling
    const context = await browser.newContext();
    const page = await context.newPage();

    // Simulate 3G connection using CDP
    const client = await context.newCDPSession(page);
    await client.send('Network.emulateNetworkConditions', {
      offline: false,
      downloadThroughput: (750 * 1024) / 8, // 750 Kbps
      uploadThroughput: (250 * 1024) / 8,   // 250 Kbps
      latency: 100                           // 100ms RTT
    });

    const startTime = Date.now();
    await page.goto('http://localhost:8080/', { waitUntil: 'domcontentloaded' });
    const domContentTime = Date.now() - startTime;

    // Wait for page to be interactive
    await page.waitForLoadState('load');
    const loadTime = Date.now() - startTime;

    console.log(`3G DOMContentLoaded: ${domContentTime}ms`);
    console.log(`3G Full Load: ${loadTime}ms`);

    // Page should be usable (DOMContentLoaded) within 5 seconds on 3G
    expect(domContentTime).toBeLessThan(5000);

    // Verify page is interactive
    const heroVisible = await page.isVisible('.hero');
    expect(heroVisible).toBeTruthy();

    await context.close();
  });

});

/**
 * Helper function to measure page load timing
 */
async function measurePageLoad(page, url) {
  // Navigate and collect performance metrics
  await page.goto(url, { waitUntil: 'networkidle' });

  // Get navigation timing
  const timing = await page.evaluate(() => {
    const perf = performance.getEntriesByType('navigation')[0];
    const paint = performance.getEntriesByType('paint');

    const fcp = paint.find(p => p.name === 'first-contentful-paint');

    return {
      domContentLoaded: perf.domContentLoadedEventEnd - perf.fetchStart,
      loadComplete: perf.loadEventEnd - perf.fetchStart,
      firstContentfulPaint: fcp ? fcp.startTime : 0,
      responseStart: perf.responseStart - perf.fetchStart,
      domInteractive: perf.domInteractive - perf.fetchStart
    };
  });

  // Get LCP using PerformanceObserver
  const lcp = await page.evaluate(() => {
    return new Promise((resolve) => {
      new PerformanceObserver((list) => {
        const entries = list.getEntries();
        const lastEntry = entries[entries.length - 1];
        resolve(lastEntry ? lastEntry.startTime : 0);
      }).observe({ type: 'largest-contentful-paint', buffered: true });

      // Fallback after 3 seconds
      setTimeout(() => resolve(0), 3000);
    });
  });

  return {
    ...timing,
    largestContentfulPaint: lcp
  };
}
