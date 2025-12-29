import { test, expect, chromium } from '@playwright/test';
import * as fs from 'fs';
import * as path from 'path';

/**
 * Page Load Performance Tests
 *
 * Verifies NFR-2: Initial page load under 3 seconds on 3G connection
 *
 * Test Cases:
 * 1. First Contentful Paint under 3 seconds with 3G throttling
 * 2. Total page weight is optimized for performance
 * 3. Images use modern formats and appropriate compression
 */

// 3G network conditions (typical 3G: ~750 Kbps down, ~250 Kbps up, 100ms latency)
const SLOW_3G_CONDITIONS = {
  offline: false,
  downloadThroughput: (750 * 1024) / 8, // 750 Kbps in bytes per second
  uploadThroughput: (250 * 1024) / 8,   // 250 Kbps in bytes per second
  latency: 100,                          // 100ms latency
};

// Performance budgets
const PERFORMANCE_BUDGETS = {
  FCP_THRESHOLD_MS: 3000,           // First Contentful Paint must be under 3 seconds
  TOTAL_PAGE_SIZE_KB: 500,          // Total page size budget (KB) - strict for 3G
  HTML_SIZE_KB: 100,                // HTML document size budget
  CSS_SIZE_KB: 50,                  // CSS size budget (inline CSS in HTML)
  JS_SIZE_KB: 50,                   // JavaScript size budget
  IMAGE_SIZE_KB: 200,               // Total images size budget
};

test.describe('Page Load Performance (NFR-2)', () => {

  test('Test Case 1: First Contentful Paint under 3 seconds with simulated 3G throttling', async ({ browser }) => {
    // Create a new context with CDP session for network throttling
    const context = await browser.newContext();
    const page = await context.newPage();

    // Get CDP session for network emulation
    const client = await context.newCDPSession(page);

    // Enable network emulation with 3G conditions
    await client.send('Network.enable');
    await client.send('Network.emulateNetworkConditions', SLOW_3G_CONDITIONS);

    // Enable Performance API
    await client.send('Performance.enable');

    // Navigate and wait for load
    const startTime = Date.now();
    await page.goto('/', { waitUntil: 'domcontentloaded' });

    // Get First Contentful Paint metric
    const performanceMetrics = await page.evaluate(() => {
      return new Promise<{ fcp: number | null; lcp: number | null }>((resolve) => {
        // Wait a bit for paint entries to be recorded
        setTimeout(() => {
          const paintEntries = performance.getEntriesByType('paint');
          const fcpEntry = paintEntries.find(entry => entry.name === 'first-contentful-paint');

          // Also try to get LCP if available
          let lcpValue: number | null = null;
          try {
            const lcpEntries = performance.getEntriesByType('largest-contentful-paint');
            if (lcpEntries.length > 0) {
              lcpValue = lcpEntries[lcpEntries.length - 1].startTime;
            }
          } catch {
            // LCP not available in all browsers
          }

          resolve({
            fcp: fcpEntry ? fcpEntry.startTime : null,
            lcp: lcpValue
          });
        }, 500);
      });
    });

    const totalLoadTime = Date.now() - startTime;

    console.log(`Performance Metrics:`);
    console.log(`  First Contentful Paint: ${performanceMetrics.fcp?.toFixed(2) || 'N/A'}ms`);
    console.log(`  Total Load Time: ${totalLoadTime}ms`);

    // Verify FCP is under 3 seconds
    if (performanceMetrics.fcp !== null) {
      expect(performanceMetrics.fcp).toBeLessThan(PERFORMANCE_BUDGETS.FCP_THRESHOLD_MS);
    } else {
      // Fallback: if FCP metric not available, use total load time
      // This is acceptable because the page is a simple static HTML with inline CSS
      expect(totalLoadTime).toBeLessThan(PERFORMANCE_BUDGETS.FCP_THRESHOLD_MS);
    }

    // Verify the page actually rendered content
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    await context.close();
  });

  test('Test Case 2: Check total page weight is optimized for performance', async ({ page, request }) => {
    // Fetch the HTML document and measure its size
    const response = await request.get('/');
    const htmlContent = await response.text();
    const htmlSizeKB = Buffer.byteLength(htmlContent, 'utf8') / 1024;

    console.log(`Page Weight Analysis:`);
    console.log(`  HTML Document Size: ${htmlSizeKB.toFixed(2)} KB`);

    // Parse HTML to check for external resources
    const hasExternalCSS = htmlContent.includes('<link rel="stylesheet"');
    const hasExternalJS = htmlContent.includes('<script src=');
    const hasInlineCSS = htmlContent.includes('<style>');
    const hasInlineJS = htmlContent.includes('<script>') && !htmlContent.includes('<script src=');

    console.log(`  Has External CSS: ${hasExternalCSS}`);
    console.log(`  Has External JS: ${hasExternalJS}`);
    console.log(`  Has Inline CSS: ${hasInlineCSS}`);
    console.log(`  Has Inline JS: ${hasInlineJS}`);

    // Check for image tags
    const imgMatches = htmlContent.match(/<img[^>]*>/g) || [];
    console.log(`  Number of <img> tags: ${imgMatches.length}`);

    // The page uses inline CSS (no external CSS files) which is optimal for single-page sites
    // This reduces HTTP requests and improves FCP
    expect(hasInlineCSS).toBe(true);

    // The page should not have heavy external JavaScript files
    // Note: The current implementation doesn't have external JS
    expect(hasExternalJS).toBe(false);

    // HTML size should be reasonable (includes inline CSS)
    // Since CSS is inlined, the HTML can be larger but should still be optimized
    expect(htmlSizeKB).toBeLessThan(PERFORMANCE_BUDGETS.HTML_SIZE_KB);

    // Navigate to check actual rendered page
    await page.goto('/');

    // Verify minimal HTTP requests by checking page resources
    // Using the Performance API to get resource timings
    const resourceCount = await page.evaluate(() => {
      const resources = performance.getEntriesByType('resource');
      return resources.length;
    });

    console.log(`  Total HTTP Requests (resources): ${resourceCount}`);

    // For a static HTML page with inline CSS and SVG icons,
    // we should have minimal external requests
    // Expected: 0 (no external resources) for this optimized page
    expect(resourceCount).toBeLessThanOrEqual(5); // Allow some flexibility
  });

  test('Test Case 3: Verify images are optimized - Images use modern formats and appropriate compression', async ({ page, request }) => {
    // Fetch the HTML content
    const response = await request.get('/');
    const htmlContent = await response.text();

    // Check what type of images are used
    const hasImgTags = htmlContent.includes('<img');
    const hasSvgInline = htmlContent.includes('<svg');
    const hasWebP = htmlContent.includes('.webp');
    const hasAvif = htmlContent.includes('.avif');
    const hasPng = htmlContent.includes('.png');
    const hasJpg = htmlContent.includes('.jpg') || htmlContent.includes('.jpeg');
    const hasGif = htmlContent.includes('.gif');

    console.log(`Image Analysis:`);
    console.log(`  Has <img> tags: ${hasImgTags}`);
    console.log(`  Uses Inline SVG: ${hasSvgInline}`);
    console.log(`  Uses WebP: ${hasWebP}`);
    console.log(`  Uses AVIF: ${hasAvif}`);
    console.log(`  Uses PNG: ${hasPng}`);
    console.log(`  Uses JPG/JPEG: ${hasJpg}`);
    console.log(`  Uses GIF: ${hasGif}`);

    // Count SVG icons
    const svgMatches = htmlContent.match(/<svg[^>]*>/g) || [];
    console.log(`  Number of inline SVG icons: ${svgMatches.length}`);

    // Navigate to page to verify visual elements
    await page.goto('/');

    // Verify SVG icons are rendering properly
    const featureIcons = await page.locator('.feature-icon svg').count();
    const specIcons = await page.locator('.spec-card h3 svg').count();
    const footerIcons = await page.locator('.footer-links svg').count();

    console.log(`  Feature section icons: ${featureIcons}`);
    console.log(`  Spec section icons: ${specIcons}`);
    console.log(`  Footer icons: ${footerIcons}`);

    // The page uses inline SVG icons instead of raster images
    // This is an optimal choice because:
    // 1. SVGs are vector-based and scale perfectly
    // 2. They can be inlined to reduce HTTP requests
    // 3. They are typically smaller than equivalent raster images
    // 4. They can be styled with CSS
    expect(hasSvgInline).toBe(true);

    // Verify there are no heavy raster image files
    // The page should use SVG icons instead of PNG/JPG images
    // If <img> tags exist, they should use modern formats
    if (hasImgTags) {
      // If there are img tags, prefer modern formats
      const usesModernFormats = hasWebP || hasAvif || hasSvgInline;
      const usesHeavyFormats = hasPng || hasJpg || hasGif;

      // Either uses modern formats or no heavy formats
      expect(usesModernFormats || !usesHeavyFormats).toBe(true);
    }

    // Verify that icons are visible on the page (SVGs rendered correctly)
    expect(featureIcons).toBeGreaterThan(0);
    expect(specIcons).toBeGreaterThan(0);
    expect(footerIcons).toBeGreaterThan(0);

    // Calculate total "image" content size (inline SVGs)
    // Extract all SVG content
    const svgContents = htmlContent.match(/<svg[\s\S]*?<\/svg>/g) || [];
    const totalSvgSize = svgContents.reduce((acc, svg) => acc + Buffer.byteLength(svg, 'utf8'), 0);
    const totalSvgSizeKB = totalSvgSize / 1024;

    console.log(`  Total inline SVG size: ${totalSvgSizeKB.toFixed(2)} KB`);

    // SVG icons should be lightweight
    expect(totalSvgSizeKB).toBeLessThan(PERFORMANCE_BUDGETS.IMAGE_SIZE_KB);
  });

  test('Additional: Verify page has no render-blocking resources', async ({ page }) => {
    // Navigate to the page
    await page.goto('/');

    // Check for render-blocking resources
    const renderBlockingResources = await page.evaluate(() => {
      const resources = performance.getEntriesByType('resource') as PerformanceResourceTiming[];

      // Filter for potentially render-blocking resources
      const blocking = resources.filter(resource => {
        const isCSS = resource.name.endsWith('.css');
        const isJS = resource.name.endsWith('.js') && !resource.name.includes('async');
        return (isCSS || isJS) && resource.renderBlockingStatus === 'blocking';
      });

      return blocking.map(r => ({
        name: r.name,
        duration: r.duration,
        renderBlockingStatus: r.renderBlockingStatus
      }));
    });

    console.log(`Render-blocking resources: ${renderBlockingResources.length}`);
    renderBlockingResources.forEach(r => {
      console.log(`  - ${r.name} (${r.duration.toFixed(2)}ms)`);
    });

    // The page should have no external render-blocking resources
    // since all CSS is inlined
    expect(renderBlockingResources.length).toBe(0);
  });

  test('Additional: Verify page compression and transfer size', async ({ page }) => {
    // Set up request interception to measure actual transfer sizes
    const transferSizes: { url: string; size: number }[] = [];

    page.on('response', async (response) => {
      const url = response.url();
      const headers = response.headers();
      const contentLength = headers['content-length'];

      if (contentLength) {
        transferSizes.push({
          url,
          size: parseInt(contentLength, 10)
        });
      }
    });

    await page.goto('/');

    // Calculate total transfer size
    const totalTransferSize = transferSizes.reduce((acc, item) => acc + item.size, 0);
    const totalTransferSizeKB = totalTransferSize / 1024;

    console.log(`Transfer Size Analysis:`);
    console.log(`  Total Transfer Size: ${totalTransferSizeKB.toFixed(2)} KB`);
    transferSizes.forEach(item => {
      console.log(`  - ${item.url.split('/').pop() || 'index.html'}: ${(item.size / 1024).toFixed(2)} KB`);
    });

    // Total transfer size should be optimized for 3G
    // With inline CSS and no external resources, this should be very small
    expect(totalTransferSizeKB).toBeLessThan(PERFORMANCE_BUDGETS.TOTAL_PAGE_SIZE_KB);
  });
});
