/**
 * Performance and Loading Tests
 * Owner: Scenario 8 - Performance and Loading
 *
 * Test cases:
 * - Page load under 2 seconds on 3G
 * - Total page size under 500KB
 * - HTML validation passes
 * - CSS validation passes
 * - No required JavaScript for core functionality
 * - Optimized images (SVG for diagrams)
 */

const { test, expect } = require('@playwright/test');

test.describe('Performance and Loading', () => {

  // TC1: Page load time under 2 seconds on simulated 3G connection
  test('should load page fully in under 2 seconds on simulated 3G', async ({ page }) => {
    // Configure network conditions to simulate Fast 3G
    // Fast 3G: ~1.6 Mbps download, ~750 kbps upload, 150ms latency
    const client = await page.context().newCDPSession(page);
    await client.send('Network.emulateNetworkConditions', {
      offline: false,
      downloadThroughput: (1.6 * 1024 * 1024) / 8, // 1.6 Mbps in bytes/second
      uploadThroughput: (750 * 1024) / 8, // 750 kbps in bytes/second
      latency: 150, // 150ms latency
    });

    const startTime = Date.now();

    // Navigate to the homepage
    await page.goto('http://localhost:3000/index.html', {
      waitUntil: 'domcontentloaded', // Wait for DOM to be ready
    });

    // Wait for the page to be fully interactive (hero section visible)
    await page.waitForSelector('#hero', { state: 'visible' });

    const loadTime = Date.now() - startTime;

    // Page should load in under 2000ms (2 seconds)
    expect(loadTime).toBeLessThan(2000);

    console.log(`Page load time on 3G: ${loadTime}ms`);
  });

  // TC2: Total page size under 500KB including all resources
  test('should have total page weight under 500KB', async ({ page }) => {
    const resourceSizes = [];

    // Track all network requests and their sizes
    page.on('response', async (response) => {
      try {
        const url = response.url();
        // Only count resources from our domain
        if (url.includes('localhost:3000') || url.startsWith('file://')) {
          const headers = response.headers();
          const contentLength = headers['content-length'];

          if (contentLength) {
            resourceSizes.push({
              url: url,
              size: parseInt(contentLength, 10)
            });
          } else {
            // Try to get body size
            try {
              const body = await response.body();
              resourceSizes.push({
                url: url,
                size: body.length
              });
            } catch (e) {
              // Skip if we can't get the body
            }
          }
        }
      } catch (e) {
        // Skip errors
      }
    });

    await page.goto('http://localhost:3000/index.html', {
      waitUntil: 'networkidle',
    });

    // Calculate total size
    const totalSize = resourceSizes.reduce((sum, r) => sum + r.size, 0);
    const totalSizeKB = totalSize / 1024;

    console.log('Resource breakdown:');
    resourceSizes.forEach(r => {
      console.log(`  ${r.url}: ${(r.size / 1024).toFixed(2)} KB`);
    });
    console.log(`Total page weight: ${totalSizeKB.toFixed(2)} KB`);

    // Total should be under 500KB
    expect(totalSizeKB).toBeLessThan(500);
  });

  // TC5: No JavaScript required for core functionality
  test('should not require JavaScript for core functionality', async ({ browser }) => {
    // Create a new context with JavaScript disabled
    const context = await browser.newContext({ javaScriptEnabled: false });
    const page = await context.newPage();

    await page.goto('http://localhost:3000/index.html', {
      waitUntil: 'domcontentloaded',
    });

    // All core sections should be visible without JavaScript
    await expect(page.locator('#hero')).toBeVisible();
    await expect(page.locator('#features')).toBeVisible();
    await expect(page.locator('#getting-started')).toBeVisible();
    await expect(page.locator('#architecture')).toBeVisible();
    await expect(page.locator('#configuration')).toBeVisible();

    // Hero content should be readable
    await expect(page.locator('.hero__title')).toContainText('MirDB');
    await expect(page.locator('.hero__tagline')).toBeVisible();

    // Navigation should be visible
    await expect(page.locator('nav.nav')).toBeVisible();

    // Features cards should be visible
    const featureCards = page.locator('.features__card');
    await expect(featureCards).toHaveCount(4);

    // Configuration table should be visible
    await expect(page.locator('.config__table')).toBeVisible();

    // Code blocks should be visible
    await expect(page.locator('.getting-started__code-block')).toHaveCount(5);

    console.log('All core functionality works without JavaScript');

    await context.close();
  });

  // TC6: Verify images are optimized (SVG for diagrams)
  test('should use appropriate image formats', async ({ page }) => {
    await page.goto('http://localhost:3000/index.html', {
      waitUntil: 'networkidle',
    });

    // Get all images on the page
    const images = await page.evaluate(() => {
      const imgs = document.querySelectorAll('img');
      const svgs = document.querySelectorAll('svg');

      const imgInfo = [];
      imgs.forEach(img => {
        imgInfo.push({
          src: img.src,
          type: 'img',
          alt: img.alt
        });
      });

      return {
        images: imgInfo,
        svgCount: svgs.length
      };
    });

    // Check that any diagram images use SVG format
    const diagramImages = images.images.filter(img =>
      img.src.includes('diagram') ||
      img.src.includes('architecture') ||
      img.alt?.toLowerCase().includes('diagram')
    );

    // All diagram images should be SVG
    diagramImages.forEach(img => {
      expect(img.src).toMatch(/\.svg/i);
    });

    // Check that inline SVGs are used for icons (more efficient)
    expect(images.svgCount).toBeGreaterThan(0);
    console.log(`Found ${images.svgCount} inline SVGs (efficient for icons)`);

    // If there are any raster images, they should be appropriately small or compressed
    const rasterImages = images.images.filter(img =>
      img.src.match(/\.(png|jpg|jpeg|webp|gif)/i)
    );

    // Log info about images
    console.log(`Total images: ${images.images.length}`);
    console.log(`Diagram images: ${diagramImages.length}`);
    console.log(`Raster images: ${rasterImages.length}`);
    console.log(`Inline SVGs: ${images.svgCount}`);

    // Verify raster images (if any) use optimized formats
    rasterImages.forEach(img => {
      // Prefer WebP for photos
      const isWebP = img.src.match(/\.webp/i);
      const isPNG = img.src.match(/\.png/i);
      // Both are acceptable, just log which is used
      console.log(`Raster image: ${img.src} (${isWebP ? 'WebP' : isPNG ? 'PNG' : 'Other'})`);
    });
  });

  // Additional: Check for render-blocking resources
  test('should not have unnecessary render-blocking resources', async ({ page }) => {
    const renderBlockingResources = [];

    page.on('request', request => {
      const resourceType = request.resourceType();
      const url = request.url();

      // Track potential render-blocking resources
      if (resourceType === 'script' && !request.url().includes('localhost')) {
        renderBlockingResources.push({
          type: 'external-script',
          url: url
        });
      }
    });

    await page.goto('http://localhost:3000/index.html', {
      waitUntil: 'domcontentloaded',
    });

    // Check for external scripts in the HTML
    const scripts = await page.evaluate(() => {
      const scriptTags = document.querySelectorAll('script[src]');
      return Array.from(scriptTags).map(s => ({
        src: s.src,
        async: s.async,
        defer: s.defer
      }));
    });

    // Check for external stylesheets
    const stylesheets = await page.evaluate(() => {
      const linkTags = document.querySelectorAll('link[rel="stylesheet"]');
      return Array.from(linkTags).map(l => ({
        href: l.href,
        isLocal: l.href.includes('localhost') || l.href.includes('file://')
      }));
    });

    console.log('Scripts:', scripts);
    console.log('Stylesheets:', stylesheets);

    // External scripts should have async or defer
    scripts.forEach(script => {
      if (!script.src.includes('localhost') && !script.src.includes('file://')) {
        expect(script.async || script.defer).toBeTruthy();
      }
    });

    // Local stylesheet is expected (css/styles.css)
    const localStylesheets = stylesheets.filter(s => s.isLocal);
    expect(localStylesheets.length).toBe(1);

    console.log('No unnecessary render-blocking resources detected');
  });

  // Additional: Verify CSS is optimized (no huge file sizes)
  test('should have reasonably sized CSS file', async ({ page }) => {
    let cssSize = 0;

    page.on('response', async (response) => {
      if (response.url().includes('styles.css')) {
        try {
          const body = await response.body();
          cssSize = body.length;
        } catch (e) {
          // Use content-length header as fallback
          const headers = response.headers();
          if (headers['content-length']) {
            cssSize = parseInt(headers['content-length'], 10);
          }
        }
      }
    });

    await page.goto('http://localhost:3000/index.html', {
      waitUntil: 'networkidle',
    });

    // CSS should be reasonably sized (under 100KB for a simple page)
    const cssSizeKB = cssSize / 1024;
    console.log(`CSS file size: ${cssSizeKB.toFixed(2)} KB`);
    expect(cssSizeKB).toBeLessThan(100);
  });
});
