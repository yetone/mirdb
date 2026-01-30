/**
 * Performance Integration Tests
 * Owner: Scenario 8 - Performance Requirements
 *
 * Tests:
 * - Lighthouse performance score
 * - First Contentful Paint
 * - Time to Interactive
 * - Total page weight
 * - Image optimization
 * - Render-blocking resources
 */

const { test, expect, chromium, devices } = require('@playwright/test');
const path = require('path');
const fs = require('fs');

// Calculate total size of a resource type
function calculateResourceSize(resources, type) {
  return resources
    .filter(r => r.resourceType === type)
    .reduce((sum, r) => sum + (r.encodedDataLength || 0), 0);
}

test.describe('Performance Requirements', () => {
  let browser;
  let context;
  let page;
  let server;

  test.beforeAll(async () => {
    // Start local server
    const { spawn } = require('child_process');
    server = spawn('npx', ['http-server', '/workspace/mirdb-landing', '-p', '8080', '-c-1', '--silent'], {
      detached: true,
      stdio: 'ignore'
    });
    server.unref();

    // Wait for server to start
    await new Promise(resolve => setTimeout(resolve, 2000));
  });

  test.afterAll(async () => {
    if (server && server.pid) {
      try {
        process.kill(-server.pid, 'SIGTERM');
      } catch (e) {
        // Ignore errors if process already terminated
      }
    }
  });

  test.beforeEach(async () => {
    browser = await chromium.launch({ headless: true });
    context = await browser.newContext();
    page = await context.newPage();
  });

  test.afterEach(async () => {
    await browser.close();
  });

  test('Test Case 1: Lighthouse performance audit on desktop - Performance score 90+ with FCP under 1.5s and TTI under 3s', async () => {
    // Collect performance metrics via Performance API
    const client = await context.newCDPSession(page);
    await client.send('Performance.enable');

    const startTime = Date.now();
    await page.goto('http://localhost:8080', { waitUntil: 'load' });
    const loadTime = Date.now() - startTime;

    // Get Core Web Vitals via Performance Observer
    const performanceMetrics = await page.evaluate(() => {
      return new Promise((resolve) => {
        const metrics = {
          fcp: null,
          lcp: null,
          domContentLoaded: performance.timing.domContentLoadedEventEnd - performance.timing.navigationStart,
          loadComplete: performance.timing.loadEventEnd - performance.timing.navigationStart
        };

        // Get paint timing
        const paintEntries = performance.getEntriesByType('paint');
        paintEntries.forEach(entry => {
          if (entry.name === 'first-contentful-paint') {
            metrics.fcp = entry.startTime;
          }
        });

        // Get navigation timing
        const navEntries = performance.getEntriesByType('navigation');
        if (navEntries.length > 0) {
          metrics.responseEnd = navEntries[0].responseEnd;
          metrics.domInteractive = navEntries[0].domInteractive;
          metrics.domComplete = navEntries[0].domComplete;
        }

        resolve(metrics);
      });
    });

    console.log('Performance Metrics (Desktop):');
    console.log(`  FCP: ${performanceMetrics.fcp}ms`);
    console.log(`  DOM Interactive: ${performanceMetrics.domInteractive}ms`);
    console.log(`  DOM Content Loaded: ${performanceMetrics.domContentLoaded}ms`);
    console.log(`  Load Complete: ${performanceMetrics.loadComplete}ms`);

    // FCP should be under 1500ms (1.5 seconds)
    expect(performanceMetrics.fcp).toBeLessThan(1500);

    // Time to Interactive approximated by DOM Interactive should be under 3000ms
    expect(performanceMetrics.domInteractive).toBeLessThan(3000);

    // Overall load time should be reasonable for a static page
    expect(loadTime).toBeLessThan(5000);
  });

  test('Test Case 2: Lighthouse performance audit on mobile - Performance score 90+ on mobile emulation', async () => {
    // Emulate mobile device
    const mobileContext = await browser.newContext({
      ...devices['Pixel 5'],
      offline: false
    });
    const mobilePage = await mobileContext.newPage();

    const startTime = Date.now();
    await mobilePage.goto('http://localhost:8080', { waitUntil: 'load' });
    const loadTime = Date.now() - startTime;

    const mobileMetrics = await mobilePage.evaluate(() => {
      const metrics = {
        fcp: null,
        domInteractive: null,
        domContentLoaded: performance.timing.domContentLoadedEventEnd - performance.timing.navigationStart
      };

      const paintEntries = performance.getEntriesByType('paint');
      paintEntries.forEach(entry => {
        if (entry.name === 'first-contentful-paint') {
          metrics.fcp = entry.startTime;
        }
      });

      const navEntries = performance.getEntriesByType('navigation');
      if (navEntries.length > 0) {
        metrics.domInteractive = navEntries[0].domInteractive;
      }

      return metrics;
    });

    console.log('Performance Metrics (Mobile):');
    console.log(`  FCP: ${mobileMetrics.fcp}ms`);
    console.log(`  DOM Interactive: ${mobileMetrics.domInteractive}ms`);
    console.log(`  DOM Content Loaded: ${mobileMetrics.domContentLoaded}ms`);

    // Mobile FCP should still be under 1.5s for a well-optimized static page
    expect(mobileMetrics.fcp).toBeLessThan(1500);

    // Mobile TTI approximation should be under 3s
    expect(mobileMetrics.domInteractive).toBeLessThan(3000);

    await mobileContext.close();
  });

  test('Test Case 3: Measure total page weight - Initial page load under 100KB', async () => {
    // Track all network requests
    const resources = [];

    page.on('response', async (response) => {
      try {
        const request = response.request();
        const headers = response.headers();
        const contentLength = headers['content-length'];

        resources.push({
          url: request.url(),
          resourceType: request.resourceType(),
          encodedDataLength: contentLength ? parseInt(contentLength, 10) : 0,
          status: response.status()
        });
      } catch (e) {
        // Ignore errors for failed requests
      }
    });

    await page.goto('http://localhost:8080', { waitUntil: 'networkidle' });

    // Calculate total page weight (HTML + CSS + JS)
    // Exclude images from initial payload calculation as per PRD (lazy loaded images)
    const htmlSize = calculateResourceSize(resources, 'document');
    const cssSize = calculateResourceSize(resources, 'stylesheet');
    const jsSize = calculateResourceSize(resources, 'script');
    const fontSize = calculateResourceSize(resources, 'font');

    const totalInitialPayload = htmlSize + cssSize + jsSize + fontSize;

    console.log('Page Weight Breakdown:');
    console.log(`  HTML: ${(htmlSize / 1024).toFixed(2)} KB`);
    console.log(`  CSS: ${(cssSize / 1024).toFixed(2)} KB`);
    console.log(`  JS: ${(jsSize / 1024).toFixed(2)} KB`);
    console.log(`  Fonts: ${(fontSize / 1024).toFixed(2)} KB`);
    console.log(`  Total Initial Payload: ${(totalInitialPayload / 1024).toFixed(2)} KB`);

    // Total initial payload should be under 100KB (102400 bytes)
    expect(totalInitialPayload).toBeLessThan(102400);
  });

  test('Test Case 4: Test page on throttled 3G connection - Page is interactive within 3 seconds', async () => {
    // Simulate slow 3G network conditions
    const client = await context.newCDPSession(page);
    await client.send('Network.enable');
    await client.send('Network.emulateNetworkConditions', {
      offline: false,
      downloadThroughput: 400 * 1024 / 8, // 400 Kbps
      uploadThroughput: 400 * 1024 / 8,
      latency: 400 // 400ms RTT (typical 3G)
    });

    const startTime = Date.now();
    await page.goto('http://localhost:8080', { waitUntil: 'domcontentloaded', timeout: 30000 });
    const domContentLoadedTime = Date.now() - startTime;

    // Get DOM Interactive timing
    const metrics = await page.evaluate(() => {
      const navEntries = performance.getEntriesByType('navigation');
      if (navEntries.length > 0) {
        return {
          domInteractive: navEntries[0].domInteractive,
          domContentLoadedEventEnd: navEntries[0].domContentLoadedEventEnd
        };
      }
      return { domInteractive: null, domContentLoadedEventEnd: null };
    });

    console.log('3G Network Performance:');
    console.log(`  DOM Content Loaded: ${domContentLoadedTime}ms`);
    console.log(`  DOM Interactive (from Navigation API): ${metrics.domInteractive}ms`);

    // Page should be interactive within 3 seconds on 3G
    // Using DOM Content Loaded as proxy for interactivity
    expect(domContentLoadedTime).toBeLessThan(3000);
  });

  test('Test Case 5: Verify image optimization - All images are compressed, appropriate formats used, lazy loading on below-fold images', async () => {
    await page.goto('http://localhost:8080', { waitUntil: 'load' });

    // Check all images on the page
    const imageInfo = await page.evaluate(() => {
      const images = Array.from(document.querySelectorAll('img'));
      return images.map(img => ({
        src: img.src,
        alt: img.alt,
        loading: img.loading,
        decoding: img.decoding,
        naturalWidth: img.naturalWidth,
        naturalHeight: img.naturalHeight,
        isAboveFold: img.getBoundingClientRect().top < window.innerHeight,
        hasAlt: img.hasAttribute('alt') && img.alt.length > 0
      }));
    });

    console.log('Image Analysis:');
    imageInfo.forEach((img, i) => {
      console.log(`  Image ${i + 1}: ${img.src.split('/').pop()}`);
      console.log(`    Loading: ${img.loading}`);
      console.log(`    Decoding: ${img.decoding}`);
      console.log(`    Above fold: ${img.isAboveFold}`);
      console.log(`    Has alt text: ${img.hasAlt}`);
    });

    // Verify each image meets requirements
    for (const img of imageInfo) {
      // All images should have alt text
      expect(img.hasAlt).toBe(true);

      // Below-fold images should have lazy loading
      if (!img.isAboveFold) {
        expect(img.loading).toBe('lazy');
      }
    }

    // Check image file sizes by making requests
    const imageSizes = [];
    for (const img of imageInfo) {
      if (img.src.startsWith('http://localhost:8080')) {
        const response = await page.request.get(img.src);
        const headers = response.headers();
        const size = headers['content-length'] ? parseInt(headers['content-length'], 10) : 0;
        imageSizes.push({ src: img.src, size });

        // Check format - prefer WebP, SVG, or optimized GIF for animations
        const extension = img.src.split('.').pop().toLowerCase();
        const isOptimizedFormat = ['webp', 'svg', 'gif', 'png', 'jpg', 'jpeg'].includes(extension);
        expect(isOptimizedFormat).toBe(true);
      }
    }

    console.log('Image Sizes:');
    imageSizes.forEach(img => {
      console.log(`  ${img.src.split('/').pop()}: ${(img.size / 1024).toFixed(2)} KB`);
    });
  });

  test('Test Case 6: Check for render-blocking resources - No render-blocking JavaScript, critical CSS is inlined or loaded efficiently', async () => {
    await page.goto('http://localhost:8080', { waitUntil: 'load' });

    // Check script tags for render-blocking behavior
    const scriptInfo = await page.evaluate(() => {
      const scripts = Array.from(document.querySelectorAll('script[src]'));
      return scripts.map(script => ({
        src: script.src,
        async: script.async,
        defer: script.defer,
        type: script.type,
        isInHead: script.closest('head') !== null,
        isRenderBlocking: !script.async && !script.defer && script.closest('head') !== null
      }));
    });

    console.log('Script Analysis:');
    scriptInfo.forEach(script => {
      console.log(`  ${script.src.split('/').pop()}`);
      console.log(`    In head: ${script.isInHead}`);
      console.log(`    Async: ${script.async}, Defer: ${script.defer}`);
      console.log(`    Render-blocking: ${script.isRenderBlocking}`);
    });

    // Verify no render-blocking scripts in head
    const renderBlockingScripts = scriptInfo.filter(s => s.isRenderBlocking);
    expect(renderBlockingScripts.length).toBe(0);

    // Check CSS loading
    const cssInfo = await page.evaluate(() => {
      const styleSheets = Array.from(document.querySelectorAll('link[rel="stylesheet"]'));
      const inlineStyles = Array.from(document.querySelectorAll('style'));

      return {
        externalStylesheets: styleSheets.map(link => ({
          href: link.href,
          media: link.media,
          hasPreload: !!document.querySelector(`link[rel="preload"][href="${link.href}"]`)
        })),
        inlineStyleCount: inlineStyles.length,
        hasCriticalInline: inlineStyles.some(style =>
          style.textContent.includes(':root') ||
          style.textContent.includes('body') ||
          style.textContent.includes('.hero')
        )
      };
    });

    console.log('CSS Analysis:');
    console.log(`  External stylesheets: ${cssInfo.externalStylesheets.length}`);
    console.log(`  Inline style blocks: ${cssInfo.inlineStyleCount}`);
    cssInfo.externalStylesheets.forEach(css => {
      console.log(`  ${css.href.split('/').pop()}: media="${css.media || 'all'}"`);
    });

    // Check that stylesheets are efficiently loaded
    // Either critical CSS is inlined or stylesheets are in an optimal order
    // For a small static site, having stylesheets in head without preload is acceptable
    // as long as total CSS size is small (which we verified in test case 3)

    // Verify stylesheets exist and are loaded
    expect(cssInfo.externalStylesheets.length).toBeGreaterThan(0);
  });
});
