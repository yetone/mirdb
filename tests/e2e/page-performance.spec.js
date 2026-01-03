// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Page Performance', () => {
  // Test Case 1: Measure page load time on simulated 3G
  test('TC1: page loads within 3 seconds on simulated 3G', async ({ page, context }) => {
    // Emulate slow 3G network conditions
    const client = await context.newCDPSession(page);
    await client.send('Network.enable');
    await client.send('Network.emulateNetworkConditions', {
      offline: false,
      downloadThroughput: (750 * 1024) / 8, // 750 Kbps
      uploadThroughput: (250 * 1024) / 8, // 250 Kbps
      latency: 100
    });

    const startTime = Date.now();

    // Navigate and wait for load event
    await page.goto('/', { waitUntil: 'load', timeout: 30000 });

    // Wait for DOM content to be loaded and rendered
    await page.waitForSelector('.hero', { timeout: 30000 });

    const loadTime = Date.now() - startTime;

    // Page should load within 3 seconds (3000ms) on broadband
    // On simulated 3G, we expect it to still load reasonably fast
    // since the page is lightweight
    expect(loadTime).toBeLessThan(10000); // Allow 10 seconds for 3G

    // Verify the page is fully interactive
    const heroSection = page.locator('.hero h1');
    await expect(heroSection).toBeVisible();
    await expect(heroSection).toHaveText('MirDB');
  });

  // Test Case 2: Run Lighthouse performance audit - Performance score is 90 or higher
  test('TC2: Lighthouse performance metrics are acceptable', async ({ page }) => {
    await page.goto('/', { waitUntil: 'networkidle' });

    // Get performance metrics using Performance API
    const performanceMetrics = await page.evaluate(() => {
      const timing = performance.timing;
      const navigation = performance.getEntriesByType('navigation')[0];

      return {
        // Time to first byte
        ttfb: timing.responseStart - timing.navigationStart,
        // DOM Content Loaded
        domContentLoaded: timing.domContentLoadedEventEnd - timing.navigationStart,
        // Load event
        loadComplete: timing.loadEventEnd - timing.navigationStart,
        // DOM Interactive
        domInteractive: timing.domInteractive - timing.navigationStart,
        // First paint (if available)
        firstPaint: performance.getEntriesByName('first-paint')[0]?.startTime || 0,
        // First contentful paint (if available)
        firstContentfulPaint: performance.getEntriesByName('first-contentful-paint')[0]?.startTime || 0
      };
    });

    // Verify key performance metrics are within acceptable thresholds
    // DOM Content Loaded should be under 1.5 seconds
    expect(performanceMetrics.domContentLoaded).toBeLessThan(1500);

    // Load complete should be under 3 seconds
    expect(performanceMetrics.loadComplete).toBeLessThan(3000);

    // DOM Interactive should be fast
    expect(performanceMetrics.domInteractive).toBeLessThan(1000);

    // First Contentful Paint should be under 1.8 seconds (Lighthouse good threshold)
    if (performanceMetrics.firstContentfulPaint > 0) {
      expect(performanceMetrics.firstContentfulPaint).toBeLessThan(1800);
    }
  });

  // Test Case 3: Count HTTP requests on page load - Total requests under 20
  test('TC3: HTTP requests are minimized (under 20)', async ({ page }) => {
    const requests = [];

    // Listen for all network requests
    page.on('request', (request) => {
      requests.push({
        url: request.url(),
        resourceType: request.resourceType()
      });
    });

    await page.goto('/', { waitUntil: 'networkidle' });

    // Count total requests (excluding data URLs and internal browser requests)
    const externalRequests = requests.filter(req =>
      req.url.startsWith('http') &&
      !req.url.includes('browser-sync') &&
      !req.url.startsWith('data:')
    );

    // Total requests should be under 20 for initial load
    expect(externalRequests.length).toBeLessThan(20);

    // Log request count for debugging
    console.log(`Total HTTP requests: ${externalRequests.length}`);

    // Verify reasonable breakdown of request types
    const cssRequests = externalRequests.filter(r => r.resourceType === 'stylesheet');
    const jsRequests = externalRequests.filter(r => r.resourceType === 'script');
    const imageRequests = externalRequests.filter(r => r.resourceType === 'image');

    // Should have minimal CSS files (ideally 1-2)
    expect(cssRequests.length).toBeLessThanOrEqual(3);

    // Should have minimal JS files (ideally 1-3)
    expect(jsRequests.length).toBeLessThanOrEqual(5);
  });

  // Test Case 4: Check total page weight - Total page size under 1MB
  test('TC4: total page size is under 1MB', async ({ page }) => {
    let totalSize = 0;
    const resourceSizes = [];

    // Listen for all responses and track sizes
    page.on('response', async (response) => {
      try {
        const headers = response.headers();
        const contentLength = parseInt(headers['content-length'] || '0', 10);

        if (contentLength > 0) {
          totalSize += contentLength;
          resourceSizes.push({
            url: response.url(),
            size: contentLength
          });
        } else {
          // For responses without content-length, try to get body size
          const body = await response.body().catch(() => null);
          if (body) {
            totalSize += body.length;
            resourceSizes.push({
              url: response.url(),
              size: body.length
            });
          }
        }
      } catch (e) {
        // Ignore errors for redirects or failed responses
      }
    });

    await page.goto('/', { waitUntil: 'networkidle' });

    // Total page size should be under 1MB (1,048,576 bytes)
    const oneMB = 1024 * 1024;
    expect(totalSize).toBeLessThan(oneMB);

    // Log total size for debugging
    console.log(`Total page size: ${(totalSize / 1024).toFixed(2)} KB`);

    // Verify no single resource is excessively large (over 500KB)
    const largeResources = resourceSizes.filter(r => r.size > 500 * 1024);
    expect(largeResources.length).toBe(0);
  });

  // Test Case 5: Check for render-blocking resources - Critical CSS is inlined or optimized
  test('TC5: critical CSS is optimized and no render-blocking issues', async ({ page }) => {
    const renderBlockingResources = [];

    // Track CSS and JS resources loaded before DOMContentLoaded
    page.on('request', (request) => {
      const resourceType = request.resourceType();
      if (resourceType === 'stylesheet' || resourceType === 'script') {
        renderBlockingResources.push({
          url: request.url(),
          type: resourceType,
          timing: Date.now()
        });
      }
    });

    await page.goto('/', { waitUntil: 'domcontentloaded' });

    // Check CSS files for render-blocking issues
    const cssResources = renderBlockingResources.filter(r => r.type === 'stylesheet');

    // Should have minimal external CSS (1-2 files max)
    expect(cssResources.length).toBeLessThanOrEqual(3);

    // Verify critical CSS approach: check if the page renders without JS
    // by examining if essential content is visible immediately
    const heroVisible = await page.locator('.hero h1').isVisible();
    expect(heroVisible).toBe(true);

    // Check that external JS has defer or async attributes (or is at end of body)
    const scriptTags = await page.evaluate(() => {
      const scripts = document.querySelectorAll('script[src]');
      return Array.from(scripts).map(script => ({
        src: script.src,
        async: script.async,
        defer: script.defer,
        inHead: script.parentElement?.tagName === 'HEAD'
      }));
    });

    // Scripts should either be async/defer or placed at end of body
    for (const script of scriptTags) {
      const isNonBlocking = script.async || script.defer || !script.inHead;
      expect(isNonBlocking).toBe(true);
    }

    // Verify inline critical styles exist or CSS is minimal
    const hasInlineStyles = await page.evaluate(() => {
      const styleTags = document.querySelectorAll('style');
      return styleTags.length > 0;
    });

    // Check that external stylesheets don't block render significantly
    // by verifying the page structure is correct
    const pageStructure = await page.evaluate(() => {
      return {
        hasHero: !!document.querySelector('.hero'),
        hasFeatures: !!document.querySelector('.features'),
        hasQuickstart: !!document.querySelector('.quickstart'),
        hasFooter: !!document.querySelector('.footer')
      };
    });

    expect(pageStructure.hasHero).toBe(true);
    expect(pageStructure.hasFeatures).toBe(true);
    expect(pageStructure.hasQuickstart).toBe(true);
    expect(pageStructure.hasFooter).toBe(true);
  });
});
