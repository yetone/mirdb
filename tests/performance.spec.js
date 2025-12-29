// @ts-check
const { test, expect } = require('@playwright/test');
const fs = require('fs');
const path = require('path');

/**
 * Performance Tests
 * Tests that verify the homepage meets performance requirements
 * - Page loads within 3 seconds
 * - Total page size under 2MB recommended
 * - Images are compressed and appropriately sized
 * - Minimal render-blocking resources
 * - Below-fold images use lazy loading
 */

test.describe('Performance Requirements', () => {
  test.beforeEach(async ({ page }) => {
    // Clear cache before each test for accurate measurements
    await page.context().clearCookies();
  });

  test('TC1: Page loads within 3 seconds on standard connection', async ({ page }) => {
    const MAX_LOAD_TIME_MS = 3000;

    // Start timing
    const startTime = Date.now();

    // Navigate and wait for the page to be fully loaded (domcontentloaded + networkidle)
    await page.goto('/', { waitUntil: 'domcontentloaded' });

    const domContentLoadedTime = Date.now() - startTime;

    // Wait for network to be idle (all resources loaded)
    await page.waitForLoadState('networkidle', { timeout: 10000 }).catch(() => {
      // Network idle timeout is acceptable - we'll measure what we have
    });

    const fullLoadTime = Date.now() - startTime;

    // Verify page content is visible
    await expect(page.locator('h1')).toContainText('MirDB');

    // Log performance metrics
    console.log(`DOM Content Loaded: ${domContentLoadedTime}ms`);
    console.log(`Full Page Load: ${fullLoadTime}ms`);

    // Check that DOM content loads within 3 seconds
    expect(domContentLoadedTime).toBeLessThanOrEqual(MAX_LOAD_TIME_MS);
  });

  test('TC2: Total page size is optimized (under 2MB recommended)', async ({ page }) => {
    const MAX_PAGE_SIZE_BYTES = 2 * 1024 * 1024; // 2MB in bytes

    let totalSize = 0;
    const resourceSizes = [];

    // Listen for all responses and track sizes
    page.on('response', async (response) => {
      try {
        const url = response.url();
        const headers = response.headers();
        let size = 0;

        // Get content-length if available
        if (headers['content-length']) {
          size = parseInt(headers['content-length'], 10);
        } else {
          // Try to get body size
          try {
            const body = await response.body();
            size = body.length;
          } catch (e) {
            // Some responses may not have a body
          }
        }

        if (size > 0) {
          totalSize += size;
          resourceSizes.push({
            url: url.split('/').pop() || url,
            size: size,
            sizeKB: (size / 1024).toFixed(2)
          });
        }
      } catch (e) {
        // Ignore errors from cross-origin or failed requests
      }
    });

    await page.goto('/', { waitUntil: 'networkidle' });

    // Sort by size descending for reporting
    resourceSizes.sort((a, b) => b.size - a.size);

    // Log the largest resources
    console.log('\nLargest resources:');
    resourceSizes.slice(0, 10).forEach((r) => {
      console.log(`  ${r.url}: ${r.sizeKB} KB`);
    });
    console.log(`\nTotal page size: ${(totalSize / 1024 / 1024).toFixed(2)} MB`);

    // Check total page size is under 2MB
    expect(totalSize).toBeLessThanOrEqual(MAX_PAGE_SIZE_BYTES);
  });

  test('TC3: Images are compressed and appropriately sized', async ({ page }) => {
    const MAX_IMAGE_SIZE_BYTES = 500 * 1024; // 500KB max per image (reasonable for web)
    const imageSizes = [];

    // Track image responses
    page.on('response', async (response) => {
      try {
        const url = response.url();
        const contentType = response.headers()['content-type'] || '';

        // Check if it's an image
        if (contentType.includes('image') || /\.(png|jpg|jpeg|gif|webp|svg|ico)(\?|$)/i.test(url)) {
          let size = 0;

          if (response.headers()['content-length']) {
            size = parseInt(response.headers()['content-length'], 10);
          } else {
            try {
              const body = await response.body();
              size = body.length;
            } catch (e) {
              // Ignore
            }
          }

          if (size > 0) {
            imageSizes.push({
              url: url.split('/').pop() || url,
              fullUrl: url,
              size: size,
              sizeKB: (size / 1024).toFixed(2),
              type: contentType
            });
          }
        }
      } catch (e) {
        // Ignore errors
      }
    });

    await page.goto('/', { waitUntil: 'networkidle' });

    // Log all images and their sizes
    console.log('\nImage sizes:');
    imageSizes.forEach((img) => {
      const status = img.size > MAX_IMAGE_SIZE_BYTES ? '[TOO LARGE]' : '[OK]';
      console.log(`  ${status} ${img.url}: ${img.sizeKB} KB`);
    });

    // Check all images are under the max size
    const oversizedImages = imageSizes.filter((img) => img.size > MAX_IMAGE_SIZE_BYTES);

    if (oversizedImages.length > 0) {
      console.log('\nOversized images that need compression:');
      oversizedImages.forEach((img) => {
        console.log(`  - ${img.url}: ${img.sizeKB} KB (max: ${MAX_IMAGE_SIZE_BYTES / 1024} KB)`);
      });
    }

    expect(oversizedImages.length).toBe(0);
  });

  test('TC4: Critical CSS is inlined or minimal render-blocking scripts', async ({ page }) => {
    const renderBlockingResources = [];

    // Navigate to page and check for render-blocking resources
    await page.goto('/');

    // Get all stylesheets that are render-blocking (in head without async/defer)
    const stylesheets = await page.evaluate(() => {
      const links = document.querySelectorAll('link[rel="stylesheet"]');
      return Array.from(links).map((link) => ({
        href: link.getAttribute('href'),
        media: link.getAttribute('media'),
        // Check if it has preload or non-blocking media query
        isBlocking: !link.getAttribute('media') || link.getAttribute('media') === 'all'
      }));
    });

    // Get all scripts that are render-blocking (in head without async/defer)
    const scripts = await page.evaluate(() => {
      const scriptTags = document.querySelectorAll('head script[src]');
      return Array.from(scriptTags).map((script) => ({
        src: script.getAttribute('src'),
        async: script.hasAttribute('async'),
        defer: script.hasAttribute('defer'),
        type: script.getAttribute('type'),
        isBlocking: !script.hasAttribute('async') && !script.hasAttribute('defer')
      }));
    });

    console.log('\nStylesheets:');
    stylesheets.forEach((css) => {
      const status = css.isBlocking ? '[BLOCKING]' : '[OK]';
      console.log(`  ${status} ${css.href} (media: ${css.media || 'all'})`);
      if (css.isBlocking) {
        renderBlockingResources.push({ type: 'stylesheet', url: css.href });
      }
    });

    console.log('\nScripts in <head>:');
    scripts.forEach((script) => {
      const status = script.isBlocking ? '[BLOCKING]' : '[OK]';
      console.log(`  ${status} ${script.src} (async: ${script.async}, defer: ${script.defer})`);
      if (script.isBlocking) {
        renderBlockingResources.push({ type: 'script', url: script.src });
      }
    });

    // Check if there's inline critical CSS
    const hasInlineStyles = await page.evaluate(() => {
      const styleElements = document.querySelectorAll('head style');
      return styleElements.length > 0;
    });

    console.log(`\nInline critical CSS: ${hasInlineStyles ? 'Yes' : 'No'}`);

    // For a static site, one external CSS file is acceptable
    // No render-blocking JS in head is the goal
    const blockingScripts = scripts.filter((s) => s.isBlocking);
    const blockingStylesheets = stylesheets.filter((s) => s.isBlocking);

    console.log(`\nRender-blocking scripts: ${blockingScripts.length}`);
    console.log(`Render-blocking stylesheets: ${blockingStylesheets.length}`);

    // Allow 1 render-blocking stylesheet (main CSS) but no blocking scripts
    expect(blockingScripts.length).toBe(0);
    expect(blockingStylesheets.length).toBeLessThanOrEqual(1);
  });

  test('TC5: Below-fold images use lazy loading', async ({ page }) => {
    await page.goto('/');

    // Get all images on the page
    const images = await page.evaluate(() => {
      const imgs = document.querySelectorAll('img');
      const viewportHeight = window.innerHeight;

      return Array.from(imgs).map((img) => {
        const rect = img.getBoundingClientRect();
        const isAboveFold = rect.top < viewportHeight;
        const hasLazyLoading = img.getAttribute('loading') === 'lazy';
        const hasSrcset = img.hasAttribute('srcset');

        return {
          src: img.getAttribute('src'),
          alt: img.getAttribute('alt'),
          loading: img.getAttribute('loading'),
          hasLazyLoading,
          isAboveFold,
          top: rect.top,
          viewportHeight,
          hasSrcset
        };
      });
    });

    console.log('\nImage lazy loading analysis:');
    images.forEach((img) => {
      const location = img.isAboveFold ? '[Above fold]' : '[Below fold]';
      const lazyStatus = img.hasLazyLoading ? '[lazy]' : '[eager/default]';
      console.log(`  ${location} ${lazyStatus} ${img.src} (top: ${img.top.toFixed(0)}px)`);
    });

    // Check below-fold images have lazy loading
    const belowFoldImages = images.filter((img) => !img.isAboveFold);
    const belowFoldWithoutLazy = belowFoldImages.filter((img) => !img.hasLazyLoading);

    console.log(`\nTotal images: ${images.length}`);
    console.log(`Above-fold images: ${images.length - belowFoldImages.length}`);
    console.log(`Below-fold images: ${belowFoldImages.length}`);
    console.log(`Below-fold without lazy loading: ${belowFoldWithoutLazy.length}`);

    if (belowFoldWithoutLazy.length > 0) {
      console.log('\nBelow-fold images needing lazy loading:');
      belowFoldWithoutLazy.forEach((img) => {
        console.log(`  - ${img.src}`);
      });
    }

    // All below-fold images should have lazy loading
    // If there are no below-fold images, this test passes
    if (belowFoldImages.length > 0) {
      expect(belowFoldWithoutLazy.length).toBe(0);
    }
  });
});
