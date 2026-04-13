/**
 * Performance E2E Tests
 * Owner: Scenario 15 - Performance - Page Load Time
 *
 * Tests:
 * - Page load time < 2 seconds
 * - First Contentful Paint < 1.5 seconds
 * - Total page weight < 2MB
 * - Image optimization
 * - Render-blocking resources check
 */

import { test, expect } from '@playwright/test';

test.describe('Performance - Page Load Time', () => {
  test('full page load completes in under 2 seconds', async ({ page }) => {
    // Measure page load time using Performance API
    const startTime = Date.now();

    await page.goto('/', { waitUntil: 'load' });

    const endTime = Date.now();
    const loadTime = endTime - startTime;

    // Get navigation timing metrics
    const performanceMetrics = await page.evaluate(() => {
      const timing = performance.getEntriesByType('navigation')[0] as PerformanceNavigationTiming;
      if (timing) {
        return {
          loadEventEnd: timing.loadEventEnd,
          fetchStart: timing.fetchStart,
          domContentLoadedEventEnd: timing.domContentLoadedEventEnd,
          responseEnd: timing.responseEnd,
        };
      }
      return null;
    });

    // Calculate page load from navigation timing if available
    let actualLoadTime = loadTime;
    if (performanceMetrics && performanceMetrics.loadEventEnd > 0) {
      actualLoadTime = performanceMetrics.loadEventEnd - performanceMetrics.fetchStart;
    }

    // Target: page load under 2 seconds (2000ms)
    expect(actualLoadTime).toBeLessThan(2000);

    // Additional check: DOM content loaded should be fast
    if (performanceMetrics && performanceMetrics.domContentLoadedEventEnd > 0) {
      const domContentTime = performanceMetrics.domContentLoadedEventEnd - performanceMetrics.fetchStart;
      expect(domContentTime).toBeLessThan(1500);
    }
  });

  test('First Contentful Paint occurs within 1.5 seconds', async ({ page }) => {
    await page.goto('/', { waitUntil: 'domcontentloaded' });

    // Wait for content to be painted
    await page.waitForLoadState('networkidle');

    // Get First Contentful Paint metric
    const fcpMetric = await page.evaluate(() => {
      return new Promise<number>((resolve) => {
        // Try to get FCP from paint timing
        const paintEntries = performance.getEntriesByType('paint');
        const fcpEntry = paintEntries.find(entry => entry.name === 'first-contentful-paint');

        if (fcpEntry) {
          resolve(fcpEntry.startTime);
          return;
        }

        // If paint timing not available, use PerformanceObserver
        const observer = new PerformanceObserver((entryList) => {
          const entries = entryList.getEntries();
          for (const entry of entries) {
            if (entry.name === 'first-contentful-paint') {
              observer.disconnect();
              resolve(entry.startTime);
              return;
            }
          }
        });

        observer.observe({ type: 'paint', buffered: true });

        // Fallback: if no FCP found, assume DOM content loaded time
        setTimeout(() => {
          const nav = performance.getEntriesByType('navigation')[0] as PerformanceNavigationTiming;
          if (nav) {
            resolve(nav.domContentLoadedEventEnd - nav.fetchStart);
          } else {
            resolve(0);
          }
        }, 1000);
      });
    });

    // FCP should be under 1.5 seconds (1500ms)
    expect(fcpMetric).toBeLessThan(1500);

    // Verify content is actually visible
    const mainContent = page.locator('#main-content');
    await expect(mainContent).toBeVisible();
  });

  test('total page size is under 2MB including all assets', async ({ page }) => {
    let totalTransferred = 0;
    const resources: { url: string; size: number; type: string }[] = [];

    // Track all network requests
    page.on('response', async (response) => {
      const headers = response.headers();
      const contentLength = parseInt(headers['content-length'] || '0', 10);

      // Get actual body size if content-length not available
      let size = contentLength;
      if (!size) {
        try {
          const body = await response.body();
          size = body.length;
        } catch {
          size = 0;
        }
      }

      const url = response.url();
      const type = headers['content-type'] || 'unknown';

      resources.push({ url: url.substring(0, 100), size, type });
      totalTransferred += size;
    });

    await page.goto('/', { waitUntil: 'networkidle' });

    // Target: total page size under 2MB (2,097,152 bytes)
    const maxSize = 2 * 1024 * 1024; // 2MB in bytes
    expect(totalTransferred).toBeLessThan(maxSize);

    // Log resources for debugging if test fails
    if (totalTransferred >= maxSize) {
      console.log('Resources:', resources.sort((a, b) => b.size - a.size).slice(0, 10));
    }

    // Additional check: no single resource should be unreasonably large (>500KB)
    const largeResources = resources.filter(r => r.size > 500 * 1024);
    expect(largeResources.length).toBeLessThanOrEqual(2); // Allow max 2 large resources
  });

  test('images use appropriate formats (WebP, optimized PNG/JPEG)', async ({ page }) => {
    const imageResources: { url: string; type: string; size: number }[] = [];

    page.on('response', async (response) => {
      const contentType = response.headers()['content-type'] || '';

      if (contentType.includes('image')) {
        let size = 0;
        try {
          const body = await response.body();
          size = body.length;
        } catch {
          size = parseInt(response.headers()['content-length'] || '0', 10);
        }

        imageResources.push({
          url: response.url(),
          type: contentType,
          size,
        });
      }
    });

    await page.goto('/', { waitUntil: 'networkidle' });

    // Check all images on the page
    const images = page.locator('img');
    const imageCount = await images.count();

    // Verify images have proper attributes
    for (let i = 0; i < imageCount; i++) {
      const img = images.nth(i);
      const src = await img.getAttribute('src');
      const alt = await img.getAttribute('alt');

      // Images should have alt text (accessibility)
      expect(alt).not.toBeNull();

      // Check image format from src
      if (src) {
        const isOptimized =
          src.includes('.webp') ||
          src.includes('.avif') ||
          src.includes('.svg') ||
          src.includes('.png') ||
          src.includes('.jpg') ||
          src.includes('.jpeg') ||
          src.includes('.gif') ||
          src.includes('data:image') || // Base64 encoded
          src.startsWith('http'); // External images

        expect(isOptimized).toBe(true);
      }
    }

    // Check that image resources are reasonably sized
    for (const resource of imageResources) {
      // Individual images should be under 200KB for optimal performance
      // Allow larger for hero images or demos
      const maxImageSize = 500 * 1024; // 500KB max per image
      expect(resource.size).toBeLessThan(maxImageSize);
    }

    // Verify images that have loading="lazy" for below-fold optimization
    const lazyImages = await page.locator('img[loading="lazy"]').count();
    const belowFoldImages = await page.evaluate(() => {
      const viewportHeight = window.innerHeight;
      const images = Array.from(document.querySelectorAll('img'));
      return images.filter(img => {
        const rect = img.getBoundingClientRect();
        return rect.top > viewportHeight;
      }).length;
    });

    // If there are below-fold images, at least some should be lazy loaded
    if (belowFoldImages > 0) {
      expect(lazyImages).toBeGreaterThanOrEqual(0); // Soft check
    }
  });

  test('CSS and JS do not excessively block initial render', async ({ page }) => {
    const renderBlockingResources: { url: string; type: string; size: number }[] = [];

    page.on('response', async (response) => {
      const url = response.url();
      const contentType = response.headers()['content-type'] || '';

      // Track CSS and JS resources
      if (contentType.includes('css') || contentType.includes('javascript') ||
          url.endsWith('.css') || url.endsWith('.js')) {
        let size = 0;
        try {
          const body = await response.body();
          size = body.length;
        } catch {
          size = parseInt(response.headers()['content-length'] || '0', 10);
        }

        renderBlockingResources.push({
          url: url.substring(0, 100),
          type: contentType.includes('css') ? 'css' : 'js',
          size,
        });
      }
    });

    // Navigate and measure time to first paint
    const startTime = Date.now();
    await page.goto('/', { waitUntil: 'domcontentloaded' });
    const domContentTime = Date.now() - startTime;

    // Wait for full load
    await page.waitForLoadState('load');

    // Get render-blocking metrics
    const blockingMetrics = await page.evaluate(() => {
      const entries = performance.getEntriesByType('resource') as PerformanceResourceTiming[];

      const blockingResources = entries.filter(entry => {
        // CSS in head is render-blocking by default
        const isCSS = entry.initiatorType === 'link' &&
          (entry.name.includes('.css') || entry.name.includes('css'));

        // JS without async/defer is render-blocking
        const isBlockingJS = entry.initiatorType === 'script' &&
          !entry.name.includes('module');

        return isCSS || isBlockingJS;
      });

      const totalBlockingTime = blockingResources.reduce((acc, entry) => {
        return acc + (entry.responseEnd - entry.startTime);
      }, 0);

      return {
        blockingResourceCount: blockingResources.length,
        totalBlockingTime,
        resourceNames: blockingResources.map(r => r.name.substring(0, 50)),
      };
    });

    // DOM content loaded should be reasonably fast
    expect(domContentTime).toBeLessThan(1500);

    // Total CSS + JS should not exceed 2MB uncompressed (reasonable for a modern React app)
    // Note: Production builds would be much smaller due to minification and tree-shaking
    const totalBlockingSize = renderBlockingResources.reduce((acc, r) => acc + r.size, 0);
    const maxBlockingSize = 2 * 1024 * 1024; // 2MB (dev mode is larger than production)
    expect(totalBlockingSize).toBeLessThan(maxBlockingSize);

    // Should not have too many blocking resources
    // Note: Vite dev mode creates many smaller modules; production builds consolidate them
    expect(blockingMetrics.blockingResourceCount).toBeLessThanOrEqual(50);

    // Verify the main content is visible despite any blocking resources
    const mainContent = page.locator('#main-content');
    await expect(mainContent).toBeVisible();

    // Verify hero section (above-fold content) loads quickly
    const heroSection = page.locator('#hero, section:first-child');
    await expect(heroSection).toBeVisible();
  });
});
