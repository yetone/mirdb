/**
 * Performance E2E Tests
 * Owner: Scenario 11 - Performance - Page Load Time
 *
 * Tests for NFR-1: Page load time must be under 2 seconds on standard broadband.
 * Validates Core Web Vitals metrics:
 * - Time to Interactive (TTI)
 * - Largest Contentful Paint (LCP)
 * - First Input Delay (FID)
 * - Cumulative Layout Shift (CLS)
 * - Total page size
 * - Image optimization
 */

import { test, expect } from '@playwright/test';

const BASE_URL = 'http://localhost:3000';

test.describe('Performance - Page Load Time', () => {
  test.beforeEach(async ({ page }) => {
    // Clear cache by setting a fresh context for each test
    await page.context().clearCookies();
  });

  test('TC1: Time to Interactive (TTI) is under 2000ms on broadband', async ({ page }) => {
    // Start timing before navigation
    const startTime = Date.now();

    // Navigate to the page and wait for network to be idle
    await page.goto(BASE_URL, { waitUntil: 'networkidle' });

    // Wait for the page to be interactive (DOM content loaded and main content visible)
    await page.waitForLoadState('domcontentloaded');

    // Ensure key interactive elements are present
    await page.locator('#hero').waitFor({ state: 'visible' });
    await page.locator('.btn-primary').waitFor({ state: 'visible' });

    const endTime = Date.now();
    const loadTime = endTime - startTime;

    // Measure performance using Navigation Timing API
    const performanceMetrics = await page.evaluate(() => {
      const timing = performance.timing;
      const navigationEntry = performance.getEntriesByType('navigation')[0];

      return {
        // Time from navigation start to DOM interactive
        domInteractive: navigationEntry
          ? navigationEntry.domInteractive
          : timing.domInteractive - timing.navigationStart,
        // Time from navigation start to DOM content loaded
        domContentLoaded: navigationEntry
          ? navigationEntry.domContentLoadedEventEnd
          : timing.domContentLoadedEventEnd - timing.navigationStart,
        // Time from navigation start to load event
        loadEventEnd: navigationEntry
          ? navigationEntry.loadEventEnd
          : timing.loadEventEnd - timing.navigationStart,
      };
    });

    // TTI should be under 2000ms (using domInteractive as proxy)
    expect(performanceMetrics.domInteractive).toBeLessThan(2000);

    // Also verify the overall load time measured externally
    expect(loadTime).toBeLessThan(2000);
  });

  test('TC2: Largest Contentful Paint (LCP) is under 2.5 seconds', async ({ page }) => {
    // Navigate to the page first
    await page.goto(BASE_URL, { waitUntil: 'domcontentloaded' });

    // Set up LCP measurement after navigation
    const lcpValue = await page.evaluate(() => {
      return new Promise((resolve) => {
        let lcpValue = 0;

        // Check for buffered LCP entries first
        const entries = performance.getEntriesByType('largest-contentful-paint');
        if (entries.length > 0) {
          lcpValue = entries[entries.length - 1].startTime;
        }

        const observer = new PerformanceObserver((list) => {
          const newEntries = list.getEntries();
          if (newEntries.length > 0) {
            lcpValue = newEntries[newEntries.length - 1].startTime;
          }
        });

        try {
          observer.observe({ type: 'largest-contentful-paint', buffered: true });
        } catch {
          // PerformanceObserver for LCP may not be supported
        }

        // Give time for any additional LCP entries
        setTimeout(() => {
          observer.disconnect();
          resolve(lcpValue);
        }, 1000);
      });
    });

    // LCP should be under 2500ms (2.5 seconds)
    expect(lcpValue).toBeLessThan(2500);
  });

  test('TC3: First Input Delay (FID) is under 100ms', async ({ page }) => {
    // Navigate to the page
    await page.goto(BASE_URL);
    await page.waitForLoadState('domcontentloaded');

    // Set up FID measurement before interaction
    const fidPromise = page.evaluate(() => {
      return new Promise((resolve) => {
        let fidValue = 0;

        const observer = new PerformanceObserver((list) => {
          const entries = list.getEntries();
          if (entries.length > 0) {
            fidValue = entries[0].processingStart - entries[0].startTime;
          }
        });

        observer.observe({ type: 'first-input', buffered: true });

        // Give time for FID to be measured
        setTimeout(() => {
          observer.disconnect();
          resolve(fidValue);
        }, 2000);
      });
    });

    // Trigger a user interaction (click on button)
    const button = page.locator('.btn-secondary');
    await button.waitFor({ state: 'visible' });
    await button.click();

    // Wait for FID measurement
    const fidValue = await fidPromise;

    // FID should be under 100ms
    // Note: If FID is 0, it means no input delay was detected (which is good)
    expect(fidValue).toBeLessThanOrEqual(100);
  });

  test('TC4: Cumulative Layout Shift (CLS) is under 0.1', async ({ page }) => {
    // Navigate to the page first
    await page.goto(BASE_URL, { waitUntil: 'networkidle' });

    // Scroll through the page to trigger any lazy-loaded content
    await page.evaluate(() => {
      window.scrollTo(0, document.body.scrollHeight / 2);
    });
    await page.waitForTimeout(300);
    await page.evaluate(() => {
      window.scrollTo(0, document.body.scrollHeight);
    });
    await page.waitForTimeout(300);
    await page.evaluate(() => {
      window.scrollTo(0, 0);
    });
    await page.waitForTimeout(300);

    // Measure CLS after scrolling
    const clsValue = await page.evaluate(() => {
      return new Promise((resolve) => {
        let clsValue = 0;

        // Check for buffered layout shift entries
        const entries = performance.getEntriesByType('layout-shift');
        for (const entry of entries) {
          if (!entry.hadRecentInput) {
            clsValue += entry.value;
          }
        }

        const observer = new PerformanceObserver((list) => {
          for (const entry of list.getEntries()) {
            if (!entry.hadRecentInput) {
              clsValue += entry.value;
            }
          }
        });

        try {
          observer.observe({ type: 'layout-shift', buffered: true });
        } catch {
          // PerformanceObserver for layout-shift may not be supported
        }

        // Give brief time for any final layout shifts
        setTimeout(() => {
          observer.disconnect();
          resolve(clsValue);
        }, 500);
      });
    });

    // CLS should be under 0.1
    expect(clsValue).toBeLessThan(0.1);
  });

  test('TC5: Total page size (HTML + CSS + JS + images) is under 500KB', async ({ page }) => {
    // Track all resource sizes
    const resourceSizes = [];

    // Listen for all network responses
    page.on('response', async (response) => {
      try {
        const url = response.url();
        const request = response.request();

        // Only count resources from our domain or relative paths
        if (url.startsWith('http://localhost:') || url.startsWith('/')) {
          const resourceType = request.resourceType();

          // Include HTML, CSS, JS, and images
          if (['document', 'stylesheet', 'script', 'image', 'font'].includes(resourceType)) {
            const headers = response.headers();
            const contentLength = headers['content-length'];

            if (contentLength) {
              resourceSizes.push({
                url: url,
                type: resourceType,
                size: parseInt(contentLength, 10),
              });
            } else {
              // Try to get body size if content-length not available
              try {
                const body = await response.body();
                resourceSizes.push({
                  url: url,
                  type: resourceType,
                  size: body.length,
                });
              } catch {
                // Response may not have a body (e.g., 304)
              }
            }
          }
        }
      } catch {
        // Ignore errors for failed requests
      }
    });

    // Navigate to the page
    await page.goto(BASE_URL, { waitUntil: 'networkidle' });

    // Calculate total size
    const totalSize = resourceSizes.reduce((sum, resource) => sum + resource.size, 0);
    const totalSizeKB = totalSize / 1024;

    // Total page size should be under 500KB
    expect(totalSizeKB).toBeLessThan(500);
  });

  test('TC6: Images use modern formats (WebP/AVIF) or are SVG', async ({ page }) => {
    // Navigate to the page
    await page.goto(BASE_URL, { waitUntil: 'networkidle' });

    // Get all image elements and their sources
    const images = await page.evaluate(() => {
      const imgElements = document.querySelectorAll('img');
      const imageData = [];

      imgElements.forEach((img) => {
        const src = img.src || img.getAttribute('src');
        if (src) {
          imageData.push({
            src: src,
            alt: img.alt,
          });
        }
      });

      // Also check background images in stylesheets
      const allElements = document.querySelectorAll('*');
      allElements.forEach((el) => {
        const bgImage = getComputedStyle(el).backgroundImage;
        if (bgImage && bgImage !== 'none') {
          const urlMatch = bgImage.match(/url\(['"]?([^'"]+)['"]?\)/);
          if (urlMatch) {
            imageData.push({
              src: urlMatch[1],
              alt: 'background-image',
            });
          }
        }
      });

      return imageData;
    });

    // Check that all images use modern formats
    const allowedFormats = ['.svg', '.webp', '.avif'];
    const disallowedImages = [];

    for (const image of images) {
      const src = image.src.toLowerCase();

      // Skip data URIs and inline SVGs
      if (src.startsWith('data:')) {
        // Data URIs for SVGs are acceptable
        if (src.startsWith('data:image/svg+xml')) {
          continue;
        }
        // Check if it's a modern format data URI
        if (src.startsWith('data:image/webp') || src.startsWith('data:image/avif')) {
          continue;
        }
      }

      // Check if the image uses an allowed format
      const isAllowedFormat = allowedFormats.some((format) => src.endsWith(format));

      if (!isAllowedFormat && !src.startsWith('data:')) {
        disallowedImages.push(image.src);
      }
    }

    // All images should use modern formats
    expect(
      disallowedImages,
      `Images not using modern formats: ${disallowedImages.join(', ')}`
    ).toHaveLength(0);
  });

  test('Page loads within acceptable time on throttled connection (Fast 3G)', async ({
    browser,
  }) => {
    // Create a context with throttled network
    const context = await browser.newContext();
    const page = await context.newPage();

    // Simulate Fast 3G connection using CDP
    const client = await context.newCDPSession(page);
    await client.send('Network.enable');
    await client.send('Network.emulateNetworkConditions', {
      offline: false,
      downloadThroughput: (1.6 * 1024 * 1024) / 8, // 1.6 Mbps
      uploadThroughput: (750 * 1024) / 8, // 750 Kbps
      latency: 150, // 150ms RTT
    });

    const startTime = Date.now();

    // Navigate to the page
    await page.goto(BASE_URL, { waitUntil: 'domcontentloaded' });

    // Wait for main content to be visible
    await page.locator('#hero').waitFor({ state: 'visible' });

    const loadTime = Date.now() - startTime;

    // On Fast 3G, we allow more time but still expect reasonable performance
    // For a well-optimized static page, 5 seconds should be achievable
    expect(loadTime).toBeLessThan(5000);

    await context.close();
  });

  test('All CSS and JS resources are optimized', async ({ page }) => {
    const resourceInfo = [];

    page.on('response', async (response) => {
      const url = response.url();
      const request = response.request();

      if (url.startsWith('http://localhost:')) {
        const resourceType = request.resourceType();

        if (resourceType === 'stylesheet' || resourceType === 'script') {
          const headers = response.headers();
          const contentLength = headers['content-length'];

          if (contentLength) {
            resourceInfo.push({
              url: url,
              type: resourceType,
              size: parseInt(contentLength, 10),
            });
          }
        }
      }
    });

    await page.goto(BASE_URL, { waitUntil: 'networkidle' });

    // Individual CSS/JS files should be reasonably sized (under 100KB each)
    for (const resource of resourceInfo) {
      const sizeKB = resource.size / 1024;
      expect(sizeKB, `${resource.url} is too large (${sizeKB.toFixed(2)}KB)`).toBeLessThan(100);
    }
  });
});
