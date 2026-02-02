/**
 * Asset Loading and Optimization E2E Tests
 * Owner: Scenario 18 - Asset Loading and Optimization
 *
 * Tests:
 * - Logo image loads correctly
 * - Usage GIF loads correctly (if used)
 * - No broken images (404 errors)
 * - Total initial payload under 100KB (excluding images)
 * - Lazy loading works correctly
 */

import { test, expect } from '@playwright/test';

test.describe('Asset Loading and Optimization', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: MirDB logo displays without errors', async ({ page }) => {
    // Check for logo in hero section
    const heroLogo = page.locator('.hero__logo');

    // If hero logo exists, verify it loads
    const heroLogoCount = await heroLogo.count();
    if (heroLogoCount > 0) {
      await expect(heroLogo).toBeVisible();

      // Verify the image loaded successfully (naturalWidth > 0 means loaded)
      const isLoaded = await heroLogo.evaluate((img) => {
        return img.complete && img.naturalWidth > 0;
      });
      expect(isLoaded).toBe(true);
    }

    // Also check for logo in navigation
    const navLogo = page.locator('.nav-logo');
    await expect(navLogo).toBeVisible();
  });

  test('TC2: Usage demo GIF displays correctly if used', async ({ page }) => {
    // Check if usage.gif is used anywhere on the page
    const usageGifs = page.locator('img[src*="usage.gif"]');
    const usageGifCount = await usageGifs.count();

    if (usageGifCount > 0) {
      // If usage GIF is present, verify it loads
      const firstUsageGif = usageGifs.first();
      await expect(firstUsageGif).toBeVisible();

      const isLoaded = await firstUsageGif.evaluate((img) => {
        return img.complete && img.naturalWidth > 0;
      });
      expect(isLoaded).toBe(true);
    } else {
      // Test passes if usage.gif is not used on the page
      expect(usageGifCount).toBe(0);
    }
  });

  test('TC5: No 404 errors for image resources', async ({ page }) => {
    const failedRequests = [];

    // Listen for failed requests
    page.on('response', (response) => {
      if (response.status() === 404) {
        const url = response.url();
        // Check if it's an image request
        if (/\.(gif|png|jpg|jpeg|svg|webp|ico)$/i.test(url)) {
          failedRequests.push(url);
        }
      }
    });

    // Reload the page to capture all requests
    await page.goto('/', { waitUntil: 'networkidle' });

    // Wait for lazy loaded images
    await page.waitForTimeout(1000);

    // Scroll to trigger lazy loading
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
    await page.waitForTimeout(500);

    // No image requests should have failed
    expect(failedRequests).toEqual([]);
  });

  test('TC4: Total initial payload under 100KB excluding images', async ({ page }) => {
    let totalPayloadSize = 0;
    const resourceSizes = [];

    // Listen for all responses
    page.on('response', async (response) => {
      const url = response.url();
      const contentType = response.headers()['content-type'] || '';

      // Exclude images from calculation
      const isImage = /\.(gif|png|jpg|jpeg|svg|webp|ico)$/i.test(url) ||
                      contentType.startsWith('image/');

      if (!isImage && response.status() === 200) {
        try {
          const body = await response.body();
          const size = body.length;
          resourceSizes.push({ url, size, type: contentType });
          totalPayloadSize += size;
        } catch (e) {
          // Some responses may not have a body
        }
      }
    });

    await page.goto('/', { waitUntil: 'networkidle' });

    // 100KB = 102400 bytes
    expect(totalPayloadSize).toBeLessThan(102400);
  });

  test('Architecture diagram has lazy loading attribute', async ({ page }) => {
    const architectureDiagram = page.locator('.architecture-diagram');
    const count = await architectureDiagram.count();

    if (count > 0) {
      const loadingAttr = await architectureDiagram.getAttribute('loading');
      expect(loadingAttr).toBe('lazy');
    }
  });

  test('All images have valid src attributes', async ({ page }) => {
    const images = page.locator('img');
    const imageCount = await images.count();

    for (let i = 0; i < imageCount; i++) {
      const img = images.nth(i);
      const src = await img.getAttribute('src');
      const dataSrc = await img.getAttribute('data-src');

      // Image should have either src or data-src
      expect(src || dataSrc).toBeTruthy();
    }
  });

  test('All images have alt text for accessibility', async ({ page }) => {
    const images = page.locator('img');
    const imageCount = await images.count();

    for (let i = 0; i < imageCount; i++) {
      const img = images.nth(i);
      const alt = await img.getAttribute('alt');

      // All images should have alt text
      expect(alt).toBeTruthy();
    }
  });

  test('Lazy loaded images become visible when scrolled into view', async ({ page }) => {
    // Check for any lazy elements
    const lazyImages = page.locator('img[loading="lazy"]');
    const lazyCount = await lazyImages.count();

    if (lazyCount > 0) {
      // Scroll to the lazy element
      const firstLazy = lazyImages.first();
      await firstLazy.scrollIntoViewIfNeeded();

      // Wait for loading
      await page.waitForTimeout(500);

      // Verify the image is now visible
      await expect(firstLazy).toBeVisible();

      // Verify it loaded successfully
      const isLoaded = await firstLazy.evaluate((img) => {
        return img.complete && img.naturalWidth > 0;
      });
      expect(isLoaded).toBe(true);
    }
  });

  test('SVG assets load correctly', async ({ page }) => {
    // Check for SVG images
    const svgImages = page.locator('img[src$=".svg"]');
    const svgCount = await svgImages.count();

    for (let i = 0; i < svgCount; i++) {
      const svg = svgImages.nth(i);
      await svg.scrollIntoViewIfNeeded();
      await page.waitForTimeout(100);

      const isLoaded = await svg.evaluate((img) => {
        return img.complete && img.naturalWidth > 0;
      });
      expect(isLoaded).toBe(true);
    }
  });
});
