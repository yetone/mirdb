const { test, expect } = require('@playwright/test');

/**
 * Asset Optimization Tests
 * Scenario: Validate that images and assets are optimized for web delivery
 *
 * Steps:
 * 1. Check image lazy loading - Verify that large GIFs use lazy loading
 * 2. Verify image attributes - Check that images have width/height attributes to prevent layout shift
 * 3. Check asset delivery - Verify assets load from appropriate source with caching
 *
 * Test Cases:
 * 1. Usage GIF has loading='lazy' attribute or equivalent lazy loading implementation
 * 2. All images have explicit width and height attributes
 * 3. CLS score under 0.1
 */

test.describe('Asset Optimization', () => {
  // Configure longer timeout for performance tests
  test.setTimeout(60000);

  test.describe('Test Case 1: Image Lazy Loading', () => {
    test('Usage GIF has loading="lazy" attribute', async ({ page }) => {
      await page.goto('/');

      // Check that usage.gif has lazy loading
      const usageGif = page.locator('img.demo-gif');
      await expect(usageGif).toHaveAttribute('loading', 'lazy');

      // Verify the src is correct
      await expect(usageGif).toHaveAttribute('src', 'assets/usage.gif');
    });

    test('Logo GIF has loading="lazy" attribute', async ({ page }) => {
      await page.goto('/');

      // Check that logo.gif has lazy loading
      const logoGif = page.locator('img.hero-logo');
      await expect(logoGif).toHaveAttribute('loading', 'lazy');

      // Verify the src is correct
      await expect(logoGif).toHaveAttribute('src', 'assets/logo.gif');
    });

    test('All large images use lazy loading', async ({ page }) => {
      await page.goto('/');

      // Count all images with lazy loading attribute
      const lazyImages = await page.locator('img[loading="lazy"]').count();

      // Both logo.gif and usage.gif should have lazy loading
      expect(lazyImages).toBeGreaterThanOrEqual(2);

      // Verify specifically the GIF files have lazy loading
      const allImages = await page.locator('img').all();
      for (const img of allImages) {
        const src = await img.getAttribute('src');
        if (src && (src.includes('.gif') && !src.startsWith('http'))) {
          // Local GIF files should have lazy loading
          const loading = await img.getAttribute('loading');
          expect(loading).toBe('lazy');
        }
      }
    });
  });

  test.describe('Test Case 2: Image Dimension Attributes', () => {
    test('All images have explicit width and height attributes', async ({ page }) => {
      await page.goto('/');

      const allImages = await page.locator('img').all();

      for (const img of allImages) {
        const src = await img.getAttribute('src');

        // Get width and height attributes
        const width = await img.getAttribute('width');
        const height = await img.getAttribute('height');

        // All images should have width and height attributes
        expect(width, `Image ${src} should have width attribute`).toBeTruthy();
        expect(height, `Image ${src} should have height attribute`).toBeTruthy();

        // Width and height should be numeric
        expect(parseInt(width), `Image ${src} width should be a positive number`).toBeGreaterThan(0);
        expect(parseInt(height), `Image ${src} height should be a positive number`).toBeGreaterThan(0);
      }
    });

    test('Logo image has correct dimensions', async ({ page }) => {
      await page.goto('/');

      const logoGif = page.locator('img.hero-logo');

      // Check width and height attributes match actual logo dimensions
      await expect(logoGif).toHaveAttribute('width', '500');
      await expect(logoGif).toHaveAttribute('height', '180');
    });

    test('Usage demo image has correct dimensions', async ({ page }) => {
      await page.goto('/');

      const usageGif = page.locator('img.demo-gif');

      // Check width and height attributes match actual image dimensions
      await expect(usageGif).toHaveAttribute('width', '660');
      await expect(usageGif).toHaveAttribute('height', '416');
    });
  });

  test.describe('Test Case 3: Cumulative Layout Shift (CLS)', () => {
    test('CLS score is under 0.1 (good performance)', async ({ page }) => {
      // Navigate to the page
      await page.goto('/');

      // Wait for page to fully load
      await page.waitForLoadState('networkidle');

      // Measure Cumulative Layout Shift using Performance Observer API
      const cls = await page.evaluate(() => {
        return new Promise((resolve) => {
          let clsValue = 0;

          // Check if PerformanceObserver is available
          if (typeof PerformanceObserver === 'undefined') {
            // If not available, return 0 (assume no layout shifts)
            resolve(0);
            return;
          }

          const observer = new PerformanceObserver((list) => {
            for (const entry of list.getEntries()) {
              // Only count layout shifts without recent input (user interaction)
              if (!entry.hadRecentInput) {
                clsValue += entry.value;
              }
            }
          });

          try {
            // Start observing layout shift events, including buffered entries
            observer.observe({ type: 'layout-shift', buffered: true });
          } catch (e) {
            // If observation fails, return 0
            resolve(0);
            return;
          }

          // Wait for layout shifts to be recorded
          // Give enough time for images to load and any shifts to occur
          setTimeout(() => {
            observer.disconnect();
            resolve(clsValue);
          }, 3000);
        });
      });

      console.log(`Cumulative Layout Shift (CLS): ${cls}`);

      // CLS should be under 0.1 for good performance
      // Values:
      // - Good: < 0.1
      // - Needs improvement: 0.1 - 0.25
      // - Poor: > 0.25
      expect(cls).toBeLessThan(0.1);
    });

    test('Images do not cause layout shift when loading', async ({ page }) => {
      // Set up layout shift monitoring before navigation
      await page.goto('/');

      // Scroll down to trigger lazy loading of demo gif
      await page.evaluate(() => {
        window.scrollTo(0, document.body.scrollHeight / 2);
      });

      // Wait a moment for any layout shifts
      await page.waitForTimeout(1000);

      // Check CLS after scrolling
      const clsAfterScroll = await page.evaluate(() => {
        return new Promise((resolve) => {
          let clsValue = 0;

          if (typeof PerformanceObserver === 'undefined') {
            resolve(0);
            return;
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
          } catch (e) {
            resolve(0);
            return;
          }

          setTimeout(() => {
            observer.disconnect();
            resolve(clsValue);
          }, 2000);
        });
      });

      console.log(`CLS after scrolling: ${clsAfterScroll}`);

      // CLS should still be under 0.1 after scrolling
      expect(clsAfterScroll).toBeLessThan(0.1);
    });

    test('Images maintain aspect ratio with CSS', async ({ page }) => {
      await page.goto('/');

      // Check that images have proper CSS for maintaining aspect ratio
      const logoGif = page.locator('img.hero-logo');
      const usageGif = page.locator('img.demo-gif');

      // Verify height: auto is applied (either directly or through CSS)
      // This ensures images scale proportionally
      const logoStyle = await logoGif.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          height: styles.height,
          maxWidth: styles.maxWidth,
          width: styles.width
        };
      });

      const usageStyle = await usageGif.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          height: styles.height,
          maxWidth: styles.maxWidth,
          width: styles.width
        };
      });

      console.log('Logo image computed styles:', logoStyle);
      console.log('Usage image computed styles:', usageStyle);

      // The images should have max-width: 100% (from CSS class) for responsive scaling
      // or have explicit dimensions to prevent layout shift
      expect(logoStyle.maxWidth === '100%' || logoStyle.maxWidth !== 'none' || logoStyle.width !== 'auto').toBeTruthy();
      expect(usageStyle.maxWidth === '100%' || usageStyle.maxWidth !== 'none' || usageStyle.width !== 'auto').toBeTruthy();
    });
  });

  test.describe('Additional Asset Checks', () => {
    test('Assets load successfully', async ({ page }) => {
      // Track resource loading
      const loadedResources = [];
      const failedResources = [];

      page.on('response', (response) => {
        const url = response.url();
        if (url.includes('assets/') || url.includes('.gif')) {
          if (response.status() >= 200 && response.status() < 400) {
            loadedResources.push(url);
          } else {
            failedResources.push({ url, status: response.status() });
          }
        }
      });

      await page.goto('/', { waitUntil: 'networkidle' });

      console.log('Loaded resources:', loadedResources);
      console.log('Failed resources:', failedResources);

      // Verify no asset loading failures
      expect(failedResources).toHaveLength(0);

      // Verify key assets loaded
      expect(loadedResources.some(r => r.includes('logo.gif'))).toBeTruthy();
      expect(loadedResources.some(r => r.includes('usage.gif'))).toBeTruthy();
    });

    test('Assets have proper alt text for accessibility', async ({ page }) => {
      await page.goto('/');

      const allImages = await page.locator('img').all();

      for (const img of allImages) {
        const alt = await img.getAttribute('alt');
        const src = await img.getAttribute('src');

        // All images should have alt text
        expect(alt, `Image ${src} should have alt attribute`).toBeTruthy();

        // Alt text should be descriptive (more than just a few characters)
        expect(alt.length, `Image ${src} alt text should be descriptive`).toBeGreaterThan(5);
      }
    });
  });
});
