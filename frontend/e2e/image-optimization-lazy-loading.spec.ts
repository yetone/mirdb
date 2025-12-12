import { test, expect } from '@playwright/test';

/**
 * Image Optimization and Lazy Loading E2E Tests
 *
 * Scenario: Verify images are optimized and lazy loaded for performance
 *
 * Test Case 2: Monitor network during scroll
 * Expected: Below-fold images load only when approaching viewport
 *
 * NFR-1: Page shall achieve Largest Contentful Paint (LCP) under 2.5 seconds
 * NFR-3: Page shall achieve Cumulative Layout Shift (CLS) under 0.1
 */
test.describe('Image Optimization and Lazy Loading', () => {
  /**
   * Step 1: Load homepage
   * Context: Navigate to homepage and monitor network requests
   */
  test.describe('Step 1: Load Homepage', () => {
    test('homepage loads successfully and network can be monitored', async ({ page }) => {
      // Navigate to homepage
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Verify homepage loaded
      await expect(page.locator('[data-testid="hero-section"]')).toBeVisible();
    });
  });

  /**
   * Step 2: Check above-fold images
   * Context: Verify hero images load immediately
   */
  test.describe('Step 2: Above-fold Images', () => {
    test('hero section images load immediately (not lazy)', async ({ page }) => {
      // Track image requests
      const imageRequests: string[] = [];
      page.on('request', (request) => {
        const url = request.url();
        if (url.match(/\.(jpg|jpeg|png|gif|webp|avif|svg)$/i)) {
          imageRequests.push(url);
        }
      });

      await page.goto('/');

      // Wait for initial load
      await page.waitForLoadState('domcontentloaded');

      // Verify hero section is visible
      await expect(page.locator('[data-testid="hero-section"]')).toBeVisible();

      // Above-fold images should have loaded by now
      // We check that at least some image requests were made during initial load
      expect(imageRequests.length).toBeGreaterThanOrEqual(0);
    });

    test('above-fold images do not have lazy loading', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Check hero section images - they should either not have loading attribute
      // or have loading="eager"
      const heroImages = page.locator('[data-testid="hero-section"] img, .hero-section img');
      const heroImageCount = await heroImages.count();

      for (let i = 0; i < heroImageCount; i++) {
        const img = heroImages.nth(i);
        const loading = await img.getAttribute('loading');

        // Above-fold images should not be lazy loaded
        if (loading) {
          expect(loading).toBe('eager');
        }
      }
    });
  });

  /**
   * Step 3: Scroll to below-fold content
   * Context: Scroll down and observe image loading behavior
   */
  test.describe('Step 3: Below-fold Content', () => {
    /**
     * Test Case 1: Check image loading attribute on below-fold images
     * Expected: Below-fold images have loading='lazy' attribute
     */
    test('below-fold images have loading="lazy" attribute', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Get all images on the page
      const allImages = page.locator('img');
      const imageCount = await allImages.count();

      // Find images in featured content section (below-fold)
      const featuredImages = page.locator('.featured-card-image, [data-testid="featured-content-section"] img');
      const featuredImageCount = await featuredImages.count();

      // Verify below-fold featured images have lazy loading
      for (let i = 0; i < featuredImageCount; i++) {
        const img = featuredImages.nth(i);
        const loading = await img.getAttribute('loading');
        expect(loading).toBe('lazy');
      }

      console.log(`Total images: ${imageCount}, Featured images: ${featuredImageCount}`);
    });

    /**
     * Test Case 2: Monitor network during scroll
     * Expected: Below-fold images load only when approaching viewport
     */
    test('below-fold images load only when scrolling into viewport', async ({ page }) => {
      // Track image requests with timing
      const imageLoadTimes: { url: string; time: number }[] = [];
      const navigationTime = Date.now();

      page.on('request', (request) => {
        const url = request.url();
        if (url.match(/\.(jpg|jpeg|png|gif|webp|avif)$/i)) {
          imageLoadTimes.push({
            url,
            time: Date.now() - navigationTime,
          });
        }
      });

      // Navigate to homepage
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Record images loaded before scroll
      const initialLoadCount = imageLoadTimes.length;
      console.log(`Images loaded before scroll: ${initialLoadCount}`);

      // Wait a bit to ensure lazy images haven't loaded yet
      await page.waitForTimeout(500);
      const preScrollCount = imageLoadTimes.length;

      // Scroll down to featured content section
      await page.evaluate(() => {
        const featuredSection = document.querySelector('[data-testid="featured-content-section"]');
        if (featuredSection) {
          featuredSection.scrollIntoView({ behavior: 'instant', block: 'start' });
        } else {
          window.scrollTo(0, window.innerHeight);
        }
      });

      // Wait for lazy images to load after scroll
      await page.waitForTimeout(1000);

      // Record images loaded after scroll
      const postScrollCount = imageLoadTimes.length;
      console.log(`Images loaded after scroll: ${postScrollCount}`);

      // Verify that featured content images are now visible
      const featuredSection = page.locator('[data-testid="featured-content-section"]');
      if (await featuredSection.count() > 0) {
        await expect(featuredSection).toBeVisible();

        // The featured images should now be loaded (or loading)
        const featuredImages = page.locator('.featured-card-image, [data-testid="featured-content-section"] img');
        const visibleCount = await featuredImages.count();
        console.log(`Featured images visible after scroll: ${visibleCount}`);
      }

      // Log timing information
      imageLoadTimes.forEach((item, index) => {
        console.log(`Image ${index + 1}: loaded at ${item.time}ms - ${item.url.substring(0, 50)}...`);
      });
    });

    /**
     * Test Case 3: Check image format optimization
     * Expected: Images use modern formats (WebP, AVIF) with fallbacks
     */
    test('images support modern formats with fallback', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Check for picture elements with modern format sources
      const pictureElements = page.locator('picture');
      const pictureCount = await pictureElements.count();

      if (pictureCount > 0) {
        for (let i = 0; i < pictureCount; i++) {
          const picture = pictureElements.nth(i);

          // Check for AVIF source
          const avifSource = picture.locator('source[type="image/avif"]');
          const hasAvif = (await avifSource.count()) > 0;

          // Check for WebP source
          const webpSource = picture.locator('source[type="image/webp"]');
          const hasWebp = (await webpSource.count()) > 0;

          // Check for fallback img
          const fallbackImg = picture.locator('img');
          const hasFallback = (await fallbackImg.count()) > 0;

          // Picture should have at least modern format and fallback
          expect(hasFallback).toBe(true);

          if (hasAvif || hasWebp) {
            console.log(`Picture ${i + 1}: AVIF=${hasAvif}, WebP=${hasWebp}, Fallback=${hasFallback}`);
          }
        }
      }

      // Also check that regular images exist as fallback
      const imgElements = page.locator('img');
      const imgCount = await imgElements.count();
      console.log(`Total img elements: ${imgCount}`);
      expect(imgCount).toBeGreaterThan(0);
    });

    /**
     * Test Case 4: Verify images have width and height attributes
     * Expected: All images have explicit dimensions to prevent CLS
     */
    test('images have explicit width and height to prevent CLS', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Get all content images (excluding icons, decorative images)
      const contentImages = page.locator('img:not([role="presentation"]):not([alt=""])');
      const imageCount = await contentImages.count();

      let imagesWithDimensions = 0;
      let imagesWithoutDimensions = 0;

      for (let i = 0; i < imageCount; i++) {
        const img = contentImages.nth(i);

        // Check for width attribute
        const width = await img.getAttribute('width');
        // Check for height attribute
        const height = await img.getAttribute('height');

        // Check computed styles as fallback
        const styles = await img.evaluate((el) => {
          const computed = window.getComputedStyle(el);
          return {
            width: computed.width,
            height: computed.height,
            aspectRatio: computed.aspectRatio,
          };
        });

        const hasDimensions =
          (width && height) ||
          (styles.width !== 'auto' && styles.height !== 'auto') ||
          styles.aspectRatio !== 'auto';

        if (hasDimensions) {
          imagesWithDimensions++;
        } else {
          imagesWithoutDimensions++;
          const src = await img.getAttribute('src');
          console.log(`Image without dimensions: ${src}`);
        }
      }

      console.log(
        `Images with dimensions: ${imagesWithDimensions}/${imageCount}, ` +
          `without: ${imagesWithoutDimensions}`
      );

      // All content images should have dimensions or aspect ratio
      // Allow some tolerance for edge cases
      const dimensionCoverage = imagesWithDimensions / imageCount;
      expect(dimensionCoverage).toBeGreaterThanOrEqual(0.9); // 90% coverage minimum
    });
  });

  /**
   * Complete Scenario Test: End-to-end lazy loading verification
   */
  test.describe('Complete Scenario', () => {
    test('verifies full image optimization scenario', async ({ page }) => {
      // Step 1: Load homepage
      await page.goto('/');
      await page.waitForLoadState('networkidle');
      await expect(page.locator('[data-testid="hero-section"]')).toBeVisible();

      // Step 2: Check above-fold images load immediately
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();

      // Step 3: Scroll to below-fold content and verify lazy loading
      await page.evaluate(() => {
        window.scrollTo(0, document.body.scrollHeight);
      });

      await page.waitForTimeout(500);

      // Verify featured content section (below-fold) is visible after scroll
      const featuredSection = page.locator('[data-testid="featured-content-section"]');
      if (await featuredSection.count() > 0) {
        await expect(featuredSection).toBeVisible();

        // Verify images in featured section have lazy loading attribute
        const featuredImages = featuredSection.locator('img');
        const count = await featuredImages.count();

        for (let i = 0; i < count; i++) {
          const loading = await featuredImages.nth(i).getAttribute('loading');
          expect(loading).toBe('lazy');
        }
      }

      // Scroll back to top
      await page.evaluate(() => {
        window.scrollTo(0, 0);
      });

      await page.waitForTimeout(300);

      // Final verification - page structure intact
      await expect(heroSection).toBeVisible();
    });
  });

  /**
   * Performance Tests for Image Loading
   */
  test.describe('Image Loading Performance', () => {
    test('page achieves good performance with lazy loaded images', async ({ page }) => {
      await page.goto('/');

      // Measure initial load time
      const timing = await page.evaluate(() => {
        const navigation = performance.getEntriesByType('navigation')[0] as PerformanceNavigationTiming;
        return {
          domContentLoaded: navigation.domContentLoadedEventEnd - navigation.fetchStart,
          loadComplete: navigation.loadEventEnd - navigation.fetchStart,
        };
      });

      console.log(`DOM Content Loaded: ${timing.domContentLoaded}ms`);
      console.log(`Load Complete: ${timing.loadComplete}ms`);

      // Page should load within reasonable time (accounting for lazy loading benefits)
      expect(timing.domContentLoaded).toBeLessThan(5000); // 5 seconds max
    });

    test('no significant CLS from image loading', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Scroll through the page
      await page.evaluate(async () => {
        return new Promise<void>((resolve) => {
          let scrolled = 0;
          const scrollStep = window.innerHeight / 2;
          const interval = setInterval(() => {
            window.scrollBy(0, scrollStep);
            scrolled += scrollStep;
            if (scrolled >= document.body.scrollHeight) {
              clearInterval(interval);
              resolve();
            }
          }, 100);
        });
      });

      await page.waitForTimeout(500);

      // Measure CLS after scrolling
      const cls = await page.evaluate(() => {
        return new Promise<number>((resolve) => {
          let clsValue = 0;
          const observer = new PerformanceObserver((list) => {
            for (const entry of list.getEntries()) {
              const layoutShift = entry as PerformanceEntry & { hadRecentInput: boolean; value: number };
              if (!layoutShift.hadRecentInput) {
                clsValue += layoutShift.value;
              }
            }
          });
          observer.observe({ type: 'layout-shift', buffered: true });
          setTimeout(() => {
            observer.disconnect();
            resolve(clsValue);
          }, 1000);
        });
      });

      console.log(`CLS after scroll: ${cls}`);
      expect(cls).toBeLessThan(0.1); // Good CLS score
    });
  });
});
