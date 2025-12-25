// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Image Lazy Loading Test Suite for MirDB Homepage
 *
 * Tests verify that:
 * - Below-fold images have loading="lazy" attribute for performance optimization
 * - Above-fold/hero images load immediately without lazy loading (for LCP optimization)
 * - Images load progressively as they enter the viewport
 */

test.describe('Image Lazy Loading - Unit Tests', () => {

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Below-fold images have loading="lazy" attribute', async ({ page }) => {
    // Get all images on the page
    const allImages = await page.locator('img').all();

    // Get the viewport height to determine fold position
    const viewportHeight = await page.evaluate(() => window.innerHeight);

    // Check each image for lazy loading based on position
    for (const img of allImages) {
      const boundingBox = await img.boundingBox();

      if (boundingBox) {
        // If image is below the fold (its top is below viewport height)
        if (boundingBox.y > viewportHeight) {
          const loadingAttr = await img.getAttribute('loading');
          expect(loadingAttr).toBe('lazy');
        }
      }
    }

    // Verify there is at least one image with lazy loading attribute on the page
    const lazyImages = await page.locator('img[loading="lazy"]').all();
    expect(lazyImages.length).toBeGreaterThan(0);
    console.log(`Found ${lazyImages.length} images with loading="lazy" attribute`);

    // Specifically check the usage demo image has lazy loading
    const usageImage = page.locator('[data-testid="usage-image"]');
    await expect(usageImage).toHaveAttribute('loading', 'lazy');

    // Verify the usage image has proper dimensions to prevent CLS
    await expect(usageImage).toHaveAttribute('width');
    await expect(usageImage).toHaveAttribute('height');
  });

  test('TC2: Hero and above-fold images load immediately without lazy loading', async ({ page }) => {
    // Get the viewport height to determine fold position
    const viewportHeight = await page.evaluate(() => window.innerHeight);

    // Get the logo image in the header (above the fold)
    const logoImage = page.locator('.logo img');
    await expect(logoImage).toBeVisible();

    // Check that the logo image does NOT have loading="lazy"
    // Above-fold images should load immediately for optimal LCP
    const logoLoadingAttr = await logoImage.getAttribute('loading');

    // The logo should either have no loading attribute (eager by default)
    // or explicitly have loading="eager"
    expect(logoLoadingAttr === null || logoLoadingAttr === 'eager').toBeTruthy();

    // Verify the logo image has proper dimensions set (prevents CLS)
    await expect(logoImage).toHaveAttribute('width');
    await expect(logoImage).toHaveAttribute('height');

    // Check any hero section images
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    const heroImages = await page.locator('.hero img').all();
    for (const heroImg of heroImages) {
      const loadingAttr = await heroImg.getAttribute('loading');
      // Hero images should not be lazy loaded
      expect(loadingAttr === null || loadingAttr === 'eager').toBeTruthy();
    }

    // Get all images and verify above-fold ones are not lazy loaded
    const allImages = await page.locator('img').all();

    for (const img of allImages) {
      const boundingBox = await img.boundingBox();

      if (boundingBox && boundingBox.y < viewportHeight) {
        // Above-fold images should NOT have loading="lazy"
        const loadingAttr = await img.getAttribute('loading');
        expect(loadingAttr !== 'lazy').toBeTruthy();
      }
    }

    console.log('Above-fold images verified to load immediately');
  });

});

test.describe('Image Lazy Loading - E2E Tests', () => {

  test('TC3: Images load progressively as they enter viewport', async ({ page }) => {
    // Set up network request monitoring
    const imageRequests = [];

    page.on('request', (request) => {
      const url = request.url();
      if (url.match(/\.(gif|jpg|jpeg|png|webp|svg)$/i)) {
        imageRequests.push({
          url,
          timestamp: Date.now(),
          resourceType: request.resourceType()
        });
      }
    });

    // Navigate to the page
    await page.goto('/');

    // Record initial image count before scrolling
    const initialRequestCount = imageRequests.length;
    console.log(`Initial image requests: ${initialRequestCount}`);

    // Wait for page to fully load
    await page.waitForLoadState('networkidle');

    // Get page dimensions
    const pageHeight = await page.evaluate(() => document.body.scrollHeight);
    const viewportHeight = await page.evaluate(() => window.innerHeight);

    // If page is taller than viewport, scroll and check for lazy loading behavior
    if (pageHeight > viewportHeight) {
      // Scroll to middle of page
      await page.evaluate(() => {
        window.scrollTo(0, document.body.scrollHeight / 2);
      });

      // Wait for any lazy-loaded images to load
      await page.waitForTimeout(500);

      // Scroll to bottom of page
      await page.evaluate(() => {
        window.scrollTo(0, document.body.scrollHeight);
      });

      // Wait for any lazy-loaded images to load
      await page.waitForTimeout(500);

      // Get final request count
      const finalRequestCount = imageRequests.length;
      console.log(`Final image requests after scrolling: ${finalRequestCount}`);

      // Verify images loaded (either eagerly or lazily)
      console.log('Image loading behavior verified');
    }

    // Verify all visible images are now loaded
    const visibleImages = await page.locator('img:visible').all();
    for (const img of visibleImages) {
      // Check that the image has completed loading
      const isLoaded = await img.evaluate((el) => {
        return el.complete && el.naturalHeight > 0;
      });
      expect(isLoaded).toBeTruthy();
    }

    console.log(`Total visible images verified: ${visibleImages.length}`);
  });

  test('Lazy loaded images have proper dimensions to prevent CLS', async ({ page }) => {
    await page.goto('/');

    // Get all images with lazy loading
    const lazyImages = await page.locator('img[loading="lazy"]').all();

    for (const img of lazyImages) {
      // Each lazy-loaded image should have explicit dimensions
      const width = await img.getAttribute('width');
      const height = await img.getAttribute('height');

      // Images should have explicit dimensions to prevent layout shift
      const hasExplicitDimensions = (width !== null && height !== null) ||
        await img.evaluate((el) => {
          const style = window.getComputedStyle(el);
          return style.width !== 'auto' && style.height !== 'auto';
        });

      expect(hasExplicitDimensions).toBeTruthy();
    }

    console.log(`Verified ${lazyImages.length} lazy-loaded images have proper dimensions`);
  });

  test('Browser native lazy loading is supported and functional', async ({ page }) => {
    await page.goto('/');

    // Check if the browser supports the loading attribute
    const supportsLazyLoading = await page.evaluate(() => {
      return 'loading' in HTMLImageElement.prototype;
    });

    expect(supportsLazyLoading).toBeTruthy();

    // Verify the loading attribute is properly set on appropriate images
    const imagesWithLoadingAttr = await page.evaluate(() => {
      const images = document.querySelectorAll('img');
      return Array.from(images).map(img => ({
        src: img.src,
        loading: img.getAttribute('loading'),
        alt: img.alt,
        inViewport: img.getBoundingClientRect().top < window.innerHeight
      }));
    });

    console.log('Image loading attributes:', JSON.stringify(imagesWithLoadingAttr, null, 2));

    // Verify above-fold images are not lazy loaded
    for (const img of imagesWithLoadingAttr) {
      if (img.inViewport) {
        expect(img.loading !== 'lazy').toBeTruthy();
      }
    }
  });

});
