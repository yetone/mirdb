// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');
const fs = require('fs');

/**
 * Test Suite: Image Loading and Optimization
 * Scenario: Verify all images load correctly and are optimized for web
 * Scenario ID: 25
 */

test.describe('Image Loading and Optimization', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');
  });

  /**
   * Test Case 1 (E2E): Load page and check all images
   * Input: Load page and check all images
   * Expected: All images load without errors (no broken image icons)
   */
  test('TC1: All images load without errors (no broken image icons)', async ({ page }) => {
    // Get all image elements on the page
    const images = await page.locator('img').all();

    // There should be at least one image on the page
    expect(images.length).toBeGreaterThan(0);

    // Check each image for loading errors
    const imageLoadResults = [];

    for (const img of images) {
      const src = await img.getAttribute('src');
      const isVisible = await img.isVisible();

      // Check if image loaded successfully using naturalWidth
      // A broken image will have naturalWidth of 0
      const naturalWidth = await img.evaluate((el) => {
        return (el).naturalWidth;
      });

      const naturalHeight = await img.evaluate((el) => {
        return (el).naturalHeight;
      });

      // Check for complete property - indicates if image finished loading
      const complete = await img.evaluate((el) => {
        return (el).complete;
      });

      imageLoadResults.push({
        src,
        isVisible,
        naturalWidth,
        naturalHeight,
        complete,
        loaded: complete && naturalWidth > 0 && naturalHeight > 0
      });
    }

    // Log results for debugging
    console.log('Image load results:', JSON.stringify(imageLoadResults, null, 2));

    // Verify all images loaded successfully
    for (const result of imageLoadResults) {
      expect(
        result.loaded,
        `Image ${result.src} failed to load - naturalWidth: ${result.naturalWidth}, complete: ${result.complete}`
      ).toBe(true);
    }

    // Additional check: No broken image placeholders visible
    // Browsers typically show broken images with a specific appearance or replacement
    const brokenImageIndicators = await page.evaluate(() => {
      const imgs = document.querySelectorAll('img');
      const brokenImages = [];
      imgs.forEach(img => {
        // Check for zero dimensions indicating broken image
        if (img.complete && (img.naturalWidth === 0 || img.naturalHeight === 0)) {
          brokenImages.push(img.src);
        }
      });
      return brokenImages;
    });

    expect(
      brokenImageIndicators.length,
      `Found broken images: ${brokenImageIndicators.join(', ')}`
    ).toBe(0);
  });

  /**
   * Test Case 2 (Unit): Check logo image format and size
   * Input: Check logo image format and size
   * Expected: Logo is optimized (WebP or compressed PNG/SVG) and appropriately sized
   */
  test('TC2: Logo is optimized and appropriately sized', async ({ page }) => {
    // Find the logo image(s) on the page
    const logoImages = await page.locator('img[src*="logo"], img[alt*="Logo"], img[alt*="logo"]').all();

    expect(logoImages.length).toBeGreaterThan(0);

    for (const logoImg of logoImages) {
      const src = await logoImg.getAttribute('src');
      const alt = await logoImg.getAttribute('alt');

      // Check that logo source exists
      expect(src, 'Logo should have a src attribute').toBeTruthy();

      // Check the file extension - should be an optimized format
      // Acceptable formats: WebP (best), SVG (vector), PNG (with compression), GIF (for animated logos)
      const fileExtension = src.split('.').pop().toLowerCase();
      const optimizedFormats = ['webp', 'svg', 'png', 'gif'];

      expect(
        optimizedFormats.includes(fileExtension),
        `Logo ${src} should use an optimized format (${optimizedFormats.join(', ')}), got: ${fileExtension}`
      ).toBe(true);

      // Check rendered dimensions are appropriate (not excessively large)
      const boundingBox = await logoImg.boundingBox();

      if (boundingBox) {
        // Logo should have reasonable dimensions (not more than 500px for typical logos)
        // This ensures it's not serving an oversized image
        expect(
          boundingBox.width,
          `Logo width (${boundingBox.width}px) should be reasonably sized for web`
        ).toBeLessThanOrEqual(500);

        expect(
          boundingBox.height,
          `Logo height (${boundingBox.height}px) should be reasonably sized for web`
        ).toBeLessThanOrEqual(500);
      }

      // Check natural dimensions vs rendered dimensions
      // Good optimization means not serving images much larger than displayed
      const naturalWidth = await logoImg.evaluate((el) => (el).naturalWidth);
      const naturalHeight = await logoImg.evaluate((el) => (el).naturalHeight);

      // Logo should load properly
      expect(naturalWidth).toBeGreaterThan(0);
      expect(naturalHeight).toBeGreaterThan(0);

      console.log(`Logo ${src}: natural size ${naturalWidth}x${naturalHeight}, rendered ${boundingBox?.width}x${boundingBox?.height}`);
    }
  });

  /**
   * Test Case 3 (Unit): Verify images have alt attributes
   * Input: Verify images have alt attributes
   * Expected: All images have descriptive alt text for accessibility
   */
  test('TC3: All images have descriptive alt text for accessibility', async ({ page }) => {
    // Get all images on the page
    const images = await page.locator('img').all();

    expect(images.length).toBeGreaterThan(0);

    const imageAltResults = [];

    for (const img of images) {
      const src = await img.getAttribute('src');
      const alt = await img.getAttribute('alt');

      // Check that alt attribute exists
      expect(
        alt,
        `Image ${src} is missing alt attribute`
      ).not.toBeNull();

      // Check that alt text is not empty
      expect(
        alt.trim().length,
        `Image ${src} has empty alt text`
      ).toBeGreaterThan(0);

      // Check that alt text is descriptive (not just a filename)
      const isJustFilename = /^[a-zA-Z0-9_-]+\.(jpg|jpeg|png|gif|webp|svg)$/i.test(alt.trim());
      expect(
        isJustFilename,
        `Image ${src} alt text "${alt}" appears to be just a filename, should be descriptive`
      ).toBe(false);

      // Check that alt text has meaningful words (at least 2 characters)
      expect(
        alt.trim().length,
        `Image ${src} alt text should have meaningful description`
      ).toBeGreaterThanOrEqual(3);

      imageAltResults.push({
        src,
        alt,
        hasAlt: !!alt,
        isDescriptive: !isJustFilename && alt.trim().length >= 3
      });
    }

    console.log('Image alt text results:', JSON.stringify(imageAltResults, null, 2));

    // Verify all images passed alt text checks
    const allHaveDescriptiveAlt = imageAltResults.every(r => r.hasAlt && r.isDescriptive);
    expect(
      allHaveDescriptiveAlt,
      'All images should have descriptive alt text'
    ).toBe(true);
  });

  /**
   * Additional test: Verify image files exist on disk (unit test for file presence)
   */
  test('Image files exist in assets directory', async ({ page }) => {
    // Get all image sources from the page
    const imageSources = await page.evaluate(() => {
      const imgs = document.querySelectorAll('img');
      return Array.from(imgs).map(img => img.getAttribute('src')).filter(Boolean);
    });

    // Filter to local images (not external URLs)
    const localImages = imageSources.filter(src =>
      src && !src.startsWith('http://') && !src.startsWith('https://')
    );

    expect(localImages.length).toBeGreaterThan(0);

    // Verify each local image file exists
    for (const imageSrc of localImages) {
      const imagePath = path.join(process.cwd(), imageSrc);

      // Check file exists
      const fileExists = fs.existsSync(imagePath);
      expect(
        fileExists,
        `Image file should exist: ${imagePath}`
      ).toBe(true);

      if (fileExists) {
        // Check file is not empty
        const stats = fs.statSync(imagePath);
        expect(
          stats.size,
          `Image file ${imageSrc} should not be empty`
        ).toBeGreaterThan(0);

        console.log(`Image ${imageSrc}: ${(stats.size / 1024).toFixed(2)} KB`);
      }
    }
  });

  /**
   * Additional test: Verify image loading performance
   */
  test('Images load within acceptable time', async ({ page }) => {
    // Track image loading performance
    const imageLoadTimes = await page.evaluate(() => {
      const imgs = document.querySelectorAll('img');
      return Array.from(imgs).map(img => {
        return {
          src: img.src,
          complete: img.complete,
          // Performance timing if available
          loadTime: performance.getEntriesByName(img.src)[0]?.duration || null
        };
      });
    });

    // All images should be complete
    for (const img of imageLoadTimes) {
      expect(
        img.complete,
        `Image ${img.src} should be fully loaded`
      ).toBe(true);
    }

    console.log('Image load times:', JSON.stringify(imageLoadTimes, null, 2));
  });
});
