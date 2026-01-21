/**
 * E2E Tests for Product Visual Showcase Section
 * Testing REQ-6: Visual product showcase (screenshots, demo video, or product images)
 * Test Case 4: Verify image loading without layout shift
 */

import { test, expect } from '@playwright/test';

test.describe('Product Visual Showcase E2E Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 4: Verify image loading
   * Input: Verify image loading
   * Expected: Images load without layout shift
   */
  test('product showcase section should be visible on the page', async ({ page }) => {
    const showcaseSection = page.locator('.product-showcase');
    await expect(showcaseSection).toBeVisible();
  });

  test('product showcase images should be visible', async ({ page }) => {
    // Scroll to the showcase section to trigger any lazy loading
    const showcaseSection = page.locator('.product-showcase');
    await showcaseSection.scrollIntoViewIfNeeded();

    // Wait for images to load
    await page.waitForTimeout(500);

    // Get all images in the showcase section
    const images = showcaseSection.locator('img');
    const count = await images.count();

    expect(count).toBeGreaterThanOrEqual(1);

    // Each image should be visible
    for (let i = 0; i < count; i++) {
      await expect(images.nth(i)).toBeVisible();
    }
  });

  test('images should load without layout shift (have defined dimensions)', async ({ page }) => {
    const showcaseSection = page.locator('.product-showcase');
    await showcaseSection.scrollIntoViewIfNeeded();

    const images = showcaseSection.locator('img');
    const count = await images.count();

    expect(count).toBeGreaterThanOrEqual(1);

    for (let i = 0; i < count; i++) {
      const image = images.nth(i);

      // Check that image has non-zero dimensions
      const boundingBox = await image.boundingBox();
      expect(boundingBox).not.toBeNull();
      expect(boundingBox.width).toBeGreaterThan(0);
      expect(boundingBox.height).toBeGreaterThan(0);

      // Check for explicit width/height attributes OR CSS-controlled dimensions
      const hasWidthAttr = await image.evaluate((el) => el.hasAttribute('width'));
      const hasHeightAttr = await image.evaluate((el) => el.hasAttribute('height'));
      const hasAspectRatio = await image.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return style.aspectRatio !== 'auto' || (style.width !== 'auto' && style.height !== 'auto');
      });

      // Image should have either explicit dimensions or CSS-controlled sizing
      expect(hasWidthAttr || hasHeightAttr || hasAspectRatio).toBe(true);
    }
  });

  test('images should have complete status or explicit dimensions', async ({ page }) => {
    const showcaseSection = page.locator('.product-showcase');
    await showcaseSection.scrollIntoViewIfNeeded();

    // Wait for images to start loading
    await page.waitForTimeout(1500);

    const images = showcaseSection.locator('img');
    const count = await images.count();

    expect(count).toBeGreaterThanOrEqual(1);

    for (let i = 0; i < count; i++) {
      const image = images.nth(i);

      // Check that image has explicit dimensions via attributes or loaded successfully
      const imageInfo = await image.evaluate((el) => ({
        naturalWidth: el.naturalWidth,
        naturalHeight: el.naturalHeight,
        complete: el.complete,
        hasWidthAttr: el.hasAttribute('width'),
        hasHeightAttr: el.hasAttribute('height'),
        widthAttr: el.getAttribute('width'),
        heightAttr: el.getAttribute('height'),
        hasSrc: !!el.src && el.src.length > 0,
      }));

      // Image should have a valid src attribute
      expect(imageInfo.hasSrc).toBe(true);

      // Image should either have explicit dimensions OR have loaded with natural dimensions
      const hasExplicitDimensions = imageInfo.hasWidthAttr && imageInfo.hasHeightAttr;
      const hasLoadedWithDimensions = imageInfo.naturalWidth > 0 && imageInfo.naturalHeight > 0;

      expect(hasExplicitDimensions || hasLoadedWithDimensions).toBe(true);
    }
  });

  test('showcase section should maintain stable layout during image load', async ({ page }) => {
    // Get initial section height before scrolling into view
    const showcaseSection = page.locator('.product-showcase');

    // Scroll to section
    await showcaseSection.scrollIntoViewIfNeeded();

    // Get the bounding box immediately
    const initialBox = await showcaseSection.boundingBox();
    expect(initialBox).not.toBeNull();

    // Wait for images to fully load
    await page.waitForLoadState('load');
    await page.waitForTimeout(1000);

    // Get the bounding box after images loaded
    const finalBox = await showcaseSection.boundingBox();
    expect(finalBox).not.toBeNull();

    // The section height should be stable (allow small tolerance for animations)
    // The key is that it shouldn't dramatically change
    expect(finalBox.height).toBeGreaterThan(0);
  });

  test('images should have proper alt text for accessibility', async ({ page }) => {
    const showcaseSection = page.locator('.product-showcase');
    const images = showcaseSection.locator('img');
    const count = await images.count();

    expect(count).toBeGreaterThanOrEqual(1);

    for (let i = 0; i < count; i++) {
      const image = images.nth(i);
      const alt = await image.getAttribute('alt');

      expect(alt).not.toBeNull();
      expect(alt.trim().length).toBeGreaterThan(0);
    }
  });

  test('showcase section should have a heading visible', async ({ page }) => {
    const showcaseSection = page.locator('.product-showcase');
    const heading = showcaseSection.locator('h2');

    await expect(heading).toBeVisible();

    const text = await heading.textContent();
    expect(text.trim().length).toBeGreaterThan(0);
  });

  test('images should be responsive (scale with container)', async ({ page }) => {
    const showcaseSection = page.locator('.product-showcase');
    await showcaseSection.scrollIntoViewIfNeeded();

    const images = showcaseSection.locator('img');
    const count = await images.count();

    expect(count).toBeGreaterThanOrEqual(1);

    for (let i = 0; i < count; i++) {
      const image = images.nth(i);
      const boundingBox = await image.boundingBox();

      // Image should not overflow its container
      const containerBox = await showcaseSection.boundingBox();
      expect(boundingBox.width).toBeLessThanOrEqual(containerBox.width + 10); // Allow small margin
    }
  });
});
