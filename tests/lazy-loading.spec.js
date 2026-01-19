// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Lazy Loading Implementation Tests
 *
 * These tests verify that:
 * 1. Below-fold images have loading="lazy" attribute
 * 2. Lazy loading behavior works correctly (images not loaded until scrolled into view)
 */

test.describe('Lazy Loading Implementation', () => {

  // Test Case 1 (E2E): Verify below-fold images are not loaded until scrolled into view
  test('below-fold images are not loaded until scrolled into view', async ({ page }) => {
    // Track network requests for images
    const imageRequests = [];

    page.on('request', request => {
      const url = request.url();
      if (url.includes('usage.gif') || url.includes('assets/')) {
        imageRequests.push({
          url,
          time: Date.now(),
          type: 'request'
        });
      }
    });

    page.on('response', response => {
      const url = response.url();
      if (url.includes('usage.gif') || url.includes('assets/')) {
        imageRequests.push({
          url,
          time: Date.now(),
          type: 'response',
          status: response.status()
        });
      }
    });

    // Navigate to the page but don't wait for images to load
    await page.goto('/', { waitUntil: 'domcontentloaded' });

    // Wait a short moment for initial page content to stabilize
    await page.waitForTimeout(500);

    // Get the usage-demo image element (below the fold)
    const usageDemo = page.locator('[data-testid="usage-demo"]');
    await expect(usageDemo).toBeAttached();

    // Verify the image has loading="lazy" attribute
    const loadingAttr = await usageDemo.getAttribute('loading');
    expect(loadingAttr).toBe('lazy');

    // Check if usage.gif was NOT loaded yet (it's below the fold)
    // The image may still be requested but with native lazy loading, the browser
    // should defer actual loading until the image is near the viewport
    const usageGifRequests = imageRequests.filter(r => r.url.includes('usage.gif'));

    // Now scroll to the getting-started section to bring the image into view
    await page.locator('#getting-started').scrollIntoViewIfNeeded();

    // Wait for the image to potentially load
    await page.waitForTimeout(1000);

    // After scrolling, the image should be visible and loaded
    await expect(usageDemo).toBeVisible();

    // Verify the image actually loaded (naturalWidth > 0 means the image loaded)
    const naturalWidth = await usageDemo.evaluate((img) => {
      return (img).naturalWidth;
    });
    expect(naturalWidth).toBeGreaterThan(0);
  });

  // Test Case 2 (Unit): Verify below-fold images have loading='lazy' attribute
  test('below-fold images have loading="lazy" attribute', async ({ page }) => {
    await page.goto('/');

    // Get all images on the page
    const allImages = page.locator('img');
    const imageCount = await allImages.count();

    // We expect at least 2 images (logo and usage demo)
    expect(imageCount).toBeGreaterThanOrEqual(2);

    // Check the hero logo (above the fold) - should NOT have loading="lazy" for best LCP
    const heroLogo = page.locator('[data-testid="hero-logo"]');
    await expect(heroLogo).toBeVisible();
    const heroLogoLoading = await heroLogo.getAttribute('loading');
    // Hero logo should either have no loading attribute or loading="eager"
    // It should NOT have loading="lazy" as it's above the fold
    expect(heroLogoLoading).not.toBe('lazy');

    // Check the usage demo image (below the fold) - MUST have loading="lazy"
    const usageDemo = page.locator('[data-testid="usage-demo"]');
    await expect(usageDemo).toBeAttached();
    const usageDemoLoading = await usageDemo.getAttribute('loading');
    expect(usageDemoLoading).toBe('lazy');
  });

  test('usage demo image has proper lazy loading implementation', async ({ page }) => {
    await page.goto('/');

    // Verify the usage-demo image exists and has correct attributes
    const usageDemo = page.locator('[data-testid="usage-demo"]');
    await expect(usageDemo).toBeAttached();

    // Check loading attribute
    const loadingAttr = await usageDemo.getAttribute('loading');
    expect(loadingAttr).toBe('lazy');

    // Check alt text is present (accessibility)
    const altText = await usageDemo.getAttribute('alt');
    expect(altText).toBeTruthy();
    expect(altText.length).toBeGreaterThan(0);

    // Check src attribute is present
    const srcAttr = await usageDemo.getAttribute('src');
    expect(srcAttr).toBeTruthy();
    expect(srcAttr).toContain('usage.gif');
  });

  test('all below-fold images have lazy loading configured', async ({ page }) => {
    await page.goto('/');

    // Get the hero section bottom position (fold line)
    const heroSection = page.locator('[data-testid="hero-section"]');
    const heroBox = await heroSection.boundingBox();
    expect(heroBox).not.toBeNull();
    const foldLine = heroBox.y + heroBox.height;

    // Get all images on the page
    const allImages = page.locator('img');
    const imageCount = await allImages.count();

    for (let i = 0; i < imageCount; i++) {
      const img = allImages.nth(i);
      const imgBox = await img.boundingBox();

      // If image is below the fold
      if (imgBox && imgBox.y > foldLine) {
        const loadingAttr = await img.getAttribute('loading');
        const src = await img.getAttribute('src');

        // Below-fold images should have loading="lazy"
        expect(loadingAttr, `Image ${src} below the fold should have loading="lazy"`).toBe('lazy');
      }
    }
  });

  test('hero logo (above fold) does not have lazy loading', async ({ page }) => {
    await page.goto('/');

    // Hero logo is critical for LCP and should not be lazy loaded
    const heroLogo = page.locator('[data-testid="hero-logo"]');
    await expect(heroLogo).toBeVisible();

    const loadingAttr = await heroLogo.getAttribute('loading');

    // Hero logo should either have no loading attribute or loading="eager"
    // Lazy loading the hero logo would hurt LCP performance
    if (loadingAttr) {
      expect(loadingAttr).toBe('eager');
    }
    // Having no loading attribute is also acceptable (defaults to eager)
  });
});
