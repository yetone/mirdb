// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Error Handling - Missing Assets Scenario Tests
 *
 * Verify the page handles missing images or assets gracefully
 *
 * Test Cases:
 * 1. Block image loading and view page - Page layout remains intact, alt text is displayed for images (e2e)
 * 2. Check for broken image indicators - No broken image icons visible under normal operation (e2e)
 */

test.describe('Error Handling - Missing Assets', () => {

  /**
   * Test Case 1: Block image loading and verify graceful degradation
   * Input: Block image loading and view page
   * Expected: Page layout remains intact, alt text is displayed for images
   */
  test('TC1: Page layout remains intact when images fail to load and alt text is displayed', async ({ page }) => {
    // Block all image requests to simulate missing images
    await page.route('**/*.gif', route => route.abort());
    await page.route('**/*.png', route => route.abort());
    await page.route('**/*.jpg', route => route.abort());
    await page.route('**/*.jpeg', route => route.abort());
    await page.route('**/*.svg', route => route.abort());
    await page.route('**/*.webp', route => route.abort());

    // Navigate to the page with images blocked
    await page.goto('/', { waitUntil: 'domcontentloaded' });

    // Verify the page still loads and main content is visible
    const mainContent = page.locator('main');
    await expect(mainContent).toBeVisible();

    // Verify hero section is still intact
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Verify hero headline is visible
    const heroH1 = page.locator('.hero-section h1');
    await expect(heroH1).toBeVisible();
    await expect(heroH1).toContainText('Persistent Memcached');

    // Verify features section is still intact
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Verify all feature cards are visible
    const featureCards = page.locator('.feature-card');
    const featureCount = await featureCards.count();
    expect(featureCount).toBe(5);

    for (let i = 0; i < featureCount; i++) {
      await expect(featureCards.nth(i)).toBeVisible();
    }

    // Verify getting started section is still intact
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Verify documentation section is still intact
    const docsSection = page.locator('#documentation');
    await expect(docsSection).toBeVisible();

    // Verify navigation is still functional
    const navList = page.locator('.nav-list');
    await expect(navList).toBeVisible();

    const navLinks = page.locator('.nav-link');
    const navLinkCount = await navLinks.count();
    expect(navLinkCount).toBeGreaterThan(0);

    // Verify footer is still intact
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Verify all images have meaningful alt text displayed
    const images = await page.locator('img').all();
    console.log(`Found ${images.length} images on the page`);

    for (const img of images) {
      // Each image should have an alt attribute
      const altText = await img.getAttribute('alt');
      expect(altText).not.toBeNull();
      expect(altText.trim().length).toBeGreaterThan(0);

      console.log(`Image alt text: "${altText}"`);

      // Verify the alt text is meaningful (not generic)
      expect(altText.toLowerCase()).not.toBe('image');
      expect(altText.toLowerCase()).not.toBe('picture');
      expect(altText.toLowerCase()).not.toBe('photo');
    }

    // Verify the logo image has proper alt text
    const logoImg = page.locator('.logo-image');
    if (await logoImg.count() > 0) {
      const logoAlt = await logoImg.getAttribute('alt');
      expect(logoAlt).toBe('MirDB Logo');
    }

    // Verify page layout hasn't shifted - check that key elements are in expected positions
    const header = page.locator('header.header');
    await expect(header).toBeVisible();

    const headerBox = await header.boundingBox();
    expect(headerBox).not.toBeNull();
    expect(headerBox.y).toBeLessThan(100); // Header should be near top of page

    // Verify the logo section maintains structure even without image
    const logoSection = page.locator('.logo-section');
    await expect(logoSection).toBeVisible();

    // Verify the logo text is still visible as fallback
    const logoText = page.locator('.logo-text');
    await expect(logoText).toBeVisible();
    await expect(logoText).toContainText('MirDB');
  });

  /**
   * Test Case 2: Check for broken image indicators under normal operation
   * Input: Check for broken image indicators
   * Expected: No broken image icons visible under normal operation
   */
  test('TC2: No broken image icons visible under normal operation', async ({ page }) => {
    // Track failed image requests
    const failedImages = [];

    // Listen for failed image requests
    page.on('requestfailed', request => {
      const resourceType = request.resourceType();
      if (resourceType === 'image') {
        failedImages.push({
          url: request.url(),
          error: request.failure()?.errorText,
        });
      }
    });

    // Navigate to the page normally
    await page.goto('/', { waitUntil: 'load' });

    // Wait for all images to load
    await page.waitForLoadState('networkidle');

    // Log any failed images for debugging
    if (failedImages.length > 0) {
      console.log('Failed images:', failedImages);
    }

    // Verify no images failed to load under normal operation
    expect(failedImages.length).toBe(0);

    // Get all images on the page
    const images = await page.locator('img').all();
    console.log(`Found ${images.length} images`);

    // Verify each image loaded successfully
    for (const img of images) {
      const src = await img.getAttribute('src');
      const naturalWidth = await img.evaluate((el) => el.naturalWidth);
      const naturalHeight = await img.evaluate((el) => el.naturalHeight);

      console.log(`Image ${src}: ${naturalWidth}x${naturalHeight}`);

      // A broken image typically has naturalWidth and naturalHeight of 0
      // However, if the image is not displayed (display: none), it might also be 0
      // We check if the image is actually displayed
      const isVisible = await img.isVisible();

      if (isVisible) {
        // For visible images, they should have loaded properly
        // naturalWidth > 0 indicates the image loaded
        expect(naturalWidth).toBeGreaterThan(0);
        expect(naturalHeight).toBeGreaterThan(0);
      }

      // Verify image has alt text (required for accessibility even when image loads)
      const altText = await img.getAttribute('alt');
      expect(altText).not.toBeNull();
    }

    // Check that there are no CSS indicators of broken images
    // Browsers may add specific styling to broken images
    for (const img of images) {
      const imgStyles = await img.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          display: styles.display,
          visibility: styles.visibility,
          opacity: styles.opacity,
        };
      });

      // If image is meant to be displayed, it should be visible
      if (imgStyles.display !== 'none') {
        expect(imgStyles.visibility).not.toBe('hidden');
      }
    }
  });

  /**
   * Additional Test: Verify image error handling CSS styles
   * This test ensures proper styling exists for graceful degradation
   */
  test('TC-Additional: Images have CSS fallback styling for error states', async ({ page }) => {
    await page.goto('/');

    // Check that images have defined dimensions or constraints
    // This prevents layout shift when images fail to load
    const images = await page.locator('img').all();

    for (const img of images) {
      const imgStyles = await img.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          width: styles.width,
          height: styles.height,
          maxWidth: styles.maxWidth,
          maxHeight: styles.maxHeight,
        };
      });

      console.log('Image styles:', imgStyles);

      // Images should have some dimension constraints
      // Either explicit width/height or max-width/max-height
      const hasWidthConstraint = imgStyles.width !== 'auto' ||
                                  imgStyles.maxWidth !== 'none';
      const hasHeightConstraint = imgStyles.height !== 'auto' ||
                                   imgStyles.maxHeight !== 'none';

      // At least one dimension should be constrained
      expect(hasWidthConstraint || hasHeightConstraint).toBe(true);
    }
  });

  /**
   * Additional Test: Verify layout stability when images are slow to load
   * This test simulates slow network conditions
   */
  test('TC-Additional: Page layout is stable during slow image loading', async ({ page }) => {
    // Delay image loading by 2 seconds
    await page.route('**/*.gif', async route => {
      await new Promise(resolve => setTimeout(resolve, 2000));
      await route.continue();
    });

    // Navigate to the page
    await page.goto('/', { waitUntil: 'domcontentloaded' });

    // Verify main content is visible immediately
    const mainContent = page.locator('main');
    await expect(mainContent).toBeVisible();

    // Verify text content is readable while images load
    const heroH1 = page.locator('.hero-section h1');
    await expect(heroH1).toBeVisible();

    // Verify navigation is functional
    const navLinks = page.locator('.nav-link');
    await expect(navLinks.first()).toBeVisible();

    // Get the header height before images load
    const headerBeforeImages = await page.locator('header.header').boundingBox();

    // Wait for images to load
    await page.waitForLoadState('networkidle');

    // Get the header height after images load
    const headerAfterImages = await page.locator('header.header').boundingBox();

    // The header should not have significant layout shift
    // Allow small variations due to image dimensions being finalized
    if (headerBeforeImages && headerAfterImages) {
      // Height difference should be minimal (allowing for logo image height settling)
      const heightDiff = Math.abs(headerAfterImages.height - headerBeforeImages.height);
      // Allow up to 50px difference for the logo area adjustment
      expect(heightDiff).toBeLessThan(50);
    }
  });

  /**
   * Additional Test: Verify all image sources are valid paths
   */
  test('TC-Additional: All image sources reference valid paths', async ({ page }) => {
    await page.goto('/');

    const images = await page.locator('img').all();

    for (const img of images) {
      const src = await img.getAttribute('src');

      // Verify src attribute exists and is not empty
      expect(src).toBeTruthy();
      expect(src.trim().length).toBeGreaterThan(0);

      // Verify src is not a placeholder or dummy path
      expect(src.toLowerCase()).not.toContain('placeholder');
      expect(src.toLowerCase()).not.toContain('dummy');
      expect(src).not.toBe('#');
      expect(src).not.toBe('javascript:void(0)');

      console.log(`Valid image source: ${src}`);
    }
  });

  /**
   * Additional Test: Verify graceful handling with network error simulation
   */
  test('TC-Additional: Page handles network errors for assets gracefully', async ({ page }) => {
    // Simulate network error for specific image
    await page.route('**/logo.gif', route => route.abort('failed'));

    // Navigate to page
    await page.goto('/', { waitUntil: 'domcontentloaded' });

    // Page should still be functional
    const main = page.locator('main');
    await expect(main).toBeVisible();

    // All sections should be accessible
    await expect(page.locator('#hero')).toBeVisible();
    await expect(page.locator('#features')).toBeVisible();
    await expect(page.locator('#getting-started')).toBeVisible();
    await expect(page.locator('#documentation')).toBeVisible();

    // Navigation should work
    const featureLink = page.locator('a[href="#features"]');
    await featureLink.click();

    // Verify we navigated to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();

    // Logo text should still be visible as fallback identifier
    const logoText = page.locator('.logo-text');
    await expect(logoText).toBeVisible();
    await expect(logoText).toContainText('MirDB');
  });
});
