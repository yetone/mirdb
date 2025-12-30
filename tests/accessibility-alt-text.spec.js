const { test, expect } = require('@playwright/test');

/**
 * Accessibility - Alt Text Tests
 * Scenario: Validate that all images have appropriate alt text
 *
 * This test suite verifies that:
 * 1. Logo image has descriptive alt text
 * 2. Usage demo image has descriptive alt text
 * 3. All images on the page have alt attributes (accessibility audit)
 */

test.describe('Accessibility - Alt Text', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: Logo image has descriptive alt text (e.g., MirDB Logo)', async ({ page }) => {
    // Locate the logo image
    const logoImg = page.locator('img[src*="logo.gif"], img.hero-logo');
    await expect(logoImg.first()).toBeVisible();

    // Verify alt attribute exists
    const altText = await logoImg.first().getAttribute('alt');
    expect(altText).toBeTruthy();

    // Alt text should be descriptive (not empty or generic)
    expect(altText.length).toBeGreaterThan(5);

    // Alt text should reference MirDB or Logo
    const altLower = altText.toLowerCase();
    const isDescriptive = altLower.includes('mirdb') ||
                          altLower.includes('logo') ||
                          altLower.includes('brand');
    expect(isDescriptive).toBe(true);

    // Verify specific expected alt text pattern
    expect(altText).toMatch(/MirDB.*Logo|Logo.*MirDB/i);
  });

  test('Test Case 2: Usage demo image has descriptive alt text explaining the demonstration', async ({ page }) => {
    // Locate the usage demonstration image
    const usageImg = page.locator('img[src*="usage.gif"], img.demo-gif');
    await expect(usageImg.first()).toBeVisible();

    // Verify alt attribute exists
    const altText = await usageImg.first().getAttribute('alt');
    expect(altText).toBeTruthy();

    // Alt text should be descriptive (longer than a basic label)
    expect(altText.length).toBeGreaterThan(20);

    // Alt text should describe what the demo shows
    const altLower = altText.toLowerCase();
    const describesContent = altLower.includes('demonstration') ||
                             altLower.includes('demo') ||
                             altLower.includes('usage') ||
                             altLower.includes('command') ||
                             altLower.includes('action') ||
                             altLower.includes('showing');
    expect(describesContent).toBe(true);

    // Should mention MirDB or memcached context
    const mentionsContext = altLower.includes('mirdb') ||
                            altLower.includes('memcached') ||
                            altLower.includes('key-value');
    expect(mentionsContext).toBe(true);
  });

  test('Test Case 3: Accessibility audit - No images missing alt attributes', async ({ page }) => {
    // Get all image elements on the page
    const allImages = page.locator('img');
    const imageCount = await allImages.count();

    // Ensure there are images on the page to test
    expect(imageCount).toBeGreaterThan(0);

    // Check each image has a non-empty alt attribute
    const imagesWithoutAlt = [];
    const imagesWithEmptyAlt = [];

    for (let i = 0; i < imageCount; i++) {
      const img = allImages.nth(i);
      const src = await img.getAttribute('src') || 'unknown';
      const alt = await img.getAttribute('alt');

      if (alt === null) {
        imagesWithoutAlt.push(src);
      } else if (alt.trim() === '') {
        // Empty alt is acceptable for decorative images, but we should verify
        // For this page, all images should have meaningful alt text
        imagesWithEmptyAlt.push(src);
      }
    }

    // Report any missing alt attributes
    if (imagesWithoutAlt.length > 0) {
      console.log('Images missing alt attribute:', imagesWithoutAlt);
    }

    // All images should have alt attributes
    expect(imagesWithoutAlt).toHaveLength(0);

    // For content images (logo, demo), alt should not be empty
    // Only purely decorative images should have empty alt=""
    // On this page, verify the main content images have non-empty alt
    const logoImg = page.locator('img[src*="logo.gif"]');
    const usageImg = page.locator('img[src*="usage.gif"]');

    if (await logoImg.count() > 0) {
      const logoAlt = await logoImg.first().getAttribute('alt');
      expect(logoAlt).toBeTruthy();
      expect(logoAlt.trim()).not.toBe('');
    }

    if (await usageImg.count() > 0) {
      const usageAlt = await usageImg.first().getAttribute('alt');
      expect(usageAlt).toBeTruthy();
      expect(usageAlt.trim()).not.toBe('');
    }
  });

  test('Verify all images have accessible names for screen readers', async ({ page }) => {
    // Additional accessibility check: ensure images provide accessible names
    const allImages = page.locator('img');
    const imageCount = await allImages.count();

    for (let i = 0; i < imageCount; i++) {
      const img = allImages.nth(i);
      const src = await img.getAttribute('src') || 'unknown';

      // Check for alt attribute or aria-label
      const alt = await img.getAttribute('alt');
      const ariaLabel = await img.getAttribute('aria-label');
      const ariaLabelledBy = await img.getAttribute('aria-labelledby');

      // Image should have at least one accessible name source
      const hasAccessibleName = (alt !== null && alt !== '') ||
                                (ariaLabel !== null && ariaLabel !== '') ||
                                (ariaLabelledBy !== null && ariaLabelledBy !== '');

      // For purely decorative images, empty alt="" is acceptable
      // but null alt attribute is not accessible
      expect(alt !== null || ariaLabel !== null || ariaLabelledBy !== null).toBe(true);
    }
  });

  test('Verify logo.gif specifically has MirDB Logo alt text', async ({ page }) => {
    // Specifically test logo.gif has the expected alt text
    const logoImg = page.locator('img[src*="logo.gif"]');
    await expect(logoImg).toBeVisible();

    const altText = await logoImg.getAttribute('alt');
    expect(altText).toBe('MirDB Logo');
  });

  test('Verify usage.gif has detailed descriptive alt text', async ({ page }) => {
    // Specifically test usage.gif has detailed alt text
    const usageImg = page.locator('img[src*="usage.gif"]');
    await expect(usageImg).toBeVisible();

    const altText = await usageImg.getAttribute('alt');

    // Should be a full descriptive sentence
    expect(altText).toBeTruthy();
    expect(altText.split(' ').length).toBeGreaterThan(3); // More than 3 words

    // Should describe the demonstration
    expect(altText.toLowerCase()).toContain('demonstration');
  });
});
