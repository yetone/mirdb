const { test, expect } = require('@playwright/test');

/**
 * Accessibility - Alt Text Tests
 * Scenario: Validate that all images have appropriate alt text
 *
 * This test suite verifies:
 * 1. Logo.gif has descriptive alt text
 * 2. Usage.gif has descriptive alt text explaining the demonstration
 * 3. All images on the page have alt attributes (accessibility audit)
 */

test.describe('Accessibility - Alt Text', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: Logo image has descriptive alt text', async ({ page }) => {
    // Locate the logo image in the hero section
    const logoImage = page.locator('img[src*="logo.gif"], img.hero-logo');
    await expect(logoImage.first()).toBeVisible();

    // Get the alt attribute
    const altText = await logoImage.first().getAttribute('alt');

    // Verify alt text exists
    expect(altText, 'Logo image should have alt attribute').toBeTruthy();

    // Verify alt text is descriptive (not empty or generic)
    expect(altText.length, 'Alt text should be descriptive').toBeGreaterThan(5);

    // Verify alt text contains relevant content about the logo
    const altLower = altText.toLowerCase();
    const isDescriptive = altLower.includes('mirdb') ||
                          altLower.includes('logo') ||
                          altLower.includes('brand') ||
                          altLower.includes('icon');
    expect(isDescriptive, 'Alt text should describe the logo (e.g., "MirDB Logo")').toBe(true);
  });

  test('Test Case 2: Usage demo image has descriptive alt text explaining the demonstration', async ({ page }) => {
    // Locate the usage demonstration GIF
    const usageImage = page.locator('img[src*="usage.gif"], img.demo-gif');
    await expect(usageImage.first()).toBeVisible();

    // Get the alt attribute
    const altText = await usageImage.first().getAttribute('alt');

    // Verify alt text exists
    expect(altText, 'Usage image should have alt attribute').toBeTruthy();

    // Verify alt text is descriptive (should be more detailed for demos)
    expect(altText.length, 'Alt text for demo should be detailed').toBeGreaterThan(20);

    // Verify alt text describes what the demonstration shows
    const altLower = altText.toLowerCase();
    const hasUsageContext = altLower.includes('usage') ||
                            altLower.includes('demo') ||
                            altLower.includes('demonstration');
    const hasActionContext = altLower.includes('command') ||
                             altLower.includes('action') ||
                             altLower.includes('show') ||
                             altLower.includes('memcached');

    expect(hasUsageContext || hasActionContext,
      'Alt text should explain what the demonstration shows').toBe(true);
  });

  test('Test Case 3: Accessibility audit - No images missing alt attributes', async ({ page }) => {
    // Get all images on the page
    const allImages = await page.locator('img').all();

    // Verify we have images to test
    expect(allImages.length, 'Page should contain images').toBeGreaterThan(0);

    // Track results for comprehensive reporting
    const imagesWithoutAlt = [];
    const imagesWithEmptyAlt = [];
    const imagesWithShortAlt = [];

    for (const img of allImages) {
      const src = await img.getAttribute('src') || 'unknown source';
      const alt = await img.getAttribute('alt');

      if (alt === null) {
        imagesWithoutAlt.push(src);
      } else if (alt.trim() === '') {
        imagesWithEmptyAlt.push(src);
      } else if (alt.length < 3) {
        imagesWithShortAlt.push(src);
      }
    }

    // Assert no images are missing alt attributes
    expect(imagesWithoutAlt,
      `Images missing alt attribute: ${imagesWithoutAlt.join(', ')}`
    ).toHaveLength(0);

    // Assert no images have empty alt text (unless decorative, but our images are informative)
    expect(imagesWithEmptyAlt,
      `Images with empty alt text: ${imagesWithEmptyAlt.join(', ')}`
    ).toHaveLength(0);

    // Assert no images have overly short alt text
    expect(imagesWithShortAlt,
      `Images with very short alt text: ${imagesWithShortAlt.join(', ')}`
    ).toHaveLength(0);
  });

  test('All images have meaningful alt text that describes content', async ({ page }) => {
    // Get all images on the page
    const allImages = await page.locator('img').all();

    for (const img of allImages) {
      const src = await img.getAttribute('src') || 'unknown source';
      const alt = await img.getAttribute('alt');

      // Every image must have alt attribute
      expect(alt, `Image ${src} must have alt attribute`).not.toBeNull();

      // Alt text should be non-empty for informative images
      expect(alt.trim().length,
        `Image ${src} should have non-empty alt text`
      ).toBeGreaterThan(0);

      // Alt text should be descriptive (minimum reasonable length)
      expect(alt.length,
        `Image ${src} alt text should be descriptive (at least 5 characters)`
      ).toBeGreaterThan(5);
    }
  });

  test('Logo and usage images have appropriate alt text per WCAG guidelines', async ({ page }) => {
    // Check logo image specifically
    const logoImage = page.locator('img[src*="logo.gif"]');
    if (await logoImage.count() > 0) {
      const logoAlt = await logoImage.first().getAttribute('alt');
      expect(logoAlt).toBeTruthy();
      expect(logoAlt.toLowerCase()).toContain('mirdb');
    }

    // Check usage demonstration image specifically
    const usageImage = page.locator('img[src*="usage.gif"]');
    if (await usageImage.count() > 0) {
      const usageAlt = await usageImage.first().getAttribute('alt');
      expect(usageAlt).toBeTruthy();
      // Usage GIF should have detailed description
      expect(usageAlt.length).toBeGreaterThan(15);
    }

    // Check CI badge image
    const badgeImage = page.locator('img[alt*="CircleCI"], img[alt*="Status"], img[alt*="badge"], img[src*="circleci"], img[src*="atompunk"]');
    if (await badgeImage.count() > 0) {
      const badgeAlt = await badgeImage.first().getAttribute('alt');
      expect(badgeAlt).toBeTruthy();
      expect(badgeAlt.length).toBeGreaterThan(3);
    }
  });
});
