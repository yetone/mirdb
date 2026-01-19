// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E tests for Asset Integration
 * Scenario: Verify that existing assets (logo.gif, usage.gif) are properly integrated
 */

test.describe('Asset Integration', () => {

  test.beforeEach(async ({ page }) => {
    // Navigate to the homepage
    await page.goto('/');
  });

  /**
   * Test Case 1: Logo from assets/logo.gif is displayed and loads correctly
   * Input: Check logo.gif display
   * Expected: Logo from assets/logo.gif is displayed and loads correctly
   */
  test('TC1: Logo.gif is displayed and loads correctly in hero section', async ({ page }) => {
    // Find the logo image in the hero section
    const logo = page.locator('[data-testid="hero-logo"]');
    await expect(logo).toBeVisible();

    // Verify the logo source points to assets/logo.gif
    const logoSrc = await logo.getAttribute('src');
    expect(logoSrc).toContain('logo.gif');
    expect(logoSrc).toContain('assets');

    // Verify the logo has proper alt text
    const altText = await logo.getAttribute('alt');
    expect(altText).toBeTruthy();
    expect(altText.toLowerCase()).toContain('logo');

    // Wait for the image to load (large GIF may take time)
    await page.waitForFunction(
      (selector) => {
        const img = document.querySelector(selector);
        return img && img.complete && img.naturalWidth > 0;
      },
      '[data-testid="hero-logo"]',
      { timeout: 30000 }
    );

    // Verify the logo has reasonable dimensions (not broken/tiny)
    const boundingBox = await logo.boundingBox();
    expect(boundingBox).toBeTruthy();
    expect(boundingBox.width).toBeGreaterThan(50);
    expect(boundingBox.height).toBeGreaterThan(50);
  });

  /**
   * Test Case 2: Usage demonstration gif from assets/usage.gif loads and animates correctly
   * Input: Check usage.gif display (if used)
   * Expected: Usage demonstration gif from assets/usage.gif loads and animates correctly
   */
  test('TC2: Usage.gif is displayed and loads correctly in getting-started section', async ({ page }) => {
    // Scroll to the getting-started section where usage.gif should be
    await page.locator('#getting-started').scrollIntoViewIfNeeded();

    // Find the usage gif - it may have a class or be in a specific section
    const usageGif = page.locator('img[src*="usage.gif"]');
    await expect(usageGif).toBeVisible();

    // Verify the source points to assets/usage.gif
    const usageSrc = await usageGif.getAttribute('src');
    expect(usageSrc).toContain('usage.gif');
    expect(usageSrc).toContain('assets');

    // Verify the usage gif has proper alt text for accessibility
    const altText = await usageGif.getAttribute('alt');
    expect(altText).toBeTruthy();
    // Alt text should describe the demo/usage
    expect(altText.toLowerCase()).toMatch(/usage|demo|example|mirdb/i);

    // Wait for the image to load (large GIF may take time)
    await page.waitForFunction(
      (selector) => {
        const img = document.querySelector(selector);
        return img && img.complete && img.naturalWidth > 0;
      },
      'img[src*="usage.gif"]',
      { timeout: 30000 }
    );

    // Verify the gif has reasonable dimensions for a demo
    const boundingBox = await usageGif.boundingBox();
    expect(boundingBox).toBeTruthy();
    expect(boundingBox.width).toBeGreaterThan(100);
    expect(boundingBox.height).toBeGreaterThan(50);
  });

  /**
   * Additional test: Verify both assets are served from correct paths
   */
  test('Assets are accessible via correct relative paths', async ({ request }) => {
    // The assets are served from /assets/ relative to the docs folder
    const baseURL = 'http://localhost:3000';
    const logoPath = `${baseURL}/assets/logo.gif`;
    const usagePath = `${baseURL}/assets/usage.gif`;

    // Verify logo.gif is accessible
    const logoResponse = await request.get(logoPath);
    expect(logoResponse.ok()).toBe(true);
    expect(logoResponse.headers()['content-type']).toContain('image');

    // Verify usage.gif is accessible
    const usageResponse = await request.get(usagePath);
    expect(usageResponse.ok()).toBe(true);
    expect(usageResponse.headers()['content-type']).toContain('image');
  });

  /**
   * Test for logo visibility in first viewport (above the fold)
   */
  test('Logo is visible in first viewport without scrolling', async ({ page }) => {
    const logo = page.locator('[data-testid="hero-logo"]');
    await expect(logo).toBeVisible();

    // Verify the logo is in the viewport without scrolling
    const boundingBox = await logo.boundingBox();
    expect(boundingBox).toBeTruthy();
    expect(boundingBox.y).toBeGreaterThanOrEqual(0);
    expect(boundingBox.y).toBeLessThan(600); // Should be visible without scrolling
  });

});
