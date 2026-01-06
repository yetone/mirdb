// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Usage Demonstration (REQ-8)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: Usage demo GIF or video element exists', async ({ page }) => {
    // Query for usage demonstration media on the page
    const usageDemoSection = page.locator('[data-testid="usage-demo-section"]');
    await expect(usageDemoSection).toBeVisible();

    // Check for media element (GIF or video)
    const usageMedia = page.locator('[data-testid="usage-demo-media"]');
    await expect(usageMedia).toBeVisible();
  });

  test('Test Case 2: Media source references assets/usage.gif or equivalent', async ({ page }) => {
    // Check media source - could be img for GIF or video element
    const usageImage = page.locator('[data-testid="usage-demo-media"]');
    await expect(usageImage).toBeVisible();

    // Verify the source points to assets/usage.gif
    const src = await usageImage.getAttribute('src');
    expect(src).toContain('assets/usage.gif');
  });

  test('Test Case 3: Usage demo media loads without broken media icons', async ({ page }) => {
    // Verify media loads correctly without broken image indicators
    const usageMedia = page.locator('[data-testid="usage-demo-media"]');
    await expect(usageMedia).toBeVisible();

    // For images, check natural dimensions to verify it loaded
    const naturalWidth = await usageMedia.evaluate((img) => {
      if (img instanceof HTMLImageElement) {
        return img.naturalWidth;
      }
      return null;
    });

    // Natural width > 0 indicates the image loaded successfully
    expect(naturalWidth).toBeGreaterThan(0);
  });

  test('Test Case 4: Media scales appropriately on mobile devices', async ({ page, browser }) => {
    // Test on mobile viewport
    const mobileContext = await browser.newContext({
      viewport: { width: 375, height: 667 }, // iPhone SE dimensions
    });
    const mobilePage = await mobileContext.newPage();
    await mobilePage.goto('/');

    // Check media is visible on mobile
    const usageMedia = mobilePage.locator('[data-testid="usage-demo-media"]');
    await expect(usageMedia).toBeVisible();

    // Verify media doesn't overflow container
    const mediaBox = await usageMedia.boundingBox();
    const viewportWidth = 375;

    expect(mediaBox).not.toBeNull();
    if (mediaBox) {
      // Media should fit within viewport width with some margin
      expect(mediaBox.width).toBeLessThanOrEqual(viewportWidth);
      // Media should have reasonable width (not too small)
      expect(mediaBox.width).toBeGreaterThan(200);
    }

    await mobileContext.close();
  });
});
