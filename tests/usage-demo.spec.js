// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Usage Demonstration', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: Usage demo GIF or video element exists', async ({ page }) => {
    // Query for usage demonstration media section
    const usageDemoSection = page.locator('[data-testid="usage-demo-section"]');
    await expect(usageDemoSection).toBeVisible();

    // Check for usage demo media (img or video)
    const usageDemoMedia = page.locator('[data-testid="usage-demo-media"]');
    await expect(usageDemoMedia).toBeVisible();
  });

  test('Test Case 2: Media source references assets/usage.gif or equivalent', async ({ page }) => {
    // Check media source attribute
    const usageDemoMedia = page.locator('[data-testid="usage-demo-media"]');
    await expect(usageDemoMedia).toBeVisible();

    // Get the src attribute
    const srcAttribute = await usageDemoMedia.getAttribute('src');
    expect(srcAttribute).toBeTruthy();

    // Verify it references usage.gif or a video equivalent
    const isValidSource = srcAttribute.includes('usage.gif') ||
                          srcAttribute.includes('usage.webm') ||
                          srcAttribute.includes('usage.mp4');
    expect(isValidSource).toBe(true);
  });

  test('Test Case 3: Usage demo media loads without broken media icons', async ({ page }) => {
    // Verify the media is visible and properly rendered
    const usageDemoMedia = page.locator('[data-testid="usage-demo-media"]');
    await expect(usageDemoMedia).toBeVisible();

    // Check that the image has loaded properly by verifying it has dimensions
    const boundingBox = await usageDemoMedia.boundingBox();
    expect(boundingBox).not.toBeNull();
    expect(boundingBox.width).toBeGreaterThan(100);
    expect(boundingBox.height).toBeGreaterThan(50);

    // For img elements, verify naturalWidth and naturalHeight are set (image loaded)
    const tagName = await usageDemoMedia.evaluate(el => el.tagName.toLowerCase());
    if (tagName === 'img') {
      const naturalWidth = await usageDemoMedia.evaluate(el => el.naturalWidth);
      const naturalHeight = await usageDemoMedia.evaluate(el => el.naturalHeight);
      expect(naturalWidth).toBeGreaterThan(0);
      expect(naturalHeight).toBeGreaterThan(0);
    }
  });

  test('Test Case 4: Media scales appropriately on mobile devices', async ({ page }) => {
    const usageDemoMedia = page.locator('[data-testid="usage-demo-media"]');
    await expect(usageDemoMedia).toBeVisible();

    // Get initial bounding box at desktop size (default is 1280x720)
    const desktopBoundingBox = await usageDemoMedia.boundingBox();
    expect(desktopBoundingBox).not.toBeNull();
    expect(desktopBoundingBox.width).toBeGreaterThan(0);
    expect(desktopBoundingBox.height).toBeGreaterThan(0);

    // Test tablet viewport (768px)
    await page.setViewportSize({ width: 768, height: 1024 });
    await expect(usageDemoMedia).toBeVisible();
    const tabletBoundingBox = await usageDemoMedia.boundingBox();
    expect(tabletBoundingBox).not.toBeNull();
    expect(tabletBoundingBox.width).toBeGreaterThan(0);
    expect(tabletBoundingBox.height).toBeGreaterThan(0);

    // Test mobile viewport (375px)
    await page.setViewportSize({ width: 375, height: 667 });
    await expect(usageDemoMedia).toBeVisible();
    const mobileBoundingBox = await usageDemoMedia.boundingBox();
    expect(mobileBoundingBox).not.toBeNull();
    expect(mobileBoundingBox.width).toBeGreaterThan(0);
    expect(mobileBoundingBox.height).toBeGreaterThan(0);

    // Verify media is responsive - on smaller screens it should fit within viewport
    // Mobile width should fit within mobile viewport (accounting for padding)
    expect(mobileBoundingBox.width).toBeLessThanOrEqual(375);
    // Media should maintain proper dimensions across all sizes
    expect(mobileBoundingBox.width).toBeLessThanOrEqual(tabletBoundingBox.width);
  });
});
