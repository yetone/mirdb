/**
 * Usage Demonstration E2E Tests
 * Owner: Scenario 4 - Usage Demonstration
 *
 * Tests:
 * - Usage GIF visibility and src attribute
 * - Demo container styling and centering
 */
import { test, expect } from '@playwright/test';

test.describe('Usage Demonstration Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Usage demonstration image exists with src pointing to usage.gif', async ({ page }) => {
    // Navigate to homepage and locate the demo section
    const demoSection = page.locator('#demo');
    await expect(demoSection).toBeVisible();

    // Find the usage image
    const usageImg = demoSection.locator('img');
    await expect(usageImg).toBeVisible();

    // Verify src points to usage.gif
    const src = await usageImg.getAttribute('src');
    expect(src).toContain('usage.gif');
  });

  test('TC3: Demo container is properly styled and centered', async ({ page }) => {
    // Check the demo section exists
    const demoSection = page.locator('#demo');
    await expect(demoSection).toBeVisible();

    // Check the container has centered text
    const container = demoSection.locator('.text-center');
    await expect(container).toBeVisible();

    // Check the image container has shadow and rounded corners
    const imgContainer = demoSection.locator('.rounded-lg.shadow-xl');
    await expect(imgContainer).toBeVisible();

    // Verify the image is inside the styled container
    const img = imgContainer.locator('img');
    await expect(img).toBeVisible();

    // Verify image has responsive styling classes
    await expect(img).toHaveClass(/max-w-full/);
    await expect(img).toHaveClass(/h-auto/);
  });

  test('Demo section has proper heading', async ({ page }) => {
    const demoSection = page.locator('#demo');
    const heading = demoSection.locator('h2');

    await expect(heading).toBeVisible();
    await expect(heading).toContainText(/Action|Demo|Usage/i);
  });

  test('Usage GIF is properly sized within container', async ({ page }) => {
    const demoSection = page.locator('#demo');
    const img = demoSection.locator('img');

    // The image should be visible
    await expect(img).toBeVisible();

    // Get the bounding box to verify it has dimensions
    const boundingBox = await img.boundingBox();
    expect(boundingBox).not.toBeNull();
    expect(boundingBox.width).toBeGreaterThan(0);
    expect(boundingBox.height).toBeGreaterThan(0);
  });
});
