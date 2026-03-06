/**
 * Usage Demo E2E Tests
 * Owner: Scenario 7 - Usage Demo GIF Display
 *
 * Test coverage:
 * - Usage GIF image element
 * - GIF loading verification
 * - Alt text for accessibility
 */
import { test, expect } from '@playwright/test';
import { waitForPageLoad, navigateToSection } from './test-utils';

test.describe('Usage Demo GIF Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await waitForPageLoad(page);
  });

  test('TC1: Usage GIF image element exists', async ({ page }) => {
    // Navigate to demo section
    await navigateToSection(page, 'demo');

    // Check for image element with usage demo GIF
    const demoGif = page.locator('[data-testid="usage-demo-gif"]');
    await expect(demoGif).toBeVisible();

    // Verify the src attribute contains 'usage' reference
    const src = await demoGif.getAttribute('src');
    expect(src).toBeTruthy();
    expect(src?.toLowerCase()).toContain('usage');
  });

  test('TC2: GIF loads without error (naturalWidth > 0)', async ({ page }) => {
    // Navigate to demo section
    await navigateToSection(page, 'demo');

    // Get the demo GIF element
    const demoGif = page.locator('[data-testid="usage-demo-gif"]');
    await expect(demoGif).toBeVisible();

    // Wait for the image to load and verify naturalWidth > 0
    await page.waitForFunction(() => {
      const img = document.querySelector('[data-testid="usage-demo-gif"]') as HTMLImageElement;
      return img && img.complete && img.naturalWidth > 0;
    }, { timeout: 10000 });

    // Verify the image has loaded successfully
    const naturalWidth = await demoGif.evaluate((img: HTMLImageElement) => img.naturalWidth);
    expect(naturalWidth).toBeGreaterThan(0);
  });

  test('TC3: GIF has appropriate alt text', async ({ page }) => {
    // Navigate to demo section
    await navigateToSection(page, 'demo');

    // Get the demo GIF element
    const demoGif = page.locator('[data-testid="usage-demo-gif"]');
    await expect(demoGif).toBeVisible();

    // Verify alt attribute exists and describes the usage demonstration
    const altText = await demoGif.getAttribute('alt');
    expect(altText).toBeTruthy();
    expect(altText!.length).toBeGreaterThan(10); // Should be descriptive

    // Alt text should describe the demonstration
    const altLower = altText!.toLowerCase();
    expect(
      altLower.includes('usage') ||
      altLower.includes('demo') ||
      altLower.includes('demonstration') ||
      altLower.includes('mirdb')
    ).toBe(true);
  });

  test('Demo section has proper structure', async ({ page }) => {
    // Verify demo section exists
    const demoSection = page.locator('#demo');
    await expect(demoSection).toBeVisible();

    // Verify it has a title
    const demoTitle = page.locator('#demo-title');
    await expect(demoTitle).toBeVisible();

    // Verify the GIF container exists
    const demoContainer = page.locator('.demo-container');
    await expect(demoContainer).toBeVisible();
  });

  test('Demo GIF has lazy loading for performance', async ({ page }) => {
    // Get the demo GIF element
    const demoGif = page.locator('[data-testid="usage-demo-gif"]');

    // Check for loading="lazy" attribute
    const loading = await demoGif.getAttribute('loading');
    expect(loading).toBe('lazy');
  });
});
