/**
 * Usage Section E2E Tests
 * Owner: Scenario 4 - Usage Demonstration
 *
 * Test cases:
 * 4. Load usage.gif and verify it loads successfully
 */

import { test, expect } from '@playwright/test';

test.describe('Usage Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case 4: Load usage.gif and verify it loads successfully
  test('usage.gif loads successfully without 404 or other errors', async ({ page }) => {
    // Navigate to usage section
    const usageSection = page.locator('#usage');
    await usageSection.scrollIntoViewIfNeeded();

    // Wait for the image to be visible
    const usageGif = page.locator('[data-testid="usage-gif"]');
    await expect(usageGif).toBeVisible();

    // Get the image src
    const src = await usageGif.getAttribute('src');
    expect(src).toContain('usage.gif');

    // Verify the image loads successfully by checking its natural dimensions
    const isLoaded = await usageGif.evaluate((img: HTMLImageElement) => {
      return img.complete && img.naturalHeight > 0 && img.naturalWidth > 0;
    });
    expect(isLoaded).toBe(true);
  });

  test('usage section is visible', async ({ page }) => {
    const usageSection = page.locator('#usage');
    await expect(usageSection).toBeVisible();
  });

  test('usage section has explanatory text', async ({ page }) => {
    const usageExplanation = page.locator('[data-testid="usage-explanation"]');
    await expect(usageExplanation).toBeVisible();

    const text = await usageExplanation.textContent();
    expect(text).toBeTruthy();
    expect(text!.length).toBeGreaterThan(50);
  });

  test('documentation link is present and clickable', async ({ page }) => {
    const docLink = page.locator('[data-testid="documentation-link"]');
    await expect(docLink).toBeVisible();

    const href = await docLink.getAttribute('href');
    expect(href).toContain('github.com');
    expect(href).toMatch(/readme/i);
  });

  test('usage section has proper heading', async ({ page }) => {
    const heading = page.locator('#usage-title');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText('See It in Action');
  });

  test('usage.gif has proper alt text', async ({ page }) => {
    const usageGif = page.locator('[data-testid="usage-gif"]');
    await expect(usageGif).toBeVisible();

    const altText = await usageGif.getAttribute('alt');
    expect(altText).toBeTruthy();
    expect(altText!.length).toBeGreaterThan(20);
  });

  test('documentation link opens in new tab', async ({ page }) => {
    const docLink = page.locator('[data-testid="documentation-link"]');

    await expect(docLink).toHaveAttribute('target', '_blank');
    await expect(docLink).toHaveAttribute('rel', /noopener/);
  });

  test('usage section can be navigated to via anchor', async ({ page }) => {
    // Navigate directly to the usage section via anchor
    await page.goto('/#usage');

    const usageSection = page.locator('#usage');
    await expect(usageSection).toBeVisible();
  });
});
