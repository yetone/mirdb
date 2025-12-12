import { test, expect } from '@playwright/test';

test.describe('Homepage Hero Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: Hero section is rendered with visible headline text', async ({ page }) => {
    // Navigate to root URL and verify hero section with headline
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    const headline = page.locator('[data-testid="hero-headline"]');
    await expect(headline).toBeVisible();
    await expect(headline).toHaveText(/./); // Has some text content
  });

  test('Test Case 2: Primary call-to-action button is present and clickable', async ({ page }) => {
    // Verify CTA button is present and clickable
    const ctaButton = page.locator('[data-testid="hero-cta"]');
    await expect(ctaButton).toBeVisible();
    await expect(ctaButton).toBeEnabled();

    // Verify it has proper link attributes
    const href = await ctaButton.getAttribute('href');
    expect(href).toBeTruthy();

    // Click the button to verify it's interactive
    await ctaButton.click();
  });

  test('Hero section has full-width banner layout', async ({ page }) => {
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Verify full-width by checking the hero section spans most of the viewport
    const viewportWidth = page.viewportSize()?.width ?? 1280;
    const boundingBox = await heroSection.boundingBox();

    expect(boundingBox).not.toBeNull();
    if (boundingBox) {
      expect(boundingBox.width).toBeGreaterThanOrEqual(viewportWidth * 0.9);
    }
  });

  test('Hero section contains subheadline text', async ({ page }) => {
    const subheadline = page.locator('[data-testid="hero-subheadline"]');
    await expect(subheadline).toBeVisible();
    await expect(subheadline).toHaveText(/./); // Has some text content
  });

  test('CTA button has proper styling for visibility', async ({ page }) => {
    const ctaButton = page.locator('[data-testid="hero-cta"]');

    // Check button has a background color (primary CTA should be visible)
    const backgroundColor = await ctaButton.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });

    // Should not be transparent
    expect(backgroundColor).not.toBe('rgba(0, 0, 0, 0)');
    expect(backgroundColor).not.toBe('transparent');
  });
});
