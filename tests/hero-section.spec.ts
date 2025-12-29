import { test, expect } from '@playwright/test';

test.describe('Hero Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('hero section contains h1 with MirDB text', async ({ page }) => {
    // Test Case 1: Load landing page and inspect hero section
    // Expected: Hero section contains h1 with 'MirDB' text

    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    const productName = heroSection.locator('h1[data-testid="hero-product-name"]');
    await expect(productName).toBeVisible();
    await expect(productName).toHaveText('MirDB');
  });

  test('tagline text is visible', async ({ page }) => {
    // Test Case 2: Check tagline element presence
    // Expected: Tagline text 'Persistent Memcached-compatible key-value store' is visible

    const tagline = page.locator('[data-testid="hero-tagline"]');
    await expect(tagline).toBeVisible();
    await expect(tagline).toHaveText('Persistent Memcached-compatible key-value store');
  });

  test('hero section is above the fold on desktop viewport', async ({ page }) => {
    // Test Case 3: Verify hero section is above the fold on desktop
    // Expected: Hero section is fully visible without scrolling on 1920x1080 viewport

    await page.setViewportSize({ width: 1920, height: 1080 });

    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Check that the hero section is within the viewport
    const boundingBox = await heroSection.boundingBox();
    expect(boundingBox).not.toBeNull();

    if (boundingBox) {
      // Verify the hero section starts at or near the top of the viewport
      expect(boundingBox.y).toBeGreaterThanOrEqual(0);

      // Verify the hero section is fully visible (bottom edge within viewport)
      const bottomEdge = boundingBox.y + boundingBox.height;
      expect(bottomEdge).toBeLessThanOrEqual(1080);
    }
  });

  test('hero section has proper semantic structure', async ({ page }) => {
    // Additional test for semantic HTML structure
    const heroSection = page.locator('section.hero');
    await expect(heroSection).toBeVisible();

    // Verify h1 exists within hero section
    const h1 = heroSection.locator('h1');
    await expect(h1).toBeVisible();
    await expect(h1).toContainText('MirDB');
  });

  test('value proposition is displayed', async ({ page }) => {
    // Verify the value proposition text is visible
    const valueProp = page.locator('[data-testid="hero-value-proposition"]');
    await expect(valueProp).toBeVisible();
    await expect(valueProp).toContainText('Drop-in Memcached replacement');
    await expect(valueProp).toContainText('LSM tree architecture');
  });
});
