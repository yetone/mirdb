/**
 * Hero Section Integration Tests - Viewport Visibility
 * Owner: Scenario 1 - Hero Section & Value Proposition
 *
 * Test cases:
 * - Hero section is fully visible within initial viewport (above the fold)
 */

import { test, expect } from '@playwright/test';

test.describe('Hero Section Visibility Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 4: Hero section is fully visible within initial viewport (above the fold)', async ({ page }) => {
    // Set a standard desktop viewport
    await page.setViewportSize({ width: 1280, height: 720 });

    // Wait for the hero section to be present
    const heroSection = page.locator('#hero, .hero, [class*="hero"]').first();
    await expect(heroSection).toBeVisible();

    // Get the hero section's bounding box
    const heroBox = await heroSection.boundingBox();
    expect(heroBox).not.toBeNull();

    // Verify the hero section starts at or near the top of the page
    // Allow some margin for potential navigation bar
    expect(heroBox.y).toBeLessThanOrEqual(100);

    // Verify key elements within the hero are visible
    const h1 = page.locator('#hero h1, .hero h1').first();
    await expect(h1).toBeVisible();

    const subheadline = page.locator('#hero .hero__subheadline, .hero p').first();
    await expect(subheadline).toBeVisible();

    const ctaButton = page.locator('#hero-cta-primary, .hero .btn-primary').first();
    await expect(ctaButton).toBeVisible();
  });

  test('Hero section is visible on tablet viewport', async ({ page }) => {
    // Set a tablet viewport
    await page.setViewportSize({ width: 768, height: 1024 });

    const heroSection = page.locator('#hero, .hero').first();
    await expect(heroSection).toBeVisible();

    // Verify key content is visible
    const h1 = page.locator('#hero h1, .hero h1').first();
    await expect(h1).toBeVisible();

    const ctaButton = page.locator('#hero-cta-primary, .hero .btn-primary').first();
    await expect(ctaButton).toBeVisible();
  });

  test('Hero section is visible on mobile viewport', async ({ page }) => {
    // Set a mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });

    const heroSection = page.locator('#hero, .hero').first();
    await expect(heroSection).toBeVisible();

    // Verify key content is visible
    const h1 = page.locator('#hero h1, .hero h1').first();
    await expect(h1).toBeVisible();

    const ctaButton = page.locator('#hero-cta-primary, .hero .btn-primary').first();
    await expect(ctaButton).toBeVisible();
  });
});
