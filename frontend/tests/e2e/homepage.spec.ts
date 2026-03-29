/**
 * Homepage E2E Tests
 * Owner: Scenario 1 - Hero Section Content and Value Proposition
 *
 * E2E tests validating hero section visibility and content
 * Test Case 4: Hero section is visible without scrolling on 1920x1080 viewport
 *
 * Requirements: REQ-1, US-1
 */
import { test, expect } from '@playwright/test';

test.describe('Homepage Hero Section Visibility', () => {
  test.beforeEach(async ({ page }) => {
    // Set viewport to 1920x1080 (standard desktop)
    await page.setViewportSize({ width: 1920, height: 1080 });
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  /**
   * Test Case 4: Hero section is visible without scrolling on 1920x1080 viewport
   * Input: Navigate to homepage URL
   * Expected: Hero section is visible without scrolling on 1920x1080 viewport
   */
  test('hero section is visible without scrolling on 1920x1080 viewport', async ({ page }) => {
    // Wait for the hero section to be present
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Verify hero section is above the fold (visible without scrolling)
    const boundingBox = await heroSection.boundingBox();
    expect(boundingBox).not.toBeNull();

    // The hero section should start at or near the top
    expect(boundingBox!.y).toBeLessThan(100);

    // A significant portion of the hero should be visible in the viewport
    const viewportHeight = 1080;
    const heroVisibleHeight = Math.min(
      boundingBox!.height,
      viewportHeight - boundingBox!.y
    );
    expect(heroVisibleHeight).toBeGreaterThan(viewportHeight * 0.5);
  });

  test('headline displays value proposition text', async ({ page }) => {
    const headline = page.locator('[data-testid="hero-headline"]');
    await expect(headline).toBeVisible();
    await expect(headline).toContainText('Shorten Links');
    await expect(headline).toContainText('Track Clicks');
    await expect(headline).toContainText('Grow Your Reach');
  });

  test('subheadline explains key benefit', async ({ page }) => {
    const subheadline = page.locator('[data-testid="hero-subheadline"]');
    await expect(subheadline).toBeVisible();
    await expect(subheadline).toContainText('Create short, memorable links');
    await expect(subheadline).toContainText('analytics');
  });

  test('product branding is visible', async ({ page }) => {
    const branding = page.locator('[data-testid="product-branding"]');
    await expect(branding).toBeVisible();

    const productName = page.locator('[data-testid="product-name"]');
    await expect(productName).toBeVisible();
    await expect(productName).toContainText('ShortLink');
  });

  test('hero section is in viewport without scrolling', async ({ page }) => {
    // Initial check - hero should be visible
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Verify the element is in the viewport without any scrolling
    await expect(heroSection).toBeInViewport();
  });
});
