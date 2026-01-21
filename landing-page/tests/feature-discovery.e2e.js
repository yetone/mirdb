/**
 * E2E Tests for Feature Discovery User Journey
 * Testing User Story 2: Feature Discovery
 * Scenario: Verify potential customer can explore product features and benefits
 */

import { test, expect } from '@playwright/test';

test.describe('Feature Discovery User Journey E2E Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Scroll to features section
   * Input: Scroll to features section
   * Expected: Features section becomes visible after scrolling past hero
   */
  test.describe('Test Case 1: Scroll to Features Section', () => {
    test('features section should become visible after scrolling past hero', async ({ page }) => {
      // Verify hero section is visible initially
      const heroSection = page.locator('.hero');
      await expect(heroSection).toBeVisible();

      // Features section should exist but may not be in viewport initially
      const featuresSection = page.locator('#features, .features');
      await expect(featuresSection).toBeAttached();

      // Scroll to features section
      await featuresSection.scrollIntoViewIfNeeded();

      // Wait for scroll animation to complete
      await page.waitForTimeout(500);

      // Features section should now be visible
      await expect(featuresSection).toBeVisible();
      await expect(featuresSection).toBeInViewport();
    });

    test('features section should have heading visible after scrolling', async ({ page }) => {
      const featuresSection = page.locator('#features, .features');
      await featuresSection.scrollIntoViewIfNeeded();

      await page.waitForTimeout(300);

      const heading = featuresSection.locator('h2');
      await expect(heading).toBeVisible();

      const headingText = await heading.textContent();
      expect(headingText.trim().length).toBeGreaterThan(0);
    });

    test('navigation to features section via anchor link should work', async ({ page }) => {
      // Click on Features link in navigation
      const featuresLink = page.locator('a[href="#features"]').first();
      await expect(featuresLink).toBeVisible();

      await featuresLink.click();

      // Wait for smooth scroll animation
      await page.waitForTimeout(1000);

      // Features section should be visible and in viewport
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeVisible();
    });

    test('features section should be positioned below hero section', async ({ page }) => {
      const heroSection = page.locator('.hero');
      const featuresSection = page.locator('.features');

      const heroBox = await heroSection.boundingBox();
      const featuresBox = await featuresSection.boundingBox();

      expect(heroBox).not.toBeNull();
      expect(featuresBox).not.toBeNull();

      // Features section top should be at or below hero section bottom
      expect(featuresBox.y).toBeGreaterThanOrEqual(heroBox.y + heroBox.height - 100); // Allow some overlap for sticky header
    });
  });

  /**
   * Test Case 2: Verify at least 3 features are visible
   */
  test.describe('Test Case 2: Feature Cards Visibility', () => {
    test('should display at least 3 feature cards', async ({ page }) => {
      const featuresSection = page.locator('.features');
      await featuresSection.scrollIntoViewIfNeeded();

      const featureCards = featuresSection.locator('.feature-card');
      const count = await featureCards.count();

      expect(count).toBeGreaterThanOrEqual(3);
    });

    test('all feature cards should be visible when features section is in view', async ({ page }) => {
      const featuresSection = page.locator('.features');
      await featuresSection.scrollIntoViewIfNeeded();

      await page.waitForTimeout(500);

      const featureCards = featuresSection.locator('.feature-card');
      const count = await featureCards.count();

      for (let i = 0; i < count; i++) {
        await expect(featureCards.nth(i)).toBeVisible();
      }
    });
  });

  /**
   * Test Case 3: Verify feature card completeness
   */
  test.describe('Test Case 3: Feature Card Structure', () => {
    test('each feature card should have visible icon, title, and description', async ({ page }) => {
      const featuresSection = page.locator('.features');
      await featuresSection.scrollIntoViewIfNeeded();

      const featureCards = featuresSection.locator('.feature-card');
      const count = await featureCards.count();

      expect(count).toBeGreaterThan(0);

      for (let i = 0; i < count; i++) {
        const card = featureCards.nth(i);

        // Check for icon
        const icon = card.locator('.feature-icon');
        await expect(icon).toBeVisible();

        // Check for title
        const title = card.locator('h3');
        await expect(title).toBeVisible();
        const titleText = await title.textContent();
        expect(titleText.trim().length).toBeGreaterThan(0);

        // Check for description
        const description = card.locator('p');
        await expect(description).toBeVisible();
        const descText = await description.textContent();
        expect(descText.trim().length).toBeGreaterThan(0);
      }
    });

    test('feature icons should have visible content', async ({ page }) => {
      const featuresSection = page.locator('.features');
      await featuresSection.scrollIntoViewIfNeeded();

      const featureIcons = featuresSection.locator('.feature-icon');
      const count = await featureIcons.count();

      expect(count).toBeGreaterThan(0);

      for (let i = 0; i < count; i++) {
        const icon = featureIcons.nth(i);
        await expect(icon).toBeVisible();

        const boundingBox = await icon.boundingBox();
        expect(boundingBox).not.toBeNull();
        expect(boundingBox.width).toBeGreaterThan(0);
        expect(boundingBox.height).toBeGreaterThan(0);
      }
    });
  });

  /**
   * Test Case 4: Verify product visuals presence
   */
  test.describe('Test Case 4: Product Visuals', () => {
    test('product showcase section should be visible after scrolling', async ({ page }) => {
      const showcaseSection = page.locator('.product-showcase');
      await showcaseSection.scrollIntoViewIfNeeded();

      await page.waitForTimeout(500);

      await expect(showcaseSection).toBeVisible();
    });

    test('product images should be visible in showcase section', async ({ page }) => {
      const showcaseSection = page.locator('.product-showcase');
      await showcaseSection.scrollIntoViewIfNeeded();

      await page.waitForTimeout(500);

      const images = showcaseSection.locator('img');
      const count = await images.count();

      expect(count).toBeGreaterThanOrEqual(1);

      for (let i = 0; i < count; i++) {
        await expect(images.nth(i)).toBeVisible();
      }
    });

    test('product images should have proper alt text', async ({ page }) => {
      const showcaseSection = page.locator('.product-showcase');
      const images = showcaseSection.locator('img');
      const count = await images.count();

      expect(count).toBeGreaterThanOrEqual(1);

      for (let i = 0; i < count; i++) {
        const alt = await images.nth(i).getAttribute('alt');
        expect(alt).not.toBeNull();
        expect(alt.trim().length).toBeGreaterThan(0);
      }
    });

    test('product images should have non-zero dimensions', async ({ page }) => {
      const showcaseSection = page.locator('.product-showcase');
      await showcaseSection.scrollIntoViewIfNeeded();

      await page.waitForTimeout(1000);

      const images = showcaseSection.locator('img');
      const count = await images.count();

      expect(count).toBeGreaterThanOrEqual(1);

      for (let i = 0; i < count; i++) {
        const boundingBox = await images.nth(i).boundingBox();
        expect(boundingBox).not.toBeNull();
        expect(boundingBox.width).toBeGreaterThan(0);
        expect(boundingBox.height).toBeGreaterThan(0);
      }
    });
  });

  /**
   * User Journey Flow Test - Complete Feature Discovery
   */
  test.describe('Complete Feature Discovery Flow', () => {
    test('user should be able to discover features by scrolling through the page', async ({ page }) => {
      // Step 1: User lands on homepage, sees hero section
      const heroSection = page.locator('.hero');
      await expect(heroSection).toBeVisible();

      // Step 2: User scrolls past hero section
      const featuresSection = page.locator('.features');
      await featuresSection.scrollIntoViewIfNeeded();
      await page.waitForTimeout(500);

      // Step 3: User views features section with at least 3 key benefits
      await expect(featuresSection).toBeVisible();
      const featureCards = featuresSection.locator('.feature-card');
      const featureCount = await featureCards.count();
      expect(featureCount).toBeGreaterThanOrEqual(3);

      // Step 4: User verifies each feature has icon, title, description
      for (let i = 0; i < featureCount; i++) {
        const card = featureCards.nth(i);
        await expect(card.locator('.feature-icon')).toBeVisible();
        await expect(card.locator('h3')).toBeVisible();
        await expect(card.locator('p')).toBeVisible();
      }

      // Step 5: User continues to product showcase
      const showcaseSection = page.locator('.product-showcase');
      await showcaseSection.scrollIntoViewIfNeeded();
      await page.waitForTimeout(500);

      // Step 6: User views product visuals
      await expect(showcaseSection).toBeVisible();
      const productImages = showcaseSection.locator('img');
      const imageCount = await productImages.count();
      expect(imageCount).toBeGreaterThanOrEqual(1);

      for (let i = 0; i < imageCount; i++) {
        await expect(productImages.nth(i)).toBeVisible();
      }
    });
  });
});
