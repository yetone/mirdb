/**
 * Feature Cards E2E Tests
 * Owner: Scenario 4 - Feature Highlights Display
 *
 * Tests:
 * - Test Case 4: Hover over feature card on desktop - Card displays hover effect
 * - Test Case 5: View features section on mobile viewport - Responsive grid layout
 */

import { test, expect } from '@playwright/test';

test.describe('Feature Cards - Desktop Hover Effects', () => {
  test('feature card displays hover effect with visual feedback on desktop', async ({ page }) => {
    // Navigate to homepage
    await page.goto('/');

    // Wait for features section to be visible
    const featuresSection = page.getByTestId('features-section');
    await expect(featuresSection).toBeVisible();

    // Get the first feature card
    const featureCards = page.getByTestId('feature-card');
    const firstCard = featureCards.first();
    await expect(firstCard).toBeVisible();

    // Get the GlassMorphismCard element inside (the one with .card class)
    const glassMorphismCard = firstCard.locator('.card');
    await expect(glassMorphismCard).toBeVisible();

    // Verify hover effect classes exist on the card
    await expect(glassMorphismCard).toHaveClass(/hover:scale-105/);
    await expect(glassMorphismCard).toHaveClass(/hover:shadow-2xl/);
    await expect(glassMorphismCard).toHaveClass(/hover:border-primary/);
    await expect(glassMorphismCard).toHaveClass(/transition-all/);
    await expect(glassMorphismCard).toHaveClass(/duration-300/);

    // Hover over the card to trigger visual effect
    await firstCard.hover();

    // Verify the card is still visible and interactive after hover
    await expect(glassMorphismCard).toBeVisible();
  });

  test('multiple feature cards all have hover effect classes', async ({ page }) => {
    await page.goto('/');

    const featureCards = page.getByTestId('feature-card');
    const count = await featureCards.count();

    // Verify we have 3-5 feature cards
    expect(count).toBeGreaterThanOrEqual(3);
    expect(count).toBeLessThanOrEqual(5);

    // Check each card has hover effect classes
    for (let i = 0; i < count; i++) {
      const card = featureCards.nth(i);
      const glassMorphismCard = card.locator('.card');

      await expect(glassMorphismCard).toHaveClass(/hover:scale-105/);
      await expect(glassMorphismCard).toHaveClass(/transition-all/);
    }
  });
});

test.describe('Feature Cards - Mobile Responsive Layout', () => {
  test.use({ viewport: { width: 375, height: 667 } }); // iPhone SE size

  test('feature cards display in single column layout on mobile', async ({ page }) => {
    await page.goto('/');

    // Wait for features section
    const featuresSection = page.getByTestId('features-section');
    await expect(featuresSection).toBeVisible();

    // Get the grid container
    const featureCards = page.getByTestId('feature-card');
    const firstCard = featureCards.first();
    await expect(firstCard).toBeVisible();

    const gridContainer = firstCard.locator('..');
    await expect(gridContainer).toHaveClass(/grid/);
    await expect(gridContainer).toHaveClass(/grid-cols-1/);

    // Verify cards are stacked vertically (each takes full width)
    const count = await featureCards.count();
    expect(count).toBeGreaterThanOrEqual(3);
    expect(count).toBeLessThanOrEqual(5);

    // Get the first card's bounding box
    const firstBox = await firstCard.boundingBox();
    expect(firstBox).not.toBeNull();

    // On mobile, card should be close to full width (accounting for padding)
    if (firstBox) {
      // Card width should be reasonably large relative to viewport
      expect(firstBox.width).toBeGreaterThan(300); // Most of 375px viewport
    }
  });

  test('all feature card content is visible on mobile', async ({ page }) => {
    await page.goto('/');

    const featureCards = page.getByTestId('feature-card');
    const count = await featureCards.count();

    for (let i = 0; i < count; i++) {
      const card = featureCards.nth(i);

      // Scroll to card to ensure visibility
      await card.scrollIntoViewIfNeeded();

      // Verify icon, title, and description are visible
      const icon = card.getByTestId('feature-icon');
      const title = card.getByTestId('feature-title');
      const description = card.getByTestId('feature-description');

      await expect(icon).toBeVisible();
      await expect(title).toBeVisible();
      await expect(description).toBeVisible();
    }
  });
});

test.describe('Feature Cards - Tablet Responsive Layout', () => {
  test.use({ viewport: { width: 768, height: 1024 } }); // iPad size

  test('feature cards display in 2-column grid on tablet', async ({ page }) => {
    await page.goto('/');

    const featuresSection = page.getByTestId('features-section');
    await expect(featuresSection).toBeVisible();

    // Verify grid has md:grid-cols-2 class (2 columns on medium screens)
    const featureCards = page.getByTestId('feature-card');
    const firstCard = featureCards.first();
    const gridContainer = firstCard.locator('..');

    await expect(gridContainer).toHaveClass(/md:grid-cols-2/);
  });
});

test.describe('Feature Cards - Desktop Responsive Layout', () => {
  test.use({ viewport: { width: 1280, height: 800 } }); // Desktop size

  test('feature cards display in 3-column grid on desktop', async ({ page }) => {
    await page.goto('/');

    const featuresSection = page.getByTestId('features-section');
    await expect(featuresSection).toBeVisible();

    // Verify grid has lg:grid-cols-3 class (3 columns on large screens)
    const featureCards = page.getByTestId('feature-card');
    const firstCard = featureCards.first();
    const gridContainer = firstCard.locator('..');

    await expect(gridContainer).toHaveClass(/lg:grid-cols-3/);
  });
});
