/**
 * E2E tests for FeaturesGrid responsive behavior.
 * Owner: Scenario 2 - Features Grid Implementation
 *
 * Test Case 8: Verify cards stack vertically in single column on mobile (375px viewport)
 */

import { test, expect } from '@playwright/test';

test.describe('Features Grid Responsive Layout', () => {
  test('Test Case 8: Cards stack vertically in single column layout on 375px viewport', async ({ page }) => {
    // Set viewport to 375px width (iPhone SE / small mobile)
    await page.setViewportSize({ width: 375, height: 667 });

    // Navigate to the homepage
    await page.goto('/');

    // Wait for the features section to be visible
    const featuresGrid = page.locator('[data-testid="features-grid"]');
    await expect(featuresGrid).toBeVisible();

    // Get all feature cards
    const featureCards = page.locator('[data-testid="feature-card"]');

    // Verify 6 cards are present
    await expect(featureCards).toHaveCount(6);

    // Get bounding boxes of first two cards to verify they're stacked vertically
    const firstCard = featureCards.first();
    const secondCard = featureCards.nth(1);

    await expect(firstCard).toBeVisible();
    await expect(secondCard).toBeVisible();

    const firstBox = await firstCard.boundingBox();
    const secondBox = await secondCard.boundingBox();

    // Verify cards are stacked vertically (second card should be below first)
    expect(firstBox).not.toBeNull();
    expect(secondBox).not.toBeNull();

    if (firstBox && secondBox) {
      // Second card's top should be below first card's bottom
      expect(secondBox.y).toBeGreaterThan(firstBox.y + firstBox.height - 10); // Allow small margin for gap

      // Cards should have similar widths (single column layout)
      const widthDifference = Math.abs(firstBox.width - secondBox.width);
      expect(widthDifference).toBeLessThan(20); // Allow small tolerance

      // Cards should be roughly the same width as the container (minus padding)
      expect(firstBox.width).toBeGreaterThan(300); // Should take most of the 375px viewport
    }
  });

  test('Cards display in multi-column layout on desktop viewport', async ({ page }) => {
    // Set viewport to desktop size
    await page.setViewportSize({ width: 1280, height: 800 });

    // Navigate to the homepage
    await page.goto('/');

    // Wait for the features section to be visible
    const featuresGrid = page.locator('[data-testid="features-grid"]');
    await expect(featuresGrid).toBeVisible();

    // Get all feature cards
    const featureCards = page.locator('[data-testid="feature-card"]');

    // Verify 6 cards are present
    await expect(featureCards).toHaveCount(6);

    // Get bounding boxes of first two cards to verify they're side by side (not stacked)
    const firstCard = featureCards.first();
    const secondCard = featureCards.nth(1);

    const firstBox = await firstCard.boundingBox();
    const secondBox = await secondCard.boundingBox();

    expect(firstBox).not.toBeNull();
    expect(secondBox).not.toBeNull();

    if (firstBox && secondBox) {
      // On desktop, cards should be side by side (same Y position, different X)
      const yDifference = Math.abs(firstBox.y - secondBox.y);
      expect(yDifference).toBeLessThan(10); // Should be on same row

      // Second card should be to the right of first card
      expect(secondBox.x).toBeGreaterThan(firstBox.x + firstBox.width - 10);
    }
  });

  test('All 6 feature cards have correct content', async ({ page }) => {
    await page.goto('/');

    // Wait for the features section to be visible
    const featuresGrid = page.locator('[data-testid="features-grid"]');
    await expect(featuresGrid).toBeVisible();

    // Check each feature card's title
    const expectedTitles = [
      'Memcached Compatible',
      'Persistent Storage',
      'High Performance',
      'Configurable',
      'Compaction',
      'Async I/O',
    ];

    for (const title of expectedTitles) {
      await expect(page.locator('[data-testid="feature-title"]', { hasText: title })).toBeVisible();
    }
  });
});
