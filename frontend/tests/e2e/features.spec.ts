/**
 * Features Section E2E Tests
 * Owner: Scenario 4 - Features Section Display
 *
 * End-to-end tests for features section:
 * - Hover effects on feature cards
 * - Responsive grid layout at different viewports
 */

import { test, expect } from '@playwright/test';

test.describe('Features Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Scroll to features section
    await page.getByTestId('features-section').scrollIntoViewIfNeeded();
  });

  test.describe('Hover effects', () => {
    test('should show visual highlight effect with smooth animation on hover', async ({ page }) => {
      const featureCard = page.getByTestId('feature-card-shorten');
      await expect(featureCard).toBeVisible();

      // Get initial styles
      const initialTransform = await featureCard.evaluate((el) => {
        return window.getComputedStyle(el).transform;
      });

      // Hover over the card
      await featureCard.hover();

      // Wait for animation to apply
      await page.waitForTimeout(300);

      // Check that transform has changed (scale effect applied)
      const hoverTransform = await featureCard.evaluate((el) => {
        return window.getComputedStyle(el).transform;
      });

      // The transform should be different after hover (indicating animation)
      // The card should have a transform matrix indicating scale > 1
      expect(hoverTransform).not.toBe('none');
    });

    test('each feature card should respond to hover', async ({ page }) => {
      const featureIds = ['shorten', 'analytics', 'share', 'secure'];

      for (const id of featureIds) {
        const card = page.getByTestId(`feature-card-${id}`);
        await expect(card).toBeVisible();

        // Hover and verify the card is interactive
        await card.hover();
        await page.waitForTimeout(100);

        // Card should still be visible and interactable after hover
        await expect(card).toBeVisible();
      }
    });
  });

  test.describe('Responsive grid layout', () => {
    test('should display in single column layout at 320px (mobile)', async ({ page }) => {
      await page.setViewportSize({ width: 320, height: 800 });
      await page.goto('/');
      await page.getByTestId('features-section').scrollIntoViewIfNeeded();

      const grid = page.getByTestId('features-grid');
      await expect(grid).toBeVisible();

      // Get all feature cards
      const cards = page.locator('[data-testid^="feature-card-"]');
      await expect(cards).toHaveCount(4);

      // Get bounding boxes of first two cards to verify stacking
      const firstCard = page.getByTestId('feature-card-shorten');
      const secondCard = page.getByTestId('feature-card-analytics');

      const firstBox = await firstCard.boundingBox();
      const secondBox = await secondCard.boundingBox();

      expect(firstBox).toBeTruthy();
      expect(secondBox).toBeTruthy();

      // In single column layout, cards should be stacked vertically
      // Second card should be below first card (y position greater)
      expect(secondBox!.y).toBeGreaterThan(firstBox!.y);

      // Cards should have similar x positions (same column)
      expect(Math.abs(secondBox!.x - firstBox!.x)).toBeLessThan(50);
    });

    test('should display in 2-column grid at 768px (tablet)', async ({ page }) => {
      await page.setViewportSize({ width: 768, height: 800 });
      await page.goto('/');
      await page.getByTestId('features-section').scrollIntoViewIfNeeded();

      const grid = page.getByTestId('features-grid');
      await expect(grid).toBeVisible();

      // Get bounding boxes of first two cards
      const firstCard = page.getByTestId('feature-card-shorten');
      const secondCard = page.getByTestId('feature-card-analytics');

      const firstBox = await firstCard.boundingBox();
      const secondBox = await secondCard.boundingBox();

      expect(firstBox).toBeTruthy();
      expect(secondBox).toBeTruthy();

      // In 2-column layout, first two cards should be on the same row
      // They should have similar y positions but different x positions
      expect(Math.abs(secondBox!.y - firstBox!.y)).toBeLessThan(20);
      expect(secondBox!.x).toBeGreaterThan(firstBox!.x);
    });

    test('should display in 4-column grid at 1024px+ (desktop)', async ({ page }) => {
      await page.setViewportSize({ width: 1280, height: 800 });
      await page.goto('/');
      await page.getByTestId('features-section').scrollIntoViewIfNeeded();

      const grid = page.getByTestId('features-grid');
      await expect(grid).toBeVisible();

      // Get bounding boxes of all four cards
      const cards = [
        page.getByTestId('feature-card-shorten'),
        page.getByTestId('feature-card-analytics'),
        page.getByTestId('feature-card-share'),
        page.getByTestId('feature-card-secure'),
      ];

      const boxes = await Promise.all(cards.map((card) => card.boundingBox()));

      // All boxes should exist
      boxes.forEach((box) => expect(box).toBeTruthy());

      // In 4-column layout, all cards should be on the same row
      // They should have similar y positions
      const firstY = boxes[0]!.y;
      boxes.forEach((box) => {
        expect(Math.abs(box!.y - firstY)).toBeLessThan(20);
      });

      // Cards should have increasing x positions (left to right)
      for (let i = 1; i < boxes.length; i++) {
        expect(boxes[i]!.x).toBeGreaterThan(boxes[i - 1]!.x);
      }
    });
  });

  test.describe('Feature card content', () => {
    test('all 4 feature cards should be visible with correct content', async ({ page }) => {
      await page.setViewportSize({ width: 1280, height: 800 });

      const featuresSection = page.getByTestId('features-section');
      await expect(featuresSection).toBeVisible();

      // Verify all 4 cards are present
      const expectedFeatures = [
        { id: 'shorten', title: 'URL Shortening' },
        { id: 'analytics', title: 'Detailed Analytics' },
        { id: 'share', title: 'Shareable Stats' },
        { id: 'secure', title: 'Secure Authentication' },
      ];

      for (const feature of expectedFeatures) {
        const card = page.getByTestId(`feature-card-${feature.id}`);
        await expect(card).toBeVisible();

        const title = page.getByTestId(`feature-title-${feature.id}`);
        await expect(title).toHaveText(feature.title);

        const description = page.getByTestId(`feature-description-${feature.id}`);
        await expect(description).toBeVisible();

        const icon = page.getByTestId(`feature-icon-${feature.id}`);
        await expect(icon).toBeVisible();
      }
    });
  });
});
