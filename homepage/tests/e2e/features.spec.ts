/**
 * E2E tests for Features section.
 * Owner: Scenario 2 - Value Proposition Features Section
 */

import { test, expect } from '@playwright/test';

test.describe('Features Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case 4: Card displays hover effect (shadow or scale transformation)
  test('TC4: Feature cards display hover effect on mouse over', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Get the first feature card
    const firstCard = page.locator('[data-testid="feature-card-memcached-compatible"]');
    await expect(firstCard).toBeVisible();

    // Get initial computed styles
    const initialStyles = await firstCard.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        transform: styles.transform,
        boxShadow: styles.boxShadow,
      };
    });

    // Hover over the card
    await firstCard.hover();

    // Wait for transition to complete
    await page.waitForTimeout(400);

    // Get styles after hover
    const hoverStyles = await firstCard.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        transform: styles.transform,
        boxShadow: styles.boxShadow,
      };
    });

    // Verify that either transform or boxShadow changed on hover
    const transformChanged = initialStyles.transform !== hoverStyles.transform;
    const shadowChanged = initialStyles.boxShadow !== hoverStyles.boxShadow;

    expect(
      transformChanged || shadowChanged,
      `Expected hover effect to change transform or shadow. Initial transform: ${initialStyles.transform}, hover transform: ${hoverStyles.transform}. Initial shadow: ${initialStyles.boxShadow}, hover shadow: ${hoverStyles.boxShadow}`
    ).toBe(true);
  });

  test('all three feature cards have hover effects', async ({ page }) => {
    const cardIds = [
      'feature-card-memcached-compatible',
      'feature-card-persistent-storage',
      'feature-card-rust-performance',
    ];

    for (const cardId of cardIds) {
      const card = page.locator(`[data-testid="${cardId}"]`);
      await expect(card).toBeVisible();

      // Check that the card has the hover transition class
      const hasHoverClass = await card.evaluate((el) => {
        return el.classList.contains('transition-all') ||
               el.className.includes('hover:');
      });

      // Alternative: Check for hover:-translate-y-1 or hover:shadow-xl in class
      const className = await card.getAttribute('class');
      const hasHoverStyles =
        className?.includes('hover:shadow-xl') ||
        className?.includes('hover:-translate-y-1');

      expect(hasHoverClass || hasHoverStyles).toBe(true);
    }
  });

  test('features section displays below hero', async ({ page }) => {
    const heroSection = page.locator('#hero');
    const featuresSection = page.locator('#features');

    await expect(heroSection).toBeVisible();
    await expect(featuresSection).toBeVisible();

    // Get the bounding boxes to verify order
    const heroBox = await heroSection.boundingBox();
    const featuresBox = await featuresSection.boundingBox();

    expect(heroBox).not.toBeNull();
    expect(featuresBox).not.toBeNull();

    // Features section should be below hero (higher y value)
    expect(featuresBox!.y).toBeGreaterThan(heroBox!.y);
  });

  test('features section has three-column layout on desktop', async ({ page }) => {
    // Set desktop viewport
    await page.setViewportSize({ width: 1280, height: 720 });

    const featureCards = page.locator('[data-testid^="feature-card-"]');
    await expect(featureCards).toHaveCount(3);

    // Get bounding boxes to check horizontal layout
    const boxes = await featureCards.evaluateAll((cards) => {
      return cards.map((card) => card.getBoundingClientRect());
    });

    // All cards should be on roughly the same row (similar y values)
    const yValues = boxes.map((box) => box.y);
    const yDifference = Math.max(...yValues) - Math.min(...yValues);

    // Allow some tolerance for minor differences
    expect(yDifference).toBeLessThan(10);

    // Cards should be horizontally distributed
    const xValues = boxes.map((box) => box.x);
    expect(xValues[0]).toBeLessThan(xValues[1]);
    expect(xValues[1]).toBeLessThan(xValues[2]);
  });

  test('features section stacks on mobile', async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });

    const featureCards = page.locator('[data-testid^="feature-card-"]');
    await expect(featureCards).toHaveCount(3);

    // Get bounding boxes to check vertical layout
    const boxes = await featureCards.evaluateAll((cards) => {
      return cards.map((card) => card.getBoundingClientRect());
    });

    // Cards should be stacked vertically (different y values)
    const yValues = boxes.map((box) => box.y);
    expect(yValues[0]).toBeLessThan(yValues[1]);
    expect(yValues[1]).toBeLessThan(yValues[2]);
  });
});
