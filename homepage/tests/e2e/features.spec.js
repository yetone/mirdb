/**
 * Features Section E2E Tests
 * Owner: Scenario 2 - Features Section
 *
 * Test cases:
 * - Feature cards display in grid layout on desktop
 * - Grid layout responsiveness
 */

import { test, expect } from '@playwright/test';

test.describe('Features Section - Grid Layout', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 3: Feature cards display in a grid layout (2-3 columns) on desktop', async ({ page }) => {
    // Set desktop viewport
    await page.setViewportSize({ width: 1280, height: 800 });

    // Navigate to features section
    await page.locator('#features').scrollIntoViewIfNeeded();

    // Get the features grid
    const grid = page.locator('.features-grid');
    await expect(grid).toBeVisible();

    // Check that grid has proper CSS grid display
    const display = await grid.evaluate(el => window.getComputedStyle(el).display);
    expect(display).toBe('grid');

    // Check grid-template-columns shows 3 columns on desktop (1200px+ width)
    const gridColumns = await grid.evaluate(el => window.getComputedStyle(el).gridTemplateColumns);
    // Should have 3 column values
    const columnCount = gridColumns.split(' ').filter(col => col && !col.includes('0px')).length;
    expect(columnCount).toBeGreaterThanOrEqual(2);
    expect(columnCount).toBeLessThanOrEqual(3);

    // Verify all 6 feature cards are visible
    const cards = page.locator('.feature-card');
    await expect(cards).toHaveCount(6);

    // Verify each card is visible
    for (let i = 0; i < 6; i++) {
      await expect(cards.nth(i)).toBeVisible();
    }
  });

  test('Feature cards should be arranged in rows on desktop', async ({ page }) => {
    await page.setViewportSize({ width: 1280, height: 800 });
    await page.locator('#features').scrollIntoViewIfNeeded();

    const cards = page.locator('.feature-card');
    const firstCardBox = await cards.nth(0).boundingBox();
    const secondCardBox = await cards.nth(1).boundingBox();

    // First two cards should be on the same row (same Y position approximately)
    expect(Math.abs(firstCardBox.y - secondCardBox.y)).toBeLessThan(10);
  });

  test('Features section should have proper section title', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();

    const title = featuresSection.locator('.section-title');
    await expect(title).toBeVisible();
    await expect(title).toContainText('Features');
  });

  test('Each feature card should display icon, title, and description', async ({ page }) => {
    await page.locator('#features').scrollIntoViewIfNeeded();

    const cards = page.locator('.feature-card');
    const count = await cards.count();

    for (let i = 0; i < count; i++) {
      const card = cards.nth(i);

      // Check icon
      const icon = card.locator('.feature-icon');
      await expect(icon).toBeVisible();

      // Check title
      const title = card.locator('.feature-title');
      await expect(title).toBeVisible();
      const titleText = await title.textContent();
      expect(titleText.trim().length).toBeGreaterThan(0);

      // Check description
      const description = card.locator('.feature-description');
      await expect(description).toBeVisible();
      const descText = await description.textContent();
      expect(descText.trim().length).toBeGreaterThan(0);
    }
  });
});
