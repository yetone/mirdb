/**
 * E2E tests for Features section.
 * Owner: Scenario 2 - Feature Overview Section
 *
 * Tests visual layout and responsive behavior of the features grid.
 */

import { test, expect } from '@playwright/test';

test.describe('Features Section E2E', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForSelector('#features');
  });

  test('feature cards display in a multi-column grid on desktop viewport', async ({ page }) => {
    // Set desktop viewport
    await page.setViewportSize({ width: 1200, height: 800 });

    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(6);

    // Check that the grid has computed style for multi-column layout
    const gridStyle = await featuresGrid.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      return {
        display: computed.display,
        gridTemplateColumns: computed.gridTemplateColumns,
      };
    });

    // Verify it's a grid layout
    expect(gridStyle.display).toBe('grid');

    // gridTemplateColumns should have multiple columns (not '1fr' or single value)
    // For 3-column layout, it will be something like '368px 368px 368px'
    const columns = gridStyle.gridTemplateColumns.split(' ').filter(Boolean);
    expect(columns.length).toBeGreaterThan(1);
  });

  test('features section is accessible', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toHaveAttribute('aria-labelledby', 'features-heading');

    const heading = page.locator('#features-heading');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText('Key Features');
  });

  test('all six required features are displayed', async ({ page }) => {
    const requiredFeatures = [
      'Persistent Storage',
      'Memcached Protocol',
      'LSM Tree Architecture',
      'Skip Lists',
      'Compaction',
      'Async I/O',
    ];

    for (const featureName of requiredFeatures) {
      const featureTitle = page.locator('.feature-title', { hasText: featureName });
      await expect(featureTitle).toBeVisible();
    }
  });

  test('each feature card has icon, title, and description', async ({ page }) => {
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();

    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i);
      const icon = card.locator('.feature-icon');
      const title = card.locator('.feature-title');
      const description = card.locator('.feature-description');

      await expect(icon).toBeVisible();
      await expect(title).toBeVisible();
      await expect(description).toBeVisible();

      // Verify content is not empty
      const iconText = await icon.textContent();
      const titleText = await title.textContent();
      const descText = await description.textContent();

      expect(iconText?.trim().length).toBeGreaterThan(0);
      expect(titleText?.trim().length).toBeGreaterThan(0);
      expect(descText?.trim().length).toBeGreaterThan(0);
    }
  });
});
