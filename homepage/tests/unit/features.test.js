// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Features Section Tests - Scenario 2
 *
 * These tests validate the features section displays key technical features
 * with icons and descriptions in a grid layout.
 */

test.describe('Features Section', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to the homepage
    await page.goto('/index.html');
  });

  // Test Case 1: Count feature cards in features section
  test('should have at least 5 feature cards (Memcached Protocol, LSM Tree, Skip-List, Compaction, Crash Recovery)', async ({ page }) => {
    const featureCards = page.locator('#features .feature-card');
    const count = await featureCards.count();

    expect(count).toBeGreaterThanOrEqual(5);
  });

  // Test Case 2: Search for 'Memcached Protocol' feature
  test('should have Memcached Protocol feature with title and description about protocol compatibility', async ({ page }) => {
    const featuresSection = page.locator('#features');
    const memcachedCard = featuresSection.locator('.feature-card', {
      has: page.locator('.feature-card__title', { hasText: /Memcached Protocol/i })
    });

    await expect(memcachedCard).toBeVisible();

    const title = memcachedCard.locator('.feature-card__title');
    await expect(title).toContainText('Memcached Protocol');

    const description = memcachedCard.locator('.feature-card__description');
    await expect(description).toBeVisible();
    const descriptionText = await description.textContent();
    // Should mention compatibility or client
    expect(descriptionText?.toLowerCase()).toMatch(/compatib|client|drop-in/i);
  });

  // Test Case 3: Search for 'LSM Tree' feature
  test('should have LSM Tree feature with description of storage engine architecture', async ({ page }) => {
    const featuresSection = page.locator('#features');
    const lsmCard = featuresSection.locator('.feature-card', {
      has: page.locator('.feature-card__title', { hasText: /LSM.*Tree/i })
    });

    await expect(lsmCard).toBeVisible();

    const title = lsmCard.locator('.feature-card__title');
    const titleText = await title.textContent();
    expect(titleText?.toLowerCase()).toMatch(/lsm.*tree|lsm tree/i);

    const description = lsmCard.locator('.feature-card__description');
    await expect(description).toBeVisible();
    const descriptionText = await description.textContent();
    // Should mention storage, merge, or write
    expect(descriptionText?.toLowerCase()).toMatch(/storage|merge|write|throughput/i);
  });

  // Test Case 4: Search for 'Skip-List' feature
  test('should have Skip-List feature with description of memtable implementation', async ({ page }) => {
    const featuresSection = page.locator('#features');
    const skipListCard = featuresSection.locator('.feature-card', {
      has: page.locator('.feature-card__title', { hasText: /Skip.*List/i })
    });

    await expect(skipListCard).toBeVisible();

    const title = skipListCard.locator('.feature-card__title');
    const titleText = await title.textContent();
    expect(titleText?.toLowerCase()).toMatch(/skip.*list/i);

    const description = skipListCard.locator('.feature-card__description');
    await expect(description).toBeVisible();
    const descriptionText = await description.textContent();
    // Should mention memtable, memory, or lookup
    expect(descriptionText?.toLowerCase()).toMatch(/memtable|memory|lookup|in-memory/i);
  });

  // Test Case 6: Check each feature card has icon element
  test('should have an icon or visual indicator in each feature card', async ({ page }) => {
    const featureCards = page.locator('#features .feature-card');
    const count = await featureCards.count();

    expect(count).toBeGreaterThan(0);

    for (let i = 0; i < count; i++) {
      const card = featureCards.nth(i);
      const icon = card.locator('.feature-card__icon');
      await expect(icon).toBeVisible();

      // Check that icon contains an SVG or img element
      const hasSvg = await icon.locator('svg').count();
      const hasImg = await icon.locator('img').count();
      expect(hasSvg + hasImg).toBeGreaterThan(0);
    }
  });
});
