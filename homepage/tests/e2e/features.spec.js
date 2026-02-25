/**
 * Features Section E2E Tests
 * Owner: Scenario 3 - Features Section Display
 *
 * Test cases:
 * - 4-6 feature cards are displayed
 * - Each required feature is present
 * - Hover effects work on cards
 */

const { test, expect } = require('@playwright/test');

test.describe('Features Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('should display between 4 and 6 feature cards', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();

    // Count feature cards
    const featureCards = page.locator('.feature-card');
    const count = await featureCards.count();

    expect(count).toBeGreaterThanOrEqual(4);
    expect(count).toBeLessThanOrEqual(6);
  });

  test('should have Memcached protocol feature', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();

    // Check for Memcached protocol feature
    const memcachedCard = page.locator('.feature-card').filter({
      has: page.locator('text=/memcached protocol|memcached compatible/i')
    });

    await expect(memcachedCard).toBeVisible();
  });

  test('should have disk persistence feature', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();

    // Check for disk persistence feature
    const persistenceCard = page.locator('.feature-card').filter({
      has: page.locator('text=/disk persistence|persistent storage/i')
    });

    await expect(persistenceCard).toBeVisible();
  });

  test('should have LSM tree feature', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();

    // Check for LSM tree feature
    const lsmCard = page.locator('.feature-card').filter({
      has: page.locator('text=/LSM tree/i')
    });

    await expect(lsmCard).toBeVisible();
  });

  test('should have multi-level compaction feature', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();

    // Check for compaction feature
    const compactionCard = page.locator('.feature-card').filter({
      has: page.locator('text=/multi-level compaction|compaction/i')
    });

    await expect(compactionCard).toBeVisible();
  });

  test('should have async I/O feature', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();

    // Check for async I/O feature
    const asyncCard = page.locator('.feature-card').filter({
      has: page.locator('text=/async I\\/O|asynchronous/i')
    });

    await expect(asyncCard).toBeVisible();
  });

  test('should display visual hover effect on feature cards', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();

    const firstCard = page.locator('.feature-card').first();

    // Get initial transform and box-shadow values
    const initialTransform = await firstCard.evaluate(
      (el) => window.getComputedStyle(el).transform
    );
    const initialBoxShadow = await firstCard.evaluate(
      (el) => window.getComputedStyle(el).boxShadow
    );

    // Hover over the card
    await firstCard.hover();

    // Wait for transition to complete
    await page.waitForTimeout(300);

    // Get values after hover
    const hoverTransform = await firstCard.evaluate(
      (el) => window.getComputedStyle(el).transform
    );
    const hoverBoxShadow = await firstCard.evaluate(
      (el) => window.getComputedStyle(el).boxShadow
    );

    // Verify that either transform or box-shadow changed (indicating hover effect)
    const transformChanged = initialTransform !== hoverTransform;
    const boxShadowChanged = initialBoxShadow !== hoverBoxShadow;

    expect(transformChanged || boxShadowChanged).toBe(true);
  });

  test('each feature card should have an icon and description', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();

    const featureCards = page.locator('.feature-card');
    const count = await featureCards.count();

    for (let i = 0; i < count; i++) {
      const card = featureCards.nth(i);

      // Check for icon
      const icon = card.locator('.feature-icon');
      await expect(icon).toBeVisible();

      // Check for title
      const title = card.locator('.feature-title');
      await expect(title).toBeVisible();

      // Check for description
      const description = card.locator('.feature-description');
      await expect(description).toBeVisible();
    }
  });
});
