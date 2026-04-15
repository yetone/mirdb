/**
 * Features Section E2E Tests
 * Owner: Scenario 2 - Feature List Display
 *
 * Tests for verifying the features section displays all key capabilities:
 * - Memcached protocol support
 * - Disk persistence
 * - LSM tree architecture
 * - Rust implementation
 */

import { test, expect } from '@playwright/test';

test.describe('Features Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('features section exists with identifiable container element', async ({ page }) => {
    // Test case 1: Features section exists
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Verify it has the features class
    await expect(featuresSection).toHaveClass(/features/);

    // Verify heading exists
    const heading = featuresSection.locator('h2');
    await expect(heading).toContainText('Features');
  });

  test('feature list contains Memcached protocol', async ({ page }) => {
    // Test case 2: Memcached protocol feature exists
    const featuresSection = page.locator('#features');
    const featureCards = featuresSection.locator('.feature-card');

    // Find card with Memcached text in title
    const memcachedFeature = featureCards.filter({
      has: page.locator('.feature-card__title:text-matches("Memcached", "i")')
    });

    await expect(memcachedFeature).toBeVisible();

    // Verify title contains Memcached
    const title = memcachedFeature.locator('.feature-card__title');
    await expect(title).toContainText(/Memcached/i);
  });

  test('feature list contains persistence', async ({ page }) => {
    // Test case 3: Disk persistence feature exists
    const featuresSection = page.locator('#features');
    const featureCards = featuresSection.locator('.feature-card');

    // Find card with persistence or disk text
    const persistenceFeature = featureCards.filter({
      has: page.locator('.feature-card__title:text-matches("Persist|Disk", "i")')
    });

    await expect(persistenceFeature).toBeVisible();

    // Verify it describes persistence
    const description = persistenceFeature.locator('.feature-card__description');
    await expect(description).toContainText(/persist|disk|survives/i);
  });

  test('feature list contains LSM tree', async ({ page }) => {
    // Test case 4: LSM tree architecture feature exists
    const featuresSection = page.locator('#features');
    const featureCards = featuresSection.locator('.feature-card');

    // Find card with LSM text
    const lsmFeature = featureCards.filter({
      has: page.locator('.feature-card__title:text-matches("LSM", "i")')
    });

    await expect(lsmFeature).toBeVisible();

    // Verify title mentions LSM
    const title = lsmFeature.locator('.feature-card__title');
    await expect(title).toContainText(/LSM/i);
  });

  test('feature list contains Rust', async ({ page }) => {
    // Test case 5: Rust implementation feature exists
    const featuresSection = page.locator('#features');
    const featureCards = featuresSection.locator('.feature-card');

    // Find card with Rust text
    const rustFeature = featureCards.filter({
      has: page.locator('.feature-card__title:text-matches("Rust", "i")')
    });

    await expect(rustFeature).toBeVisible();

    // Verify title or description mentions Rust
    await expect(rustFeature).toContainText(/Rust/i);
  });

  test('all four key features are displayed with icons and styling', async ({ page }) => {
    // Verify exactly 4 feature cards exist
    const featuresSection = page.locator('#features');
    const featureCards = featuresSection.locator('.feature-card');

    await expect(featureCards).toHaveCount(4);

    // Each card should have an icon, title, and description
    for (let i = 0; i < 4; i++) {
      const card = featureCards.nth(i);
      await expect(card.locator('.feature-card__icon')).toBeVisible();
      await expect(card.locator('.feature-card__title')).toBeVisible();
      await expect(card.locator('.feature-card__description')).toBeVisible();
    }
  });

  // Note: Navigation test is skipped - navigation is owned by Scenario 12
  // The features section is accessible via direct URL navigation
  test('features section is accessible via direct URL', async ({ page }) => {
    // Navigate directly to features section
    await page.goto('/#features');

    // Verify features section is visible
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();
  });
});
