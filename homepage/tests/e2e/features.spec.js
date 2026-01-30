/**
 * Features Section Tests
 * Owner: Scenario 3 - Features Section
 *
 * Tests:
 * - Feature cards presence (minimum 4)
 * - Memcached protocol feature
 * - Persistent storage feature
 * - Performance features
 * - Rust implementation feature
 */

const { test, expect } = require('@playwright/test');

const BASE_URL = process.env.BASE_URL || 'http://localhost:3000';

test.describe('Features Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(BASE_URL);
  });

  test('features section exists with id="features"', async ({ page }) => {
    // Test case 1: Check features section element
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();
    await expect(featuresSection).toHaveAttribute('id', 'features');
  });

  test('at least 4 feature cards are displayed', async ({ page }) => {
    // Test case 2: Count feature cards/items
    const featureCards = page.locator('.features__card');
    const count = await featureCards.count();
    expect(count).toBeGreaterThanOrEqual(4);
  });

  test('memcached protocol feature card exists', async ({ page }) => {
    // Test case 3: Check for 'Memcached Protocol' feature
    const memcachedFeature = page.locator('[data-feature="memcached-protocol"]');
    await expect(memcachedFeature).toBeVisible();

    // Verify it mentions memcached protocol compatibility
    const featureText = await memcachedFeature.textContent();
    expect(featureText.toLowerCase()).toContain('memcached');
    expect(featureText.toLowerCase()).toContain('protocol');

    // Should mention compatibility or drop-in replacement
    const hasCompatibility = featureText.toLowerCase().includes('compatibility') ||
                            featureText.toLowerCase().includes('drop-in');
    expect(hasCompatibility).toBe(true);
  });

  test('persistent storage feature card exists', async ({ page }) => {
    // Test case 4: Check for 'Persistent Storage' feature
    const storageFeature = page.locator('[data-feature="persistent-storage"]');
    await expect(storageFeature).toBeVisible();

    // Verify it mentions LSM-tree or persistence
    const featureText = await storageFeature.textContent();
    const hasPersistence = featureText.toLowerCase().includes('lsm') ||
                          featureText.toLowerCase().includes('persistent') ||
                          featureText.toLowerCase().includes('persistence') ||
                          featureText.toLowerCase().includes('durable');
    expect(hasPersistence).toBe(true);
  });

  test('performance feature card exists', async ({ page }) => {
    // Test case 5: Check for performance feature
    const performanceFeature = page.locator('[data-feature="high-performance"]');
    await expect(performanceFeature).toBeVisible();

    // Verify it mentions skip list, memtable, or SSTable
    const featureText = await performanceFeature.textContent();
    const hasPerformanceTerms = featureText.toLowerCase().includes('skip list') ||
                                featureText.toLowerCase().includes('memtable') ||
                                featureText.toLowerCase().includes('sstable') ||
                                featureText.toLowerCase().includes('compaction');
    expect(hasPerformanceTerms).toBe(true);
  });

  test('Rust implementation feature card exists', async ({ page }) => {
    // Test case 6: Check for Rust feature
    const rustFeature = page.locator('[data-feature="rust-implementation"]');
    await expect(rustFeature).toBeVisible();

    // Verify it mentions Rust
    const featureText = await rustFeature.textContent();
    expect(featureText.toLowerCase()).toContain('rust');
  });

  test('feature cards have proper structure', async ({ page }) => {
    // Additional test: Verify feature cards have icons, titles, and descriptions
    const featureCards = page.locator('.features__card');
    const count = await featureCards.count();

    for (let i = 0; i < count; i++) {
      const card = featureCards.nth(i);

      // Check for icon
      const icon = card.locator('.features__card-icon');
      await expect(icon).toBeVisible();

      // Check for title
      const title = card.locator('.features__card-title');
      await expect(title).toBeVisible();

      // Check for description
      const description = card.locator('.features__card-description');
      await expect(description).toBeVisible();
    }
  });

  test('features section has a title', async ({ page }) => {
    // Additional test: Verify the section has a title
    const title = page.locator('.features__title');
    await expect(title).toBeVisible();

    const titleText = await title.textContent();
    expect(titleText.length).toBeGreaterThan(0);
  });
});
