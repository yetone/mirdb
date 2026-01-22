// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Key Features Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Memcached Protocol feature card displays with drop-in replacement description', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();
    await expect(featuresSection).toBeVisible();

    // Find the Memcached Protocol feature card
    const memcachedCard = page.locator('[data-feature="memcached-protocol"]');
    await expect(memcachedCard).toBeVisible();

    // Verify title
    const title = memcachedCard.locator('.feature-title');
    await expect(title).toHaveText('Memcached Protocol');

    // Verify description mentions drop-in replacement
    const description = memcachedCard.locator('.feature-description');
    await expect(description).toContainText('Drop-in replacement');
    await expect(description).toContainText('familiar API');
  });

  test('TC2: Persistent Storage feature card displays with LSM-tree durability description', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();
    await expect(featuresSection).toBeVisible();

    // Find the Persistent Storage feature card
    const persistentStorageCard = page.locator('[data-feature="persistent-storage"]');
    await expect(persistentStorageCard).toBeVisible();

    // Verify title
    const title = persistentStorageCard.locator('.feature-title');
    await expect(title).toHaveText('Persistent Storage');

    // Verify description mentions LSM-tree and durability
    const description = persistentStorageCard.locator('.feature-description');
    await expect(description).toContainText('LSM-tree');
    await expect(description).toContainText('durability');
  });

  test('TC3: High Performance feature card displays with skip list memtables description', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();
    await expect(featuresSection).toBeVisible();

    // Find the High Performance feature card
    const highPerformanceCard = page.locator('[data-feature="high-performance"]');
    await expect(highPerformanceCard).toBeVisible();

    // Verify title
    const title = highPerformanceCard.locator('.feature-title');
    await expect(title).toHaveText('High Performance');

    // Verify description mentions skip list memtables
    const description = highPerformanceCard.locator('.feature-description');
    await expect(description).toContainText('Skip list memtables');
  });

  test('TC4: Automatic Compaction feature card displays with minor and major compaction support', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();
    await expect(featuresSection).toBeVisible();

    // Find the Automatic Compaction feature card
    const compactionCard = page.locator('[data-feature="automatic-compaction"]');
    await expect(compactionCard).toBeVisible();

    // Verify title
    const title = compactionCard.locator('.feature-title');
    await expect(title).toHaveText('Automatic Compaction');

    // Verify description mentions minor and major compaction
    const description = compactionCard.locator('.feature-description');
    await expect(description).toContainText('Minor and major compaction');
  });

  test('TC5: Configurable feature card displays with TOML-based configuration description', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();
    await expect(featuresSection).toBeVisible();

    // Find the Configurable feature card
    const configurableCard = page.locator('[data-feature="configurable"]');
    await expect(configurableCard).toBeVisible();

    // Verify title
    const title = configurableCard.locator('.feature-title');
    await expect(title).toHaveText('Configurable');

    // Verify description mentions TOML-based configuration
    const description = configurableCard.locator('.feature-description');
    await expect(description).toContainText('TOML-based configuration');
  });

  test('TC6: Async I/O feature card displays with Tokio networking description', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();
    await expect(featuresSection).toBeVisible();

    // Find the Async I/O feature card
    const asyncIoCard = page.locator('[data-feature="async-io"]');
    await expect(asyncIoCard).toBeVisible();

    // Verify title
    const title = asyncIoCard.locator('.feature-title');
    await expect(title).toHaveText('Async I/O');

    // Verify description mentions Tokio and networking
    const description = asyncIoCard.locator('.feature-description');
    await expect(description).toContainText('Tokio');
    await expect(description).toContainText('networking');
  });

  test('All six feature cards are visible in the features grid', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();
    await expect(featuresSection).toBeVisible();

    // Check all feature cards are present
    const featureCards = page.locator('.feature-card');
    await expect(featureCards).toHaveCount(6);

    // Verify each feature card is visible
    const expectedFeatures = [
      'memcached-protocol',
      'persistent-storage',
      'high-performance',
      'automatic-compaction',
      'configurable',
      'async-io'
    ];

    for (const feature of expectedFeatures) {
      const card = page.locator(`[data-feature="${feature}"]`);
      await expect(card).toBeVisible();
    }
  });
});
