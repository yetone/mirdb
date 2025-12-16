import { test, expect } from '@playwright/test';

test.describe('Features Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Memcached Protocol Compatibility feature is displayed with description mentioning drop-in compatibility', async ({ page }) => {
    const featuresSection = page.locator('.features-section');
    await expect(featuresSection).toBeVisible();

    const memcachedFeature = featuresSection.locator('.feature-card[data-feature="memcached"]');
    await expect(memcachedFeature).toBeVisible();

    const featureTitle = memcachedFeature.locator('.feature-title');
    await expect(featureTitle).toContainText('Memcached Protocol Compatibility');

    const featureDescription = memcachedFeature.locator('.feature-description');
    await expect(featureDescription).toContainText('Drop-in compatibility');
  });

  test('TC2: LSM-Tree Storage Architecture feature is displayed with description about efficient storage', async ({ page }) => {
    const featuresSection = page.locator('.features-section');
    await expect(featuresSection).toBeVisible();

    const lsmTreeFeature = featuresSection.locator('.feature-card[data-feature="lsm-tree"]');
    await expect(lsmTreeFeature).toBeVisible();

    const featureTitle = lsmTreeFeature.locator('.feature-title');
    await expect(featureTitle).toContainText('LSM-Tree Storage Architecture');

    const featureDescription = lsmTreeFeature.locator('.feature-description');
    await expect(featureDescription).toContainText('Efficient storage');
  });

  test('TC3: Write-Ahead Logging (WAL) feature is displayed with description about data durability', async ({ page }) => {
    const featuresSection = page.locator('.features-section');
    await expect(featuresSection).toBeVisible();

    const walFeature = featuresSection.locator('.feature-card[data-feature="wal"]');
    await expect(walFeature).toBeVisible();

    const featureTitle = walFeature.locator('.feature-title');
    await expect(featureTitle).toContainText('Write-Ahead Logging (WAL)');

    const featureDescription = walFeature.locator('.feature-description');
    await expect(featureDescription).toContainText('Data durability');
  });

  test('TC4: Compaction feature mentions Minor & Major Compaction and space reclamation', async ({ page }) => {
    const featuresSection = page.locator('.features-section');
    await expect(featuresSection).toBeVisible();

    const compactionFeature = featuresSection.locator('.feature-card[data-feature="compaction"]');
    await expect(compactionFeature).toBeVisible();

    const featureTitle = compactionFeature.locator('.feature-title');
    await expect(featureTitle).toContainText('Minor');
    await expect(featureTitle).toContainText('Major Compaction');

    const featureDescription = compactionFeature.locator('.feature-description');
    await expect(featureDescription).toContainText('space reclamation');
  });

  test('TC5: LRU Block Cache feature is displayed with description about fast reads and caching', async ({ page }) => {
    const featuresSection = page.locator('.features-section');
    await expect(featuresSection).toBeVisible();

    const lruCacheFeature = featuresSection.locator('.feature-card[data-feature="lru-cache"]');
    await expect(lruCacheFeature).toBeVisible();

    const featureTitle = lruCacheFeature.locator('.feature-title');
    await expect(featureTitle).toContainText('LRU Block Cache');

    const featureDescription = lruCacheFeature.locator('.feature-description');
    await expect(featureDescription).toContainText('Fast reads');
    await expect(featureDescription).toContainText('caching');
  });

  test('TC6: Cuckoo Filter feature is displayed with description about quick key existence checks', async ({ page }) => {
    const featuresSection = page.locator('.features-section');
    await expect(featuresSection).toBeVisible();

    const cuckooFilterFeature = featuresSection.locator('.feature-card[data-feature="cuckoo-filter"]');
    await expect(cuckooFilterFeature).toBeVisible();

    const featureTitle = cuckooFilterFeature.locator('.feature-title');
    await expect(cuckooFilterFeature.locator('.feature-title')).toContainText('Cuckoo Filter');

    const featureDescription = cuckooFilterFeature.locator('.feature-description');
    await expect(featureDescription).toContainText('Quick key existence checks');
  });

  test('TC7: Snappy Compression feature is displayed with description about reduced storage footprint', async ({ page }) => {
    const featuresSection = page.locator('.features-section');
    await expect(featuresSection).toBeVisible();

    const snappyFeature = featuresSection.locator('.feature-card[data-feature="snappy"]');
    await expect(snappyFeature).toBeVisible();

    const featureTitle = snappyFeature.locator('.feature-title');
    await expect(featureTitle).toContainText('Snappy Compression');

    const featureDescription = snappyFeature.locator('.feature-description');
    await expect(featureDescription).toContainText('Reduced storage footprint');
  });

  test('TC8: At least 7 features are visible in the features section', async ({ page }) => {
    const featuresSection = page.locator('.features-section');
    await expect(featuresSection).toBeVisible();

    const featureCards = featuresSection.locator('.feature-card');
    const featureCount = await featureCards.count();

    expect(featureCount).toBeGreaterThanOrEqual(7);
  });
});
