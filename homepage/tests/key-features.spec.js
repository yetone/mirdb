// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Key Features Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Memcached Compatible feature
   * Verify that the features section contains 'Memcached Compatible' with description
   * about drop-in replacement and standard protocol
   */
  test('should display Memcached Compatible feature with proper description', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toBeVisible();

    // Scroll to features section to ensure it's in view
    await featuresSection.scrollIntoViewIfNeeded();

    // Find the Memcached Compatible feature card
    const featureCards = featuresSection.locator('.feature-card');
    const memcachedCard = featureCards.filter({ hasText: /Memcached Compatible/i });

    await expect(memcachedCard).toBeVisible();

    // Verify description mentions drop-in replacement and standard protocol
    const cardText = await memcachedCard.textContent();
    expect(cardText?.toLowerCase()).toContain('drop-in replacement');
    expect(cardText?.toLowerCase()).toContain('protocol');
  });

  /**
   * Test Case 2: Persistent Storage feature
   * Verify that the features section contains 'Persistent Storage' with mention
   * of LSM-tree and SSTable
   */
  test('should display Persistent Storage feature with LSM-tree and SSTable', async ({ page }) => {
    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toBeVisible();

    // Find the Persistent Storage feature card
    const featureCards = featuresSection.locator('.feature-card');
    const persistentCard = featureCards.filter({ hasText: /Persistent Storage/i });

    await expect(persistentCard).toBeVisible();

    // Verify description mentions LSM-tree and SSTable
    const cardText = await persistentCard.textContent();
    expect(cardText?.toLowerCase()).toContain('lsm-tree');
    expect(cardText?.toLowerCase()).toContain('sstable');
  });

  /**
   * Test Case 3: Performance feature
   * Verify that the features section mentions skip list memtables, LRU block cache,
   * or Cuckoo filters
   */
  test('should display High Performance feature with skip list, LRU cache, or Cuckoo filters', async ({ page }) => {
    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toBeVisible();

    // Find the High Performance feature card
    const featureCards = featuresSection.locator('.feature-card');
    const performanceCard = featureCards.filter({ hasText: /High Performance/i });

    await expect(performanceCard).toBeVisible();

    // Verify description mentions at least one of: skip list, LRU cache, or Cuckoo filter
    const cardText = await performanceCard.textContent();
    const hasSkipList = cardText?.toLowerCase().includes('skip list');
    const hasLRUCache = cardText?.toLowerCase().includes('lru');
    const hasCuckooFilter = cardText?.toLowerCase().includes('cuckoo');

    expect(hasSkipList || hasLRUCache || hasCuckooFilter).toBeTruthy();
  });

  /**
   * Test Case 4: Durability feature
   * Verify that the features section mentions write-ahead logging and crash recovery
   */
  test('should display Durable feature with write-ahead logging and crash recovery', async ({ page }) => {
    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toBeVisible();

    // Find the Durable feature card
    const featureCards = featuresSection.locator('.feature-card');
    const durableCard = featureCards.filter({ hasText: /Durable/i });

    await expect(durableCard).toBeVisible();

    // Verify description mentions write-ahead logging and crash recovery
    const cardText = await durableCard.textContent();
    expect(cardText?.toLowerCase()).toContain('write-ahead logging');
    expect(cardText?.toLowerCase()).toContain('crash recovery');
  });

  /**
   * Test Case 5: Efficiency feature
   * Verify that the features section mentions Snappy compression and automatic compaction
   */
  test('should display Efficient feature with Snappy compression and automatic compaction', async ({ page }) => {
    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toBeVisible();

    // Find the Efficient feature card
    const featureCards = featuresSection.locator('.feature-card');
    const efficientCard = featureCards.filter({ hasText: /Efficient/i });

    await expect(efficientCard).toBeVisible();

    // Verify description mentions Snappy compression and automatic compaction
    const cardText = await efficientCard.textContent();
    expect(cardText?.toLowerCase()).toContain('snappy compression');
    expect(cardText?.toLowerCase()).toContain('compaction');
  });

  /**
   * Additional test: All six feature cards are present
   */
  test('should display all six feature cards', async ({ page }) => {
    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toBeVisible();

    const featureCards = featuresSection.locator('.feature-card');
    await expect(featureCards).toHaveCount(6);
  });

  /**
   * Additional test: Features section has proper heading
   */
  test('should display Key Features heading', async ({ page }) => {
    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toBeVisible();

    const heading = featuresSection.locator('h2');
    await expect(heading).toHaveText('Key Features');
  });
});
