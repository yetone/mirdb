// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for Features Section Content Display
 * Scenario: Verify that all key features are displayed with clear descriptions
 * as per REQ-2 (Memcached compatibility, LSM-tree storage, durability, performance)
 */

test.describe('Features Section Content Display', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to homepage before each test
    await page.goto('/');
    // Scroll to features section to ensure it's loaded
    await page.locator('#features').scrollIntoViewIfNeeded();
  });

  /**
   * Test Case 1: Check for Memcached Compatible feature
   * Input: Check for Memcached Compatible feature
   * Expected: Feature card displays 'Memcached Compatible' with description about
   * drop-in replacement supporting get, set, delete operations
   */
  test('TC1: Memcached Compatible feature card displays correct content', async ({ page }) => {
    // Verify feature card exists and is visible
    const featureCard = page.locator('[data-testid="feature-card-memcached"]');
    await expect(featureCard).toBeVisible();

    // Verify title contains 'Memcached Compatible'
    const featureTitle = page.locator('[data-testid="feature-title-memcached"]');
    await expect(featureTitle).toBeVisible();
    await expect(featureTitle).toContainText('Memcached Compatible');

    // Verify description mentions drop-in replacement and get, set, delete operations
    const featureDesc = page.locator('[data-testid="feature-desc-memcached"]');
    await expect(featureDesc).toBeVisible();
    await expect(featureDesc).toContainText('Drop-in replacement');
    await expect(featureDesc).toContainText('get');
    await expect(featureDesc).toContainText('set');
    await expect(featureDesc).toContainText('delete');
  });

  /**
   * Test Case 2: Check for Persistent Storage feature
   * Input: Check for Persistent Storage feature
   * Expected: Feature card displays 'Persistent Storage' with description about
   * LSM-tree architecture and SSTable files
   */
  test('TC2: Persistent Storage feature card displays correct content', async ({ page }) => {
    // Verify feature card exists and is visible
    const featureCard = page.locator('[data-testid="feature-card-persistent"]');
    await expect(featureCard).toBeVisible();

    // Verify title contains 'Persistent Storage'
    const featureTitle = page.locator('[data-testid="feature-title-persistent"]');
    await expect(featureTitle).toBeVisible();
    await expect(featureTitle).toContainText('Persistent Storage');

    // Verify description mentions LSM-tree architecture and SSTable files
    const featureDesc = page.locator('[data-testid="feature-desc-persistent"]');
    await expect(featureDesc).toBeVisible();
    await expect(featureDesc).toContainText('LSM-tree');
    await expect(featureDesc).toContainText('SSTable');
  });

  /**
   * Test Case 3: Check for Write-Ahead Logging feature
   * Input: Check for Write-Ahead Logging feature
   * Expected: Feature card displays 'Write-Ahead Logging' with description about
   * WAL-based crash recovery
   */
  test('TC3: Write-Ahead Logging feature card displays correct content', async ({ page }) => {
    // Verify feature card exists and is visible
    const featureCard = page.locator('[data-testid="feature-card-wal"]');
    await expect(featureCard).toBeVisible();

    // Verify title contains 'Write-Ahead Logging'
    const featureTitle = page.locator('[data-testid="feature-title-wal"]');
    await expect(featureTitle).toBeVisible();
    await expect(featureTitle).toContainText('Write-Ahead Logging');

    // Verify description mentions WAL-based crash recovery
    const featureDesc = page.locator('[data-testid="feature-desc-wal"]');
    await expect(featureDesc).toBeVisible();
    await expect(featureDesc).toContainText('WAL-based');
    await expect(featureDesc).toContainText('crash recovery');
  });

  /**
   * Test Case 4: Check for High Performance feature
   * Input: Check for High Performance feature
   * Expected: Feature card displays 'High Performance' with description about
   * skip list memtables and LRU block cache
   */
  test('TC4: High Performance feature card displays correct content', async ({ page }) => {
    // Verify feature card exists and is visible
    const featureCard = page.locator('[data-testid="feature-card-performance"]');
    await expect(featureCard).toBeVisible();

    // Verify title contains 'High Performance'
    const featureTitle = page.locator('[data-testid="feature-title-performance"]');
    await expect(featureTitle).toBeVisible();
    await expect(featureTitle).toContainText('High Performance');

    // Verify description mentions skip list memtables and LRU block cache
    const featureDesc = page.locator('[data-testid="feature-desc-performance"]');
    await expect(featureDesc).toBeVisible();
    await expect(featureDesc).toContainText('Skip list');
    await expect(featureDesc).toContainText('memtables');
    await expect(featureDesc).toContainText('LRU block cache');
  });

  /**
   * Test Case 5: Check for Efficient Compression feature
   * Input: Check for Efficient Compression feature
   * Expected: Feature card displays 'Efficient Compression' with description about
   * Snappy compression
   */
  test('TC5: Efficient Compression feature card displays correct content', async ({ page }) => {
    // Verify feature card exists and is visible
    const featureCard = page.locator('[data-testid="feature-card-compression"]');
    await expect(featureCard).toBeVisible();

    // Verify title contains 'Efficient Compression'
    const featureTitle = page.locator('[data-testid="feature-title-compression"]');
    await expect(featureTitle).toBeVisible();
    await expect(featureTitle).toContainText('Efficient Compression');

    // Verify description mentions Snappy compression
    const featureDesc = page.locator('[data-testid="feature-desc-compression"]');
    await expect(featureDesc).toBeVisible();
    await expect(featureDesc).toContainText('Snappy compression');
  });

  /**
   * Test Case 6: Check for Smart Filtering feature
   * Input: Check for Smart Filtering feature
   * Expected: Feature card displays 'Smart Filtering' with description about
   * Cuckoo filters minimizing disk reads
   */
  test('TC6: Smart Filtering feature card displays correct content', async ({ page }) => {
    // Verify feature card exists and is visible
    const featureCard = page.locator('[data-testid="feature-card-filtering"]');
    await expect(featureCard).toBeVisible();

    // Verify title contains 'Smart Filtering'
    const featureTitle = page.locator('[data-testid="feature-title-filtering"]');
    await expect(featureTitle).toBeVisible();
    await expect(featureTitle).toContainText('Smart Filtering');

    // Verify description mentions Cuckoo filters and minimizing disk reads
    const featureDesc = page.locator('[data-testid="feature-desc-filtering"]');
    await expect(featureDesc).toBeVisible();
    await expect(featureDesc).toContainText('Cuckoo filters');
    await expect(featureDesc).toContainText('disk reads');
  });

  /**
   * Test: Verify features section displays 6 feature cards (step 2)
   */
  test('Features section displays exactly 6 feature cards', async ({ page }) => {
    // Verify feature grid exists
    const featureGrid = page.locator('[data-testid="feature-grid"]');
    await expect(featureGrid).toBeVisible();

    // Count all feature cards
    const featureCards = page.locator('.feature-card');
    await expect(featureCards).toHaveCount(6);
  });

  /**
   * Test: Verify each feature card has title, description structure
   */
  test('Each feature card has title and description elements', async ({ page }) => {
    const featureCards = page.locator('.feature-card');
    const count = await featureCards.count();

    expect(count).toBe(6);

    // Check each card has a title and description
    for (let i = 0; i < count; i++) {
      const card = featureCards.nth(i);
      await expect(card).toBeVisible();

      // Each card should have an h3 title
      const title = card.locator('.feature-card-title');
      await expect(title).toBeVisible();

      // Each card should have a description paragraph
      const description = card.locator('.feature-card-description');
      await expect(description).toBeVisible();
    }
  });
});
