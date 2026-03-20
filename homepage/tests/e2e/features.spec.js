/**
 * Features Section E2E Tests
 * Owner: Scenario 2 - Features Section Display
 *
 * Test cases:
 * - 4-6 feature cards displayed
 * - 3-column layout on desktop
 * - Each core feature represented (Memcached, SSTables, LSM Tree, skip list, compaction)
 * - Feature cards have icons and descriptions
 */

const { test, expect } = require('@playwright/test');

test.describe('Features Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Navigate to features section
    await page.locator('#features').scrollIntoViewIfNeeded();
  });

  test('should display 4-6 feature cards in 3-column layout', async ({ page }) => {
    // Get the features grid
    const featuresGrid = page.locator('[data-testid="features-grid"]');
    await expect(featuresGrid).toBeVisible();

    // Count feature cards
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();

    // Verify 4-6 cards are displayed
    expect(cardCount).toBeGreaterThanOrEqual(4);
    expect(cardCount).toBeLessThanOrEqual(6);

    // Verify 3-column layout on desktop (check grid-template-columns)
    const gridStyle = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).gridTemplateColumns;
    });

    // Should have 3 columns (the computed style will show actual pixel values)
    const columns = gridStyle.split(' ').length;
    expect(columns).toBe(3);
  });

  test('should display Memcached protocol feature card', async ({ page }) => {
    // Find the Memcached feature card
    const memcachedCard = page.locator('[data-testid="feature-memcached"]');
    await expect(memcachedCard).toBeVisible();

    // Verify it has a title mentioning Memcached Protocol
    const title = memcachedCard.locator('.feature-card__title');
    await expect(title).toContainText('Memcached Protocol');

    // Verify it has a description about text protocol compatibility
    const description = memcachedCard.locator('.feature-card__description');
    await expect(description).toContainText('compatibility');
    await expect(description).toContainText('protocol');

    // Verify it has an icon
    const icon = memcachedCard.locator('.feature-card__icon');
    await expect(icon).toBeVisible();
    const svg = icon.locator('svg');
    await expect(svg).toBeVisible();
  });

  test('should display persistent storage feature card with SSTables', async ({ page }) => {
    // Find the SSTables feature card
    const sstablesCard = page.locator('[data-testid="feature-sstables"]');
    await expect(sstablesCard).toBeVisible();

    // Verify it has a title about persistent storage
    const title = sstablesCard.locator('.feature-card__title');
    await expect(title).toContainText('Persistent Storage');

    // Verify it has a description mentioning SSTables
    const description = sstablesCard.locator('.feature-card__description');
    await expect(description).toContainText('SSTables');
    await expect(description).toContainText('persist');

    // Verify it has an icon
    const icon = sstablesCard.locator('.feature-card__icon');
    await expect(icon).toBeVisible();
  });

  test('should display LSM Tree architecture feature card', async ({ page }) => {
    // Find the LSM Tree feature card
    const lsmCard = page.locator('[data-testid="feature-lsm-tree"]');
    await expect(lsmCard).toBeVisible();

    // Verify it has a title mentioning LSM Tree
    const title = lsmCard.locator('.feature-card__title');
    await expect(title).toContainText('LSM Tree');

    // Verify it has a description about LSM architecture
    const description = lsmCard.locator('.feature-card__description');
    await expect(description).toContainText('Log-Structured Merge Tree');

    // Verify it has an icon
    const icon = lsmCard.locator('.feature-card__icon');
    await expect(icon).toBeVisible();
  });

  test('should display skip list Memtable feature card', async ({ page }) => {
    // Find the skip list feature card
    const skiplistCard = page.locator('[data-testid="feature-skiplist"]');
    await expect(skiplistCard).toBeVisible();

    // Verify it has a title mentioning Skip List or Memtable
    const title = skiplistCard.locator('.feature-card__title');
    await expect(title).toContainText('Skip List');

    // Verify it has a description mentioning Memtable and skip list implementation
    const description = skiplistCard.locator('.feature-card__description');
    await expect(description).toContainText('Memtable');
    await expect(description).toContainText('skip list');

    // Verify it has an icon
    const icon = skiplistCard.locator('.feature-card__icon');
    await expect(icon).toBeVisible();
  });

  test('should display multi-level compaction feature card', async ({ page }) => {
    // Find the compaction feature card
    const compactionCard = page.locator('[data-testid="feature-compaction"]');
    await expect(compactionCard).toBeVisible();

    // Verify it has a title mentioning compaction
    const title = compactionCard.locator('.feature-card__title');
    await expect(title).toContainText('Compaction');

    // Verify it has a description about multi-level SSTable compaction
    const description = compactionCard.locator('.feature-card__description');
    await expect(description).toContainText('SSTable');
    await expect(description).toContainText('compaction');
    await expect(description).toContainText('level');

    // Verify it has an icon
    const icon = compactionCard.locator('.feature-card__icon');
    await expect(icon).toBeVisible();
  });

  test('all feature cards should have icons and descriptions', async ({ page }) => {
    // Get all feature cards
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();

    // Check each card has required elements
    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i);

      // Each card should have an icon
      const icon = card.locator('.feature-card__icon');
      await expect(icon).toBeVisible();

      // Each card should have a title
      const title = card.locator('.feature-card__title');
      await expect(title).toBeVisible();
      const titleText = await title.textContent();
      expect(titleText.length).toBeGreaterThan(0);

      // Each card should have a description
      const description = card.locator('.feature-card__description');
      await expect(description).toBeVisible();
      const descText = await description.textContent();
      expect(descText.length).toBeGreaterThan(20); // Meaningful description
    }
  });

  test('features section should have headline and intro text', async ({ page }) => {
    // Check for section headline
    const headline = page.locator('#features-headline');
    await expect(headline).toBeVisible();
    await expect(headline).toContainText('Core Features');

    // Check for intro description
    const description = page.locator('.features__description');
    await expect(description).toBeVisible();
  });
});
