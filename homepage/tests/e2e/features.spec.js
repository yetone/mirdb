/**
 * Feature Showcase E2E Tests
 * Owner: Scenario 2 - Feature Showcase Implementation
 *
 * Tests for the features section including:
 * - Section visibility and heading
 * - Feature card count and content
 * - Responsive grid layout
 * - Icon rendering
 */
const { test, expect } = require('@playwright/test');

test.describe('Features Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test case 1: Navigate to #features section
  test('features section is visible with section heading', async ({ page }) => {
    // Navigate to features section
    await page.goto('/#features');

    // Check section exists and is visible
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Check section heading exists
    const heading = page.locator('#features-heading');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText('Key Features');
  });

  // Test case 2: Count feature cards
  test('exactly 6 feature cards are displayed', async ({ page }) => {
    const featureCards = page.locator('.feature-card');
    await expect(featureCards).toHaveCount(6);
  });

  // Test case 3: Verify Memcached Protocol feature
  test('Memcached Protocol feature card exists with correct content', async ({ page }) => {
    const memcachedCard = page.locator('.feature-card').filter({
      has: page.locator('.feature-card__title', { hasText: 'Memcached Protocol' })
    });

    await expect(memcachedCard).toBeVisible();

    const description = memcachedCard.locator('.feature-card__description');
    await expect(description).toContainText('compatibility');
  });

  // Test case 4: Verify Persistent Storage feature
  test('Persistent Storage feature card exists with correct content', async ({ page }) => {
    const storageCard = page.locator('.feature-card').filter({
      has: page.locator('.feature-card__title', { hasText: 'Persistent Storage' })
    });

    await expect(storageCard).toBeVisible();

    const description = storageCard.locator('.feature-card__description');
    await expect(description).toContainText('LSM tree');
  });

  // Test case 5: Verify High Performance feature
  test('High Performance feature card exists with correct content', async ({ page }) => {
    const performanceCard = page.locator('.feature-card').filter({
      has: page.locator('.feature-card__title', { hasText: 'High Performance' })
    });

    await expect(performanceCard).toBeVisible();

    const description = performanceCard.locator('.feature-card__description');
    await expect(description).toContainText('Skip-list memtable');
  });

  // Test case 6: Verify Crash Recovery feature
  test('Crash Recovery feature card exists with correct content', async ({ page }) => {
    const recoveryCard = page.locator('.feature-card').filter({
      has: page.locator('.feature-card__title', { hasText: 'Crash Recovery' })
    });

    await expect(recoveryCard).toBeVisible();

    const description = recoveryCard.locator('.feature-card__description');
    await expect(description).toContainText('Write-Ahead Log');
  });

  // Test case 7: Verify Efficient Compaction feature
  test('Efficient Compaction feature card exists with correct content', async ({ page }) => {
    const compactionCard = page.locator('.feature-card').filter({
      has: page.locator('.feature-card__title', { hasText: 'Efficient Compaction' })
    });

    await expect(compactionCard).toBeVisible();

    const description = compactionCard.locator('.feature-card__description');
    await expect(description).toContainText('Minor and major compaction');
  });

  // Test case 8: Verify feature icons load
  test('all 6 feature cards have visible icons', async ({ page }) => {
    const featureCards = page.locator('.feature-card');
    const count = await featureCards.count();

    expect(count).toBe(6);

    for (let i = 0; i < count; i++) {
      const card = featureCards.nth(i);
      const icon = card.locator('.feature-card__icon svg');
      await expect(icon).toBeVisible();
    }
  });

  // Test case 9: Test feature grid on 1024px viewport (3 columns)
  test('features display in 3-column grid layout on desktop', async ({ page }) => {
    await page.setViewportSize({ width: 1024, height: 768 });

    const grid = page.locator('.features__grid');
    await expect(grid).toBeVisible();

    // Check computed grid-template-columns for 3 columns
    const gridStyle = await grid.evaluate((el) => {
      return window.getComputedStyle(el).getPropertyValue('grid-template-columns');
    });

    // Should have 3 columns (3 values separated by spaces)
    const columns = gridStyle.split(' ').filter(col => col.length > 0);
    expect(columns.length).toBe(3);
  });

  // Test case 10: Test feature grid on 768px viewport (2 columns)
  test('features display in 2-column grid layout on tablet', async ({ page }) => {
    await page.setViewportSize({ width: 768, height: 1024 });

    const grid = page.locator('.features__grid');
    await expect(grid).toBeVisible();

    // Check computed grid-template-columns for 2 columns
    const gridStyle = await grid.evaluate((el) => {
      return window.getComputedStyle(el).getPropertyValue('grid-template-columns');
    });

    // Should have 2 columns (2 values separated by spaces)
    const columns = gridStyle.split(' ').filter(col => col.length > 0);
    expect(columns.length).toBe(2);
  });

  // Test case 11: Test feature grid on 320px viewport (1 column)
  test('features display in single-column layout on mobile', async ({ page }) => {
    await page.setViewportSize({ width: 320, height: 568 });

    const grid = page.locator('.features__grid');
    await expect(grid).toBeVisible();

    // Check computed grid-template-columns for 1 column
    const gridStyle = await grid.evaluate((el) => {
      return window.getComputedStyle(el).getPropertyValue('grid-template-columns');
    });

    // Should have 1 column (1 value)
    const columns = gridStyle.split(' ').filter(col => col.length > 0);
    expect(columns.length).toBe(1);
  });
});
