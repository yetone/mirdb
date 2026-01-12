// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = 'file://' + path.resolve(__dirname, '../index.html');

test.describe('Features Section Display - Scenario 2', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(indexPath);
  });

  // Test Case 1: Count feature cards in features section
  test('TC1: should display exactly 5 feature cards', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Count feature cards
    const featureCards = page.locator('.feature-card');
    await expect(featureCards).toHaveCount(5);
  });

  // Test Case 2: Check Memcached Protocol Compatibility card
  test('TC2: should display Memcached Protocol Compatibility card with icon, title and description', async ({ page }) => {
    const card = page.locator('.feature-card[data-feature="memcached-protocol"]');
    await expect(card).toBeVisible();

    // Verify icon is present
    const icon = card.locator('.icon');
    await expect(icon).toBeVisible();
    const svg = icon.locator('svg');
    await expect(svg).toBeVisible();

    // Verify title
    const title = card.locator('h3');
    await expect(title).toHaveText('Memcached Protocol Compatibility');

    // Verify description mentions standard protocol support
    const description = card.locator('p');
    await expect(description).toBeVisible();
    const descText = await description.textContent();
    expect(descText).toMatch(/standard.*memcached.*protocol|memcached.*protocol/i);
  });

  // Test Case 3: Check Persistent Storage card
  test('TC3: should display Persistent Storage card with icon, title and description', async ({ page }) => {
    const card = page.locator('.feature-card[data-feature="persistent-storage"]');
    await expect(card).toBeVisible();

    // Verify icon is present
    const icon = card.locator('.icon');
    await expect(icon).toBeVisible();
    const svg = icon.locator('svg');
    await expect(svg).toBeVisible();

    // Verify title references persistence/SSTables
    const title = card.locator('h3');
    const titleText = await title.textContent();
    expect(titleText).toMatch(/persistent|sstable/i);

    // Verify description mentions data durability
    const description = card.locator('p');
    await expect(description).toBeVisible();
    const descText = await description.textContent();
    expect(descText).toMatch(/durability|persist|disk/i);
  });

  // Test Case 4: Check LSM Tree Architecture card
  test('TC4: should display LSM Tree Architecture card with icon, title and description', async ({ page }) => {
    const card = page.locator('.feature-card[data-feature="lsm-tree"]');
    await expect(card).toBeVisible();

    // Verify icon is present
    const icon = card.locator('.icon');
    await expect(icon).toBeVisible();
    const svg = icon.locator('svg');
    await expect(svg).toBeVisible();

    // Verify title
    const title = card.locator('h3');
    await expect(title).toHaveText('LSM Tree Architecture');

    // Verify description mentions data structure benefits
    const description = card.locator('p');
    await expect(description).toBeVisible();
    const descText = await description.textContent();
    expect(descText).toMatch(/log-structured|write.*performance|merge|compaction/i);
  });

  // Test Case 5: Check Configurable Compaction card
  test('TC5: should display Configurable Compaction card with icon, title and description', async ({ page }) => {
    const card = page.locator('.feature-card[data-feature="configurable-compaction"]');
    await expect(card).toBeVisible();

    // Verify icon is present
    const icon = card.locator('.icon');
    await expect(icon).toBeVisible();
    const svg = icon.locator('svg');
    await expect(svg).toBeVisible();

    // Verify title mentions compaction
    const title = card.locator('h3');
    const titleText = await title.textContent();
    expect(titleText).toMatch(/compaction/i);

    // Verify description mentions compaction strategy
    const description = card.locator('p');
    await expect(description).toBeVisible();
    const descText = await description.textContent();
    expect(descText).toMatch(/compaction.*strategy|configure|threshold|merging/i);
  });

  // Test Case 6: Check Async Networking card
  test('TC6: should display Async Networking card with icon, title and description', async ({ page }) => {
    const card = page.locator('.feature-card[data-feature="async-networking"]');
    await expect(card).toBeVisible();

    // Verify icon is present
    const icon = card.locator('.icon');
    await expect(icon).toBeVisible();
    const svg = icon.locator('svg');
    await expect(svg).toBeVisible();

    // Verify title mentions Tokio/async
    const title = card.locator('h3');
    const titleText = await title.textContent();
    expect(titleText).toMatch(/tokio|async/i);

    // Verify description mentions networking capabilities
    const description = card.locator('p');
    await expect(description).toBeVisible();
    const descText = await description.textContent();
    expect(descText).toMatch(/async|non-blocking|concurrent|connection/i);
  });

  // Additional test: Verify each card has consistent structure
  test('should have consistent card layout across all features', async ({ page }) => {
    const featureCards = page.locator('.feature-card');
    const count = await featureCards.count();

    for (let i = 0; i < count; i++) {
      const card = featureCards.nth(i);

      // Each card should have an icon
      const icon = card.locator('.icon');
      await expect(icon).toBeVisible();

      // Each card should have a title (h3)
      const title = card.locator('h3');
      await expect(title).toBeVisible();
      const titleText = await title.textContent();
      expect(titleText?.length).toBeGreaterThan(0);

      // Each card should have a description (p)
      const description = card.locator('p');
      await expect(description).toBeVisible();
      const descText = await description.textContent();
      expect(descText?.length).toBeGreaterThan(0);
    }
  });
});
