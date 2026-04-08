// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Features Grid E2E Tests
 * Owner: Scenario 2 - Features Grid Display
 *
 * Test cases:
 * - All 6 features are displayed (Memcached, SSTable, LSM, Rust, Tokio, Compaction)
 * - Features have icons
 * - Grid layout is responsive (3-col desktop, 2-col tablet, 1-col mobile)
 */

test.describe('Features Grid Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Memcached protocol feature card is displayed with correct content', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Find the Memcached feature card
    const memcachedCard = page.locator('[data-feature="memcached"]');
    await expect(memcachedCard).toBeVisible();

    // Verify title contains Memcached protocol
    const title = memcachedCard.locator('.features-card-title');
    await expect(title).toContainText('Memcached Protocol');

    // Verify description mentions protocol compatibility and drop-in replacement
    const description = memcachedCard.locator('.features-card-description');
    await expect(description).toContainText('Memcached protocol compatibility');
    await expect(description).toContainText('drop-in replacement');

    // Verify icon is present
    const icon = memcachedCard.locator('.features-card-icon');
    await expect(icon).toBeVisible();
  });

  test('TC2: Persistence feature card displays SSTable information', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    const persistenceCard = page.locator('[data-feature="persistence"]');
    await expect(persistenceCard).toBeVisible();

    const title = persistenceCard.locator('.features-card-title');
    await expect(title).toContainText('Persistent Storage');

    const description = persistenceCard.locator('.features-card-description');
    await expect(description).toContainText('Persistent storage using SSTables');

    const icon = persistenceCard.locator('.features-card-icon');
    await expect(icon).toBeVisible();
  });

  test('TC3: LSM tree feature card displays architecture information', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    const lsmCard = page.locator('[data-feature="lsm-tree"]');
    await expect(lsmCard).toBeVisible();

    const title = lsmCard.locator('.features-card-title');
    await expect(title).toContainText('LSM Tree');

    const description = lsmCard.locator('.features-card-description');
    await expect(description).toContainText('LSM tree architecture');
    await expect(description).toContainText('performance');

    const icon = lsmCard.locator('.features-card-icon');
    await expect(icon).toBeVisible();
  });

  test('TC4: Rust feature card displays implementation information', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    const rustCard = page.locator('[data-feature="rust"]');
    await expect(rustCard).toBeVisible();

    const title = rustCard.locator('.features-card-title');
    await expect(title).toContainText('Rust');

    const description = rustCard.locator('.features-card-description');
    await expect(description).toContainText('Rust implementation');
    await expect(description).toContainText('safety');
    await expect(description).toContainText('speed');

    const icon = rustCard.locator('.features-card-icon');
    await expect(icon).toBeVisible();
  });

  test('TC5: Tokio feature card displays async networking information', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    const tokioCard = page.locator('[data-feature="tokio"]');
    await expect(tokioCard).toBeVisible();

    const title = tokioCard.locator('.features-card-title');
    await expect(title).toContainText('Async');

    const description = tokioCard.locator('.features-card-description');
    await expect(description).toContainText('Async networking with Tokio');

    const icon = tokioCard.locator('.features-card-icon');
    await expect(icon).toBeVisible();
  });

  test('TC6: Compaction feature card displays multi-level compaction information', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    const compactionCard = page.locator('[data-feature="compaction"]');
    await expect(compactionCard).toBeVisible();

    const title = compactionCard.locator('.features-card-title');
    await expect(title).toContainText('Multi-level Compaction');

    const description = compactionCard.locator('.features-card-description');
    await expect(description).toContainText('Multi-level compaction');

    const icon = compactionCard.locator('.features-card-icon');
    await expect(icon).toBeVisible();
  });

  test('TC7: Features grid displays in responsive layout with icons', async ({ page }) => {
    // Desktop: 3 columns
    await page.setViewportSize({ width: 1200, height: 800 });
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    const grid = page.locator('.features-grid');
    await expect(grid).toBeVisible();

    // Verify all 6 feature cards are present
    const cards = page.locator('.features-card');
    await expect(cards).toHaveCount(6);

    // Verify grid layout at desktop (3 columns)
    const gridStyles = await grid.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      return {
        display: computed.display,
        gridTemplateColumns: computed.gridTemplateColumns,
      };
    });
    expect(gridStyles.display).toBe('grid');
    // At 1200px, should have 3 columns (check for 3 values in grid-template-columns)
    const columnCount = gridStyles.gridTemplateColumns.split(' ').length;
    expect(columnCount).toBe(3);

    // Verify all cards have icons
    for (let i = 0; i < 6; i++) {
      const cardIcon = cards.nth(i).locator('.features-card-icon');
      await expect(cardIcon).toBeVisible();
      const svg = cardIcon.locator('svg');
      await expect(svg).toBeVisible();
    }
  });

  test('TC7b: Features grid displays 2 columns on tablet', async ({ page }) => {
    // Tablet: 2 columns
    await page.setViewportSize({ width: 800, height: 1024 });

    const grid = page.locator('.features-grid');
    await expect(grid).toBeVisible();

    const gridStyles = await grid.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      return {
        gridTemplateColumns: computed.gridTemplateColumns,
      };
    });

    const columnCount = gridStyles.gridTemplateColumns.split(' ').length;
    expect(columnCount).toBe(2);
  });

  test('TC7c: Features grid displays 1 column on mobile', async ({ page }) => {
    // Mobile: 1 column
    await page.setViewportSize({ width: 375, height: 667 });

    const grid = page.locator('.features-grid');
    await expect(grid).toBeVisible();

    const gridStyles = await grid.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      return {
        gridTemplateColumns: computed.gridTemplateColumns,
      };
    });

    const columnCount = gridStyles.gridTemplateColumns.split(' ').length;
    expect(columnCount).toBe(1);
  });

  test('Features section has proper accessibility attributes', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toHaveAttribute('aria-labelledby', 'features-title');

    const title = page.locator('#features-title');
    await expect(title).toBeVisible();
    await expect(title).toContainText('Key Features');

    // Verify grid has list role
    const grid = page.locator('.features-grid');
    await expect(grid).toHaveAttribute('role', 'list');

    // Verify cards have listitem role
    const cards = page.locator('.features-card');
    const count = await cards.count();
    for (let i = 0; i < count; i++) {
      await expect(cards.nth(i)).toHaveAttribute('role', 'listitem');
    }
  });
});
