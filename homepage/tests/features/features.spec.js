/**
 * Features Section E2E Tests
 * Owner: Scenario 2 - Features Section Grid
 *
 * Test coverage:
 * - All 6 feature cards are displayed
 * - Each card has icon, title, and description
 * - Responsive grid layout (3 columns desktop, 2 tablet, 1 mobile)
 */

const { test, expect } = require('@playwright/test');

// Feature data for validation
const FEATURES = [
  {
    name: 'Memcached Compatible',
    dataAttr: 'memcached-compatible',
    description: 'Use existing memcached clients and libraries'
  },
  {
    name: 'Persistent Storage',
    dataAttr: 'persistent-storage',
    description: 'Data survives restarts with LSM-tree architecture'
  },
  {
    name: 'High Performance',
    dataAttr: 'high-performance',
    description: 'Async I/O with Tokio for concurrent connections'
  },
  {
    name: 'Efficient Compaction',
    dataAttr: 'efficient-compaction',
    description: 'Automatic minor and major compaction strategies'
  },
  {
    name: 'Memory Safe',
    dataAttr: 'memory-safe',
    description: 'Rust implementation eliminates memory safety vulnerabilities'
  },
  {
    name: 'Configurable',
    dataAttr: 'configurable',
    description: 'TOML-based configuration for all storage parameters'
  }
];

test.describe('Features Section Grid', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  test('displays 6 feature cards', async ({ page }) => {
    // Test case 1: Load features section and verify 6 cards
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    const featureCards = page.locator('.feature-card');
    await expect(featureCards).toHaveCount(6);

    // Verify all expected feature names are present
    for (const feature of FEATURES) {
      const card = page.locator(`[data-feature="${feature.dataAttr}"]`);
      await expect(card).toBeVisible();
    }
  });

  test('Memcached Compatible feature has icon, title, and description', async ({ page }) => {
    // Test case 2: Check Memcached Compatible feature content
    const card = page.locator('[data-feature="memcached-compatible"]');
    await expect(card).toBeVisible();

    // Check icon exists (SVG)
    const icon = card.locator('.feature-card__icon svg');
    await expect(icon).toBeVisible();

    // Check title
    const title = card.locator('.feature-card__title');
    await expect(title).toHaveText('Memcached Compatible');

    // Check description mentions existing memcached clients
    const description = card.locator('.feature-card__description');
    await expect(description).toContainText('existing memcached clients');
  });

  test('Persistent Storage feature has icon, title, and description', async ({ page }) => {
    // Test case 3: Check Persistent Storage feature content
    const card = page.locator('[data-feature="persistent-storage"]');
    await expect(card).toBeVisible();

    // Check icon exists (SVG)
    const icon = card.locator('.feature-card__icon svg');
    await expect(icon).toBeVisible();

    // Check title
    const title = card.locator('.feature-card__title');
    await expect(title).toHaveText('Persistent Storage');

    // Check description mentions LSM-tree and restarts
    const description = card.locator('.feature-card__description');
    await expect(description).toContainText('survives restarts');
    await expect(description).toContainText('LSM-tree');
  });

  test('all feature cards have icon, title, and description', async ({ page }) => {
    // Verify all 6 features have the required elements
    for (const feature of FEATURES) {
      const card = page.locator(`[data-feature="${feature.dataAttr}"]`);
      await expect(card).toBeVisible();

      // Check icon
      const icon = card.locator('.feature-card__icon svg');
      await expect(icon).toBeVisible();

      // Check title
      const title = card.locator('.feature-card__title');
      await expect(title).toHaveText(feature.name);

      // Check description
      const description = card.locator('.feature-card__description');
      await expect(description).toContainText(feature.description.substring(0, 20));
    }
  });
});

test.describe('Features Grid Responsive Layout', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  test('displays 3-column grid on desktop (1920px)', async ({ page, browserName }, testInfo) => {
    // Test case 4: Desktop viewport
    // Skip if not running in desktop project
    if (testInfo.project.name !== 'Desktop Chrome') {
      test.skip();
    }

    await page.setViewportSize({ width: 1920, height: 1080 });
    await page.waitForTimeout(100); // Allow CSS to apply

    const grid = page.locator('.features-grid');
    await expect(grid).toBeVisible();

    // Get computed style
    const gridStyle = await grid.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        display: style.display,
        gridTemplateColumns: style.gridTemplateColumns
      };
    });

    expect(gridStyle.display).toBe('grid');
    // Should have 3 columns (3 values in gridTemplateColumns)
    const columns = gridStyle.gridTemplateColumns.split(' ').filter(c => c !== '');
    expect(columns.length).toBe(3);
  });

  test('displays 2-column grid on tablet (768px)', async ({ page, browserName }, testInfo) => {
    // Test case 5: Tablet viewport
    // Skip if not running in tablet project
    if (testInfo.project.name !== 'Tablet') {
      test.skip();
    }

    await page.setViewportSize({ width: 768, height: 1024 });
    await page.waitForTimeout(100); // Allow CSS to apply

    const grid = page.locator('.features-grid');
    await expect(grid).toBeVisible();

    // Get computed style
    const gridStyle = await grid.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        display: style.display,
        gridTemplateColumns: style.gridTemplateColumns
      };
    });

    expect(gridStyle.display).toBe('grid');
    // Should have 2 columns
    const columns = gridStyle.gridTemplateColumns.split(' ').filter(c => c !== '');
    expect(columns.length).toBe(2);
  });

  test('displays single-column grid on mobile (375px)', async ({ page, browserName }, testInfo) => {
    // Test case 6: Mobile viewport
    // Skip if not running in mobile project
    if (testInfo.project.name !== 'Mobile') {
      test.skip();
    }

    await page.setViewportSize({ width: 375, height: 667 });
    await page.waitForTimeout(100); // Allow CSS to apply

    const grid = page.locator('.features-grid');
    await expect(grid).toBeVisible();

    // Get computed style
    const gridStyle = await grid.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        display: style.display,
        gridTemplateColumns: style.gridTemplateColumns
      };
    });

    expect(gridStyle.display).toBe('grid');
    // Should have 1 column (single value in gridTemplateColumns)
    const columns = gridStyle.gridTemplateColumns.split(' ').filter(c => c !== '');
    expect(columns.length).toBe(1);
  });
});
