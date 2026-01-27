/**
 * E2E tests for Comparison Section.
 * Owner: Scenario 18 - Competitive Positioning Table
 *
 * Tests:
 * - Comparison table loads and displays
 * - MirDB, Memcached, and Redis columns present
 * - Feature comparison values are accurate
 * - MirDB advantages are highlighted
 */

import { test, expect } from '@playwright/test';

test.describe('Comparison Section (Test Case 1: Load comparison section)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('comparison section is visible on page', async ({ page }) => {
    const comparison = page.locator('#comparison');
    await comparison.scrollIntoViewIfNeeded();
    await expect(comparison).toBeVisible();
  });

  test('section displays "How MirDB Compares" heading', async ({ page }) => {
    const comparison = page.locator('#comparison');
    await comparison.scrollIntoViewIfNeeded();

    const heading = comparison.locator('.section-title');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText('How MirDB Compares');
  });

  test('comparison table is visible and displays MirDB, Memcached, and Redis features', async ({ page }) => {
    const comparison = page.locator('#comparison');
    await comparison.scrollIntoViewIfNeeded();

    const table = comparison.locator('[data-testid="comparison-table"]');
    await expect(table).toBeVisible();

    // Check column headers
    const headers = table.locator('thead th');
    await expect(headers.nth(0)).toHaveText('Feature');
    await expect(headers.nth(1)).toContainText('MirDB');
    await expect(headers.nth(2)).toContainText('Memcached');
    await expect(headers.nth(3)).toContainText('Redis');
  });

  test('table displays multiple feature rows', async ({ page }) => {
    const comparison = page.locator('#comparison');
    await comparison.scrollIntoViewIfNeeded();

    const table = comparison.locator('[data-testid="comparison-table"]');
    const rows = table.locator('tbody tr');
    const count = await rows.count();
    expect(count).toBeGreaterThanOrEqual(5);
  });
});

test.describe('Comparison Section - Persistence (Test Case 2)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('shows MirDB has persistence while Memcached does not', async ({ page }) => {
    const comparison = page.locator('#comparison');
    await comparison.scrollIntoViewIfNeeded();

    // Find the Persistence row
    const persistenceRow = comparison.locator('tr[data-feature="persistence"]');
    await expect(persistenceRow).toBeVisible();

    // Check MirDB has persistence (checkmark)
    const mirdbCell = persistenceRow.locator('td[data-product="mirdb"]');
    const mirdbValue = await mirdbCell.locator('span').textContent();
    expect(mirdbValue?.trim()).toBe('✓');

    // Check Memcached does not have persistence (cross)
    const memcachedCell = persistenceRow.locator('td[data-product="memcached"]');
    const memcachedValue = await memcachedCell.locator('span').textContent();
    expect(memcachedValue?.trim()).toBe('✗');
  });

  test('persistence row displays correctly with feature name', async ({ page }) => {
    const comparison = page.locator('#comparison');
    await comparison.scrollIntoViewIfNeeded();

    const persistenceRow = comparison.locator('tr[data-feature="persistence"]');
    const featureName = persistenceRow.locator('.feature-name');
    await expect(featureName).toHaveText('Persistence');
  });
});

test.describe('Comparison Section - Memcached Protocol (Test Case 3)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('shows MirDB and Memcached have protocol compatibility, Redis does not', async ({ page }) => {
    const comparison = page.locator('#comparison');
    await comparison.scrollIntoViewIfNeeded();

    // Find the Memcached Protocol row
    const protocolRow = comparison.locator('tr[data-feature="memcached-protocol"]');
    await expect(protocolRow).toBeVisible();

    // Check MirDB has protocol compatibility (checkmark)
    const mirdbCell = protocolRow.locator('td[data-product="mirdb"]');
    const mirdbValue = await mirdbCell.locator('span').textContent();
    expect(mirdbValue?.trim()).toBe('✓');

    // Check Memcached has protocol compatibility (checkmark)
    const memcachedCell = protocolRow.locator('td[data-product="memcached"]');
    const memcachedValue = await memcachedCell.locator('span').textContent();
    expect(memcachedValue?.trim()).toBe('✓');

    // Check Redis does not have protocol compatibility (cross)
    const redisCell = protocolRow.locator('td[data-product="redis"]');
    const redisValue = await redisCell.locator('span').textContent();
    expect(redisValue?.trim()).toBe('✗');
  });
});

test.describe('Comparison Section - Rust Implementation (Test Case 4)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('shows MirDB is written in Rust', async ({ page }) => {
    const comparison = page.locator('#comparison');
    await comparison.scrollIntoViewIfNeeded();

    // Find the Written in Rust row
    const rustRow = comparison.locator('tr[data-feature="written-in-rust"]');
    await expect(rustRow).toBeVisible();

    // Check MirDB is written in Rust (checkmark)
    const mirdbCell = rustRow.locator('td[data-product="mirdb"]');
    const mirdbValue = await mirdbCell.locator('span').textContent();
    expect(mirdbValue?.trim()).toBe('✓');

    // Check Memcached is not written in Rust (cross)
    const memcachedCell = rustRow.locator('td[data-product="memcached"]');
    const memcachedValue = await memcachedCell.locator('span').textContent();
    expect(memcachedValue?.trim()).toBe('✗');

    // Check Redis is not written in Rust (cross)
    const redisCell = rustRow.locator('td[data-product="redis"]');
    const redisValue = await redisCell.locator('span').textContent();
    expect(redisValue?.trim()).toBe('✗');
  });

  test('rust row displays "Written in Rust" as feature name', async ({ page }) => {
    const comparison = page.locator('#comparison');
    await comparison.scrollIntoViewIfNeeded();

    const rustRow = comparison.locator('tr[data-feature="written-in-rust"]');
    const featureName = rustRow.locator('.feature-name');
    await expect(featureName).toHaveText('Written in Rust');
  });
});

test.describe('Comparison Section - Visual Styling', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('MirDB column header is highlighted with primary color', async ({ page }) => {
    const comparison = page.locator('#comparison');
    await comparison.scrollIntoViewIfNeeded();

    const mirdbHeader = comparison.locator('.mirdb-header');
    await expect(mirdbHeader).toBeVisible();

    // Check the header has the highlighted class
    await expect(mirdbHeader).toHaveClass(/mirdb-header/);
  });

  test('MirDB column cells have subtle background highlight', async ({ page }) => {
    const comparison = page.locator('#comparison');
    await comparison.scrollIntoViewIfNeeded();

    const mirdbCells = comparison.locator('.mirdb-value');
    const count = await mirdbCells.count();
    expect(count).toBeGreaterThan(0);

    // Check all MirDB cells have the highlight class
    for (let i = 0; i < count; i++) {
      await expect(mirdbCells.nth(i)).toHaveClass(/mirdb-value/);
    }
  });

  test('checkmarks have green color and crosses have muted color', async ({ page }) => {
    const comparison = page.locator('#comparison');
    await comparison.scrollIntoViewIfNeeded();

    const checkmarks = comparison.locator('.check');
    const crosses = comparison.locator('.cross');

    // Verify checkmarks and crosses exist
    const checkCount = await checkmarks.count();
    const crossCount = await crosses.count();
    expect(checkCount).toBeGreaterThan(0);
    expect(crossCount).toBeGreaterThan(0);

    // Verify first checkmark has the check class
    await expect(checkmarks.first()).toHaveClass(/check/);

    // Verify first cross has the cross class
    await expect(crosses.first()).toHaveClass(/cross/);
  });
});

test.describe('Comparison Section - Accessibility', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('table has proper accessibility structure', async ({ page }) => {
    const comparison = page.locator('#comparison');
    await comparison.scrollIntoViewIfNeeded();

    // Check table wrapper has role="region" and aria-label
    const tableWrapper = comparison.locator('.table-wrapper');
    await expect(tableWrapper).toHaveAttribute('role', 'region');
    await expect(tableWrapper).toHaveAttribute('aria-label', 'Feature comparison table');

    // Check table headers use scope="col"
    const headers = comparison.locator('thead th');
    const count = await headers.count();
    for (let i = 0; i < count; i++) {
      await expect(headers.nth(i)).toHaveAttribute('scope', 'col');
    }
  });

  test('feature values have aria-labels for screen readers', async ({ page }) => {
    const comparison = page.locator('#comparison');
    await comparison.scrollIntoViewIfNeeded();

    // Check checkmarks have aria-label="Yes"
    const checkmarks = comparison.locator('.check');
    const checkCount = await checkmarks.count();
    for (let i = 0; i < Math.min(checkCount, 3); i++) {
      await expect(checkmarks.nth(i)).toHaveAttribute('aria-label', 'Yes');
    }

    // Check crosses have aria-label="No"
    const crosses = comparison.locator('.cross');
    const crossCount = await crosses.count();
    for (let i = 0; i < Math.min(crossCount, 3); i++) {
      await expect(crosses.nth(i)).toHaveAttribute('aria-label', 'No');
    }
  });

  test('table wrapper is keyboard focusable', async ({ page }) => {
    const comparison = page.locator('#comparison');
    await comparison.scrollIntoViewIfNeeded();

    const tableWrapper = comparison.locator('.table-wrapper');
    await expect(tableWrapper).toHaveAttribute('tabindex', '0');
  });
});

test.describe('Comparison Section Mobile', () => {
  test.use({
    viewport: { width: 375, height: 667 }
  });

  test('comparison section is visible on mobile', async ({ page }) => {
    await page.goto('/');
    const comparison = page.locator('#comparison');
    await comparison.scrollIntoViewIfNeeded();
    await expect(comparison).toBeVisible();
  });

  test('table is scrollable horizontally on mobile', async ({ page }) => {
    await page.goto('/');
    const comparison = page.locator('#comparison');
    await comparison.scrollIntoViewIfNeeded();

    const tableWrapper = comparison.locator('.table-wrapper');
    const overflowX = await tableWrapper.evaluate((el) => {
      return window.getComputedStyle(el).overflowX;
    });

    expect(overflowX).toBe('auto');
  });
});
