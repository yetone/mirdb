/**
 * E2E tests for Comparison section.
 * Owner: Scenario 6 - Comparison Section
 */
import { test, expect } from '@playwright/test';

test.describe('Comparison Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Comparison section exists', async ({ page }) => {
    // Test case 1: Check Comparison section exists
    const comparisonSection = page.locator('[data-testid="comparison-section"]');
    await expect(comparisonSection).toBeVisible();

    // Check for "Why MirDB?" heading
    const heading = comparisonSection.locator('h2');
    await expect(heading).toContainText('Why MirDB');
  });

  test('Comparison includes MirDB with features highlighted', async ({ page }) => {
    // Test case 2: Verify comparison includes MirDB
    const comparisonTable = page.locator('[data-testid="comparison-table"]');
    await expect(comparisonTable).toBeVisible();

    // MirDB header should be present
    const mirdbHeader = page.locator('[data-testid="product-header-mirdb"]');
    await expect(mirdbHeader).toBeVisible();
    await expect(mirdbHeader).toHaveText('MirDB');

    // MirDB column should be highlighted
    await expect(mirdbHeader).toHaveClass(/highlightedHeader/);
  });

  test('Comparison includes Memcached showing lack of persistence', async ({ page }) => {
    // Test case 3: Verify comparison includes Memcached
    const memcachedHeader = page.locator('[data-testid="product-header-memcached"]');
    await expect(memcachedHeader).toBeVisible();
    await expect(memcachedHeader).toHaveText('Memcached');

    // Memcached persistence cell should show cross (false)
    const memcachedPersistenceCell = page.locator('[data-testid="cell-memcached-persistence"]');
    await expect(memcachedPersistenceCell).toBeVisible();

    const crossIcon = memcachedPersistenceCell.locator('[data-testid="cross-icon"]');
    await expect(crossIcon).toBeVisible();
    await expect(crossIcon).toHaveText('✗');
  });

  test('Comparison includes Redis', async ({ page }) => {
    // Test case 4: Verify comparison includes Redis
    const redisHeader = page.locator('[data-testid="product-header-redis"]');
    await expect(redisHeader).toBeVisible();
    await expect(redisHeader).toHaveText('Redis');
  });

  test('Persistence comparison shows MirDB has persistence while Memcached does not', async ({ page }) => {
    // Test case 5: Verify persistence comparison
    const persistenceRow = page.locator('[data-testid="feature-row-persistence"]');
    await expect(persistenceRow).toBeVisible();
    await expect(persistenceRow).toHaveText('Data Persistence');

    // MirDB has persistence (check icon)
    const mirdbPersistenceCell = page.locator('[data-testid="cell-mirdb-persistence"]');
    const mirdbCheckIcon = mirdbPersistenceCell.locator('[data-testid="check-icon"]');
    await expect(mirdbCheckIcon).toBeVisible();
    await expect(mirdbCheckIcon).toHaveText('✓');

    // Memcached does not have persistence (cross icon)
    const memcachedPersistenceCell = page.locator('[data-testid="cell-memcached-persistence"]');
    const memcachedCrossIcon = memcachedPersistenceCell.locator('[data-testid="cross-icon"]');
    await expect(memcachedCrossIcon).toBeVisible();
    await expect(memcachedCrossIcon).toHaveText('✗');
  });

  test('Comparison table shows all key features', async ({ page }) => {
    const comparisonTable = page.locator('[data-testid="comparison-table"]');
    await expect(comparisonTable).toBeVisible();

    // Check all key features are present
    await expect(page.locator('[data-testid="feature-row-persistence"]')).toBeVisible();
    await expect(page.locator('[data-testid="feature-row-protocol"]')).toBeVisible();
    await expect(page.locator('[data-testid="feature-row-lsmTree"]')).toBeVisible();
    await expect(page.locator('[data-testid="feature-row-inmemory"]')).toBeVisible();
  });

  test('Comparison section is accessible via anchor link', async ({ page }) => {
    // Navigate directly to comparison section
    await page.goto('/#comparison');

    const comparisonSection = page.locator('[data-testid="comparison-section"]');
    await expect(comparisonSection).toBeVisible();
  });

  test('Section has proper heading hierarchy for accessibility', async ({ page }) => {
    const comparisonSection = page.locator('[data-testid="comparison-section"]');

    // Check h2 heading exists
    const h2 = comparisonSection.locator('h2');
    await expect(h2).toBeVisible();
    await expect(h2).toHaveAttribute('id', 'comparison-heading');
  });

  test('Table has Feature column header', async ({ page }) => {
    const comparisonTable = page.locator('[data-testid="comparison-table"]');

    // Check Feature column header
    const featureHeader = comparisonTable.locator('th').first();
    await expect(featureHeader).toHaveText('Feature');
  });

  test('MirDB shows Memcached Protocol support', async ({ page }) => {
    // MirDB should show protocol support
    const mirdbProtocolCell = page.locator('[data-testid="cell-mirdb-protocol"]');
    const checkIcon = mirdbProtocolCell.locator('[data-testid="check-icon"]');
    await expect(checkIcon).toBeVisible();
  });
});
