import { test, expect } from '@playwright/test';

test.describe('Comparison Matrix Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Comparison table is visible with MirDB, Redis, Memcached, LevelDB columns', async ({ page }) => {
    // Navigate to comparison section
    const comparisonSection = page.locator('[data-testid="comparison-section"]');
    await comparisonSection.scrollIntoViewIfNeeded();
    await expect(comparisonSection).toBeVisible();

    // Verify the comparison table exists
    const comparisonTable = page.locator('[data-testid="comparison-table"]');
    await expect(comparisonTable).toBeVisible();

    // Check all solution headers are present
    await expect(page.locator('[data-testid="comparison-header-mirdb"]')).toBeVisible();
    await expect(page.locator('[data-testid="comparison-header-mirdb"]')).toContainText('MirDB');

    await expect(page.locator('[data-testid="comparison-header-redis"]')).toBeVisible();
    await expect(page.locator('[data-testid="comparison-header-redis"]')).toContainText('Redis');

    await expect(page.locator('[data-testid="comparison-header-memcached"]')).toBeVisible();
    await expect(page.locator('[data-testid="comparison-header-memcached"]')).toContainText('Memcached');

    await expect(page.locator('[data-testid="comparison-header-leveldb"]')).toBeVisible();
    await expect(page.locator('[data-testid="comparison-header-leveldb"]')).toContainText('LevelDB');
  });

  test('TC2: Row compares persistence capabilities across all solutions', async ({ page }) => {
    // Navigate to comparison section
    const comparisonSection = page.locator('[data-testid="comparison-section"]');
    await comparisonSection.scrollIntoViewIfNeeded();

    // Check persistence row exists
    const persistenceRow = page.locator('[data-testid="comparison-row-persistence"]');
    await expect(persistenceRow).toBeVisible();

    // Check the feature name
    const persistenceFeature = page.locator('[data-testid="comparison-feature-persistence"]');
    await expect(persistenceFeature).toContainText('Persistence');

    // Check persistence values for each solution
    await expect(page.locator('[data-testid="comparison-cell-persistence-mirdb"]')).toContainText('LSM Tree');
    await expect(page.locator('[data-testid="comparison-cell-persistence-redis"]')).toContainText('RDB/AOF');
    await expect(page.locator('[data-testid="comparison-cell-persistence-memcached"]')).toContainText('None');
    await expect(page.locator('[data-testid="comparison-cell-persistence-leveldb"]')).toContainText('LSM Tree');
  });

  test('TC3: Row shows protocol compatibility (memcached for MirDB)', async ({ page }) => {
    // Navigate to comparison section
    const comparisonSection = page.locator('[data-testid="comparison-section"]');
    await comparisonSection.scrollIntoViewIfNeeded();

    // Check protocol row exists
    const protocolRow = page.locator('[data-testid="comparison-row-protocol"]');
    await expect(protocolRow).toBeVisible();

    // Check the feature name
    const protocolFeature = page.locator('[data-testid="comparison-feature-protocol"]');
    await expect(protocolFeature).toContainText('Protocol');

    // Check that MirDB shows memcached protocol
    await expect(page.locator('[data-testid="comparison-cell-protocol-mirdb"]')).toContainText('Memcached');

    // Check other solutions' protocols
    await expect(page.locator('[data-testid="comparison-cell-protocol-redis"]')).toContainText('Redis');
    await expect(page.locator('[data-testid="comparison-cell-protocol-memcached"]')).toContainText('Memcached');
    await expect(page.locator('[data-testid="comparison-cell-protocol-leveldb"]')).toContainText('Embedded');
  });

  test('TC4: Row shows implementation language (Rust for MirDB)', async ({ page }) => {
    // Navigate to comparison section
    const comparisonSection = page.locator('[data-testid="comparison-section"]');
    await comparisonSection.scrollIntoViewIfNeeded();

    // Check language row exists
    const languageRow = page.locator('[data-testid="comparison-row-language"]');
    await expect(languageRow).toBeVisible();

    // Check the feature name
    const languageFeature = page.locator('[data-testid="comparison-feature-language"]');
    await expect(languageFeature).toContainText('Language');

    // Check that MirDB shows Rust
    await expect(page.locator('[data-testid="comparison-cell-language-mirdb"]')).toContainText('Rust');

    // Check other solutions' languages
    await expect(page.locator('[data-testid="comparison-cell-language-redis"]')).toContainText('C');
    await expect(page.locator('[data-testid="comparison-cell-language-memcached"]')).toContainText('C');
    await expect(page.locator('[data-testid="comparison-cell-language-leveldb"]')).toContainText('C++');
  });

  test('TC5: Table is scrollable or adapts for mobile viewing', async ({ page }) => {
    // Set viewport to mobile size
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');

    // Navigate to comparison section
    const comparisonSection = page.locator('[data-testid="comparison-section"]');
    await comparisonSection.scrollIntoViewIfNeeded();
    await expect(comparisonSection).toBeVisible();

    // Check the table container exists and has overflow-x-auto class for horizontal scrolling
    const tableContainer = page.locator('[data-testid="comparison-table-container"]');
    await expect(tableContainer).toBeVisible();
    await expect(tableContainer).toHaveClass(/overflow-x-auto/);

    // Verify the table is still visible and functional
    const comparisonTable = page.locator('[data-testid="comparison-table"]');
    await expect(comparisonTable).toBeVisible();

    // Verify all headers are still accessible (may require scrolling)
    await expect(page.locator('[data-testid="comparison-header-mirdb"]')).toBeAttached();
    await expect(page.locator('[data-testid="comparison-header-redis"]')).toBeAttached();
    await expect(page.locator('[data-testid="comparison-header-memcached"]')).toBeAttached();
    await expect(page.locator('[data-testid="comparison-header-leveldb"]')).toBeAttached();
  });
});
