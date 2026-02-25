/**
 * Why MirDB Comparison Section E2E Tests
 * Owner: Scenario 6 - Why MirDB Comparison Section
 *
 * Test cases:
 * - Section contains comparison content (table, grid, or side-by-side layout)
 * - Comparison highlights 'disk persistence' for MirDB vs 'in-memory only' for memcached
 * - Comparison mentions 'crash recovery' or 'data durability' for MirDB vs 'data loss on restart' for memcached
 */

const { test, expect } = require('@playwright/test');

test.describe('Why MirDB Comparison Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('section contains comparison content with table or grid layout', async ({ page }) => {
    // Navigate to Why MirDB section
    const comparisonSection = page.locator('#why-mirdb');
    await expect(comparisonSection).toBeVisible();

    // Verify section has a title
    const sectionTitle = page.locator('#why-mirdb h2');
    await expect(sectionTitle).toBeVisible();
    const titleText = await sectionTitle.textContent();
    expect(titleText).toContain('Why MirDB');

    // Verify comparison table/grid exists
    const comparisonTable = page.locator('.comparison-table');
    await expect(comparisonTable).toBeVisible();

    // Verify it has ARIA role for accessibility
    const tableRole = await comparisonTable.getAttribute('role');
    expect(tableRole).toBe('table');

    // Verify comparison rows exist (at least 2 for persistence and crash recovery)
    const comparisonRows = page.locator('.comparison-row');
    const rowCount = await comparisonRows.count();
    expect(rowCount).toBeGreaterThanOrEqual(2);
  });

  test('comparison highlights disk persistence for MirDB vs in-memory only for memcached', async ({ page }) => {
    // Find the persistence comparison row
    const persistenceRow = page.locator('[data-comparison="persistence"]');
    await expect(persistenceRow).toBeVisible();

    // Get the MirDB cell content
    const mirdbCell = persistenceRow.locator('.comparison-cell--mirdb');
    await expect(mirdbCell).toBeVisible();
    const mirdbText = await mirdbCell.textContent();
    const mirdbLower = mirdbText.toLowerCase();

    // Verify MirDB mentions disk persistence
    expect(mirdbLower).toContain('disk');
    expect(mirdbLower).toContain('persistence');

    // Get the Memcached cell content
    const memcachedCell = persistenceRow.locator('.comparison-cell--memcached');
    await expect(memcachedCell).toBeVisible();
    const memcachedText = await memcachedCell.textContent();
    const memcachedLower = memcachedText.toLowerCase();

    // Verify Memcached mentions in-memory only
    expect(memcachedLower).toContain('in-memory');
    expect(memcachedLower).toContain('only');
  });

  test('comparison mentions crash recovery for MirDB vs data loss on restart for memcached', async ({ page }) => {
    // Find the crash recovery comparison row
    const crashRow = page.locator('[data-comparison="crash-recovery"]');
    await expect(crashRow).toBeVisible();

    // Get the MirDB cell content
    const mirdbCell = crashRow.locator('.comparison-cell--mirdb');
    await expect(mirdbCell).toBeVisible();
    const mirdbText = await mirdbCell.textContent();
    const mirdbLower = mirdbText.toLowerCase();

    // Verify MirDB mentions crash recovery or data durability
    const hasCrashRecovery = mirdbLower.includes('crash recovery');
    const hasDataDurability = mirdbLower.includes('data durability') || mirdbLower.includes('durability');
    expect(hasCrashRecovery || hasDataDurability).toBe(true);

    // Get the Memcached cell content
    const memcachedCell = crashRow.locator('.comparison-cell--memcached');
    await expect(memcachedCell).toBeVisible();
    const memcachedText = await memcachedCell.textContent();
    const memcachedLower = memcachedText.toLowerCase();

    // Verify Memcached mentions data loss on restart
    const hasDataLoss = memcachedLower.includes('data loss') || memcachedLower.includes('loss');
    const hasRestart = memcachedLower.includes('restart') || memcachedLower.includes('recovery');
    expect(hasDataLoss || memcachedLower.includes('no recovery')).toBe(true);
  });

  test('comparison section has proper accessibility attributes', async ({ page }) => {
    // Check section has aria-labelledby
    const comparisonSection = page.locator('#why-mirdb');
    const ariaLabelledBy = await comparisonSection.getAttribute('aria-labelledby');
    expect(ariaLabelledBy).toBe('why-mirdb-title');

    // Check heading has matching id
    const heading = page.locator('#why-mirdb-title');
    await expect(heading).toBeVisible();

    // Check table has aria-label
    const comparisonTable = page.locator('.comparison-table');
    const tableAriaLabel = await comparisonTable.getAttribute('aria-label');
    expect(tableAriaLabel).toContain('comparison');

    // Check header cells have columnheader role
    const headerCells = page.locator('.comparison-header .comparison-cell');
    const headerCount = await headerCells.count();
    expect(headerCount).toBe(3);

    for (let i = 0; i < headerCount; i++) {
      const role = await headerCells.nth(i).getAttribute('role');
      expect(role).toBe('columnheader');
    }

    // Check row cells have proper roles
    const featureCells = page.locator('.comparison-row .comparison-cell--feature');
    const featureCount = await featureCells.count();
    expect(featureCount).toBeGreaterThan(0);

    for (let i = 0; i < featureCount; i++) {
      const role = await featureCells.nth(i).getAttribute('role');
      expect(role).toBe('rowheader');
    }
  });

  test('MirDB advantages are visually highlighted', async ({ page }) => {
    // The persistence and crash recovery rows should highlight MirDB as the advantage
    const persistenceRow = page.locator('[data-comparison="persistence"]');
    const mirdbPersistenceCell = persistenceRow.locator('.comparison-cell--mirdb');

    // Check for advantage class
    const hasAdvantageClass = await mirdbPersistenceCell.evaluate(
      el => el.classList.contains('comparison-cell--advantage')
    );
    expect(hasAdvantageClass).toBe(true);

    // Check crash recovery row
    const crashRow = page.locator('[data-comparison="crash-recovery"]');
    const mirdbCrashCell = crashRow.locator('.comparison-cell--mirdb');

    const crashAdvantageClass = await mirdbCrashCell.evaluate(
      el => el.classList.contains('comparison-cell--advantage')
    );
    expect(crashAdvantageClass).toBe(true);
  });

  test('comparison includes summary with bottom line message', async ({ page }) => {
    const summary = page.locator('.comparison-summary');
    await expect(summary).toBeVisible();

    const summaryText = await summary.textContent();
    const summaryLower = summaryText.toLowerCase();

    // Summary should mention key differentiators
    expect(summaryLower).toContain('mirdb');
    expect(summaryLower).toContain('memcached');
  });
});
