/**
 * Comparison Table Section E2E Tests
 * Owner: Scenario 4 - Comparison Table Section
 */
import { test, expect } from '@playwright/test';

test.describe('Comparison Table Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('should have a comparison table or grid element in the section', async ({ page }) => {
    // Navigate to comparison section
    const comparisonSection = page.locator('#comparison');
    await expect(comparisonSection).toBeVisible();

    // Check for table element within the comparison section
    const table = comparisonSection.locator('table.comparison__table');
    await expect(table).toBeVisible();

    // Verify table has basic structure (thead and tbody)
    const thead = table.locator('thead');
    const tbody = table.locator('tbody');
    await expect(thead).toBeVisible();
    await expect(tbody).toBeVisible();
  });

  test('should have MirDB column in the comparison table', async ({ page }) => {
    const comparisonSection = page.locator('#comparison');
    await expect(comparisonSection).toBeVisible();

    // Check for MirDB column header
    const tableHeaders = comparisonSection.locator('table.comparison__table thead th');
    const headerTexts = await tableHeaders.allTextContents();

    // Verify at least one header contains 'MirDB'
    const hasMirDBColumn = headerTexts.some(text => text.includes('MirDB'));
    expect(hasMirDBColumn).toBe(true);
  });

  test('should have Memcached or In-Memory column in the comparison table', async ({ page }) => {
    const comparisonSection = page.locator('#comparison');
    await expect(comparisonSection).toBeVisible();

    // Check for Memcached or In-Memory column header
    const tableHeaders = comparisonSection.locator('table.comparison__table thead th');
    const headerTexts = await tableHeaders.allTextContents();

    // Verify at least one header contains 'Memcached' or 'In-Memory'
    const hasMemcachedColumn = headerTexts.some(
      text => text.includes('Memcached') || text.includes('In-Memory')
    );
    expect(hasMemcachedColumn).toBe(true);
  });

  test('should have persistence comparison row with MirDB showing advantage', async ({ page }) => {
    const comparisonSection = page.locator('#comparison');
    await expect(comparisonSection).toBeVisible();

    // Find the row that discusses data persistence
    const persistenceRow = comparisonSection.locator('tr[data-feature="persistence"]');
    await expect(persistenceRow).toBeVisible();

    // Verify the row label contains persistence-related text
    const rowLabel = persistenceRow.locator('td').first();
    const labelText = await rowLabel.textContent();
    expect(labelText?.toLowerCase()).toContain('persist');

    // Verify MirDB has advantage (check mark or highlighted)
    const mirdbCell = persistenceRow.locator('.comparison__cell--mirdb');
    await expect(mirdbCell).toBeVisible();

    // Check for advantage indicator (checkmark, "Yes", or highlight class)
    const hasAdvantage = await mirdbCell.locator('.comparison__check, .comparison__yes').count();
    expect(hasAdvantage).toBeGreaterThan(0);
  });

  test('should have crash recovery comparison row or indicator', async ({ page }) => {
    const comparisonSection = page.locator('#comparison');
    await expect(comparisonSection).toBeVisible();

    // Find the row that discusses crash recovery
    const crashRecoveryRow = comparisonSection.locator('tr[data-feature="crash-recovery"]');
    await expect(crashRecoveryRow).toBeVisible();

    // Verify the row label contains crash recovery-related text
    const rowLabel = crashRecoveryRow.locator('td').first();
    const labelText = await rowLabel.textContent();
    expect(
      labelText?.toLowerCase().includes('crash') ||
      labelText?.toLowerCase().includes('recovery')
    ).toBe(true);

    // Verify MirDB has crash recovery capability
    const mirdbCell = crashRecoveryRow.locator('.comparison__cell--mirdb');
    await expect(mirdbCell).toBeVisible();

    // Check for positive indicator
    const hasCapability = await mirdbCell.locator('.comparison__check, .comparison__yes').count();
    expect(hasCapability).toBeGreaterThan(0);
  });
});
