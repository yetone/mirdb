/**
 * Comparison Section E2E Tests
 * Owner: Scenario 9 - MirDB vs Memcached Comparison
 *
 * Tests:
 * - Comparison section exists and is visible
 * - Comparison is presented in table/grid format
 * - Visual verification of comparison content
 */
import { test, expect } from '@playwright/test';

test.describe('MirDB vs Memcached Comparison', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/index.html');
  });

  test('TC1: Check for comparison section - Section exists comparing MirDB and memcached', async ({ page }) => {
    // Check for comparison section existence
    const comparisonSection = page.locator('#comparison');
    await expect(comparisonSection).toBeVisible();

    // Verify heading mentions both MirDB and memcached
    const heading = comparisonSection.locator('h2');
    await expect(heading).toBeVisible();
    await expect(heading).toContainText(/MirDB/i);
    await expect(heading).toContainText(/Memcached/i);
  });

  test('TC4: Check comparison table/grid structure - Comparison is presented in readable table or grid format', async ({ page }) => {
    // Check for comparison section
    const comparisonSection = page.locator('#comparison');
    await expect(comparisonSection).toBeVisible();

    // Verify table structure exists
    const comparisonTable = comparisonSection.locator('table');
    await expect(comparisonTable).toBeVisible();

    // Verify table has proper structure with thead and tbody
    const tableHead = comparisonTable.locator('thead');
    await expect(tableHead).toBeVisible();

    const tableBody = comparisonTable.locator('tbody');
    await expect(tableBody).toBeVisible();

    // Verify table has header row with 3 columns
    const headerCells = tableHead.locator('th');
    await expect(headerCells).toHaveCount(3);

    // Verify table has comparison rows
    const tableRows = tableBody.locator('tr.comparison-row');
    const rowCount = await tableRows.count();
    expect(rowCount).toBeGreaterThanOrEqual(3);
  });

  test('Comparison section is accessible via scroll', async ({ page }) => {
    // Scroll to comparison section
    const comparisonSection = page.locator('#comparison');
    await comparisonSection.scrollIntoViewIfNeeded();
    await expect(comparisonSection).toBeInViewport();
  });

  test('Persistence comparison row is visible', async ({ page }) => {
    // Navigate to comparison section
    const comparisonSection = page.locator('#comparison');
    await comparisonSection.scrollIntoViewIfNeeded();

    // Check persistence row
    const persistenceRow = comparisonSection.locator('[data-feature="persistence"]');
    await expect(persistenceRow).toBeVisible();

    // Verify MirDB shows Yes for persistence
    const mirdbCell = persistenceRow.locator('.comparison-yes').first();
    await expect(mirdbCell).toBeVisible();
    await expect(mirdbCell).toContainText('Yes');

    // Verify Memcached shows No for persistence
    const memcachedCell = persistenceRow.locator('.comparison-no');
    await expect(memcachedCell).toBeVisible();
    await expect(memcachedCell).toContainText('No');
  });

  test('Protocol compatibility row shows both support memcached protocol', async ({ page }) => {
    // Navigate to comparison section
    const comparisonSection = page.locator('#comparison');
    await comparisonSection.scrollIntoViewIfNeeded();

    // Check protocol row
    const protocolRow = comparisonSection.locator('[data-feature="protocol"]');
    await expect(protocolRow).toBeVisible();

    // Both should show Yes for protocol support
    const yesBadges = protocolRow.locator('.comparison-yes');
    await expect(yesBadges).toHaveCount(2);
  });

  test('Key differentiator callout is visible', async ({ page }) => {
    // Navigate to comparison section
    const comparisonSection = page.locator('#comparison');
    await comparisonSection.scrollIntoViewIfNeeded();

    // Check for key differentiator section
    const keyDifferentiator = comparisonSection.locator('h3', { hasText: /Key Differentiator/i });
    await expect(keyDifferentiator).toBeVisible();

    // Verify it mentions persistence
    await expect(keyDifferentiator).toContainText(/Persistence/i);
  });

  test('Table is responsive with horizontal scroll container', async ({ page }) => {
    // Check that the comparison table has overflow handling
    const tableContainer = page.locator('.comparison-table');
    await expect(tableContainer).toBeVisible();

    // Verify it has overflow-x-auto class for responsiveness
    await expect(tableContainer).toHaveClass(/overflow-x-auto/);
  });

  test('Comparison section has proper ARIA labels', async ({ page }) => {
    // Check for comparison section
    const comparisonSection = page.locator('#comparison');
    await expect(comparisonSection).toHaveAttribute('aria-labelledby', 'comparison-heading');

    // Check table has proper role and aria-label
    const table = comparisonSection.locator('table');
    await expect(table).toHaveAttribute('role', 'table');
    await expect(table).toHaveAttribute('aria-label');
  });
});
