// @ts-check
import { test, expect } from '@playwright/test';

test.describe('Feature Comparison Table (REQ-6)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: Comparison table element exists on the page', async ({ page }) => {
    // Query for comparison section which contains the table
    const comparisonSection = page.locator('#comparison');
    await expect(comparisonSection).toBeVisible();

    // Verify the table exists within the section
    const table = comparisonSection.locator('table');
    await expect(table).toBeVisible();
  });

  test('Test Case 2: Table has columns for MirDB, Memcached, and Redis', async ({ page }) => {
    // Check table headers for all three products
    const table = page.locator('#comparison table');

    // Check for MirDB column header
    const mirdbHeader = table.locator('th').filter({ hasText: /mirdb/i });
    await expect(mirdbHeader).toBeVisible();

    // Check for Memcached column header
    const memcachedHeader = table.locator('th').filter({ hasText: /memcached/i });
    await expect(memcachedHeader).toBeVisible();

    // Check for Redis column header
    const redisHeader = table.locator('th').filter({ hasText: /redis/i });
    await expect(redisHeader).toBeVisible();
  });

  test('Test Case 3: Persistence feature comparison shows MirDB has persistence while Memcached does not', async ({ page }) => {
    const table = page.locator('#comparison table');

    // Find the persistence row
    const persistenceRow = table.locator('tr').filter({ hasText: /persistence/i });
    await expect(persistenceRow).toBeVisible();

    // Get all cells in the persistence row
    const cells = persistenceRow.locator('td, th');
    const cellTexts = await cells.allTextContents();

    // Join all cell texts to analyze the row content
    const rowContent = cellTexts.join(' ').toLowerCase();

    // Verify MirDB shows persistence support (Yes, ✓, checkmark, or similar positive indicator)
    // and Memcached shows no persistence (No, ✗, or similar negative indicator)
    // The row should indicate MirDB has persistence and Memcached doesn't
    expect(rowContent).toMatch(/persistence/i);

    // Check for indicators that MirDB has persistence
    const mirdbCell = persistenceRow.locator('[data-product="mirdb"], td:nth-child(2)');
    const mirdbText = await mirdbCell.textContent();
    expect(mirdbText?.toLowerCase()).toMatch(/yes|✓|✔|lsm|disk|true/i);

    // Check for indicators that Memcached lacks persistence
    const memcachedCell = persistenceRow.locator('[data-product="memcached"], td:nth-child(3)');
    const memcachedText = await memcachedCell.textContent();
    expect(memcachedText?.toLowerCase()).toMatch(/no|✗|✘|memory|false|-/i);
  });

  test('Test Case 4: Protocol comparison shows MirDB supports Memcached protocol', async ({ page }) => {
    const table = page.locator('#comparison table');

    // Find the protocol row
    const protocolRow = table.locator('tr').filter({ hasText: /protocol/i });
    await expect(protocolRow).toBeVisible();

    // Check that MirDB shows Memcached protocol support
    const mirdbCell = protocolRow.locator('[data-product="mirdb"], td:nth-child(2)');
    const mirdbText = await mirdbCell.textContent();
    expect(mirdbText?.toLowerCase()).toMatch(/memcached|compatible|yes|✓/i);
  });

  test('Test Case 5: Table is properly structured with headers for screen readers', async ({ page }) => {
    const table = page.locator('#comparison table');

    // Check that the table element exists
    await expect(table).toBeVisible();

    // Check for proper table structure with thead or th elements
    const tableHeaders = table.locator('th');
    const headerCount = await tableHeaders.count();
    expect(headerCount).toBeGreaterThanOrEqual(3); // At least Feature, MirDB, Memcached, Redis

    // Verify table has proper semantic structure
    // Either has thead element or th elements with scope
    const thead = table.locator('thead');
    const theadExists = await thead.count() > 0;

    if (theadExists) {
      await expect(thead).toBeVisible();
    } else {
      // If no thead, at least verify th elements exist in first row
      const firstRowHeaders = table.locator('tr').first().locator('th');
      expect(await firstRowHeaders.count()).toBeGreaterThan(0);
    }

    // Check for caption or aria-label for table description (accessibility)
    const caption = table.locator('caption');
    const hasCaption = await caption.count() > 0;
    const hasAriaLabel = await table.getAttribute('aria-label');
    const hasAriaLabelledBy = await table.getAttribute('aria-labelledby');

    // Table should have at least one accessibility label
    expect(hasCaption || hasAriaLabel || hasAriaLabelledBy).toBeTruthy();
  });
});
