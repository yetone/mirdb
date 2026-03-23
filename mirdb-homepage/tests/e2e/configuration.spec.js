/**
 * Configuration Section Tests
 * Owner: Scenario 5 - Configuration Options Section
 *
 * Test cases:
 * - Listen Address setting (0.0.0.0:12333)
 * - Max LSM Levels setting (7)
 * - Work Directory setting (/tmp/mirdb)
 * - SSTable Max Size setting (100MB)
 * - Memtable Max Size setting (4MB)
 * - Block Size setting (4KB)
 * - Table structure with Setting, Default, Description columns
 */

const { test, expect } = require('@playwright/test');

test.describe('Configuration Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('http://localhost:3000/index.html');
    // Navigate to configuration section
    await page.locator('#configuration').scrollIntoViewIfNeeded();
  });

  test('should display Listen Address setting with correct default', async ({ page }) => {
    const configSection = page.locator('#configuration');

    // Check for Listen Address setting
    await expect(configSection.locator('text=Listen Address')).toBeVisible();

    // Check for default value 0.0.0.0:12333
    await expect(configSection.locator('text=0.0.0.0:12333')).toBeVisible();

    // Check for description
    const listenRow = configSection.locator('tr', { has: page.locator('text=Listen Address') });
    await expect(listenRow.locator('.config__setting-desc')).toContainText('Network interface and port');
  });

  test('should display Max LSM Levels setting with correct default', async ({ page }) => {
    const configSection = page.locator('#configuration');

    // Check for Max LSM Levels setting
    await expect(configSection.locator('text=Max LSM Levels')).toBeVisible();

    // Check for default value 7
    const lsmRow = configSection.locator('tr', { has: page.locator('text=Max LSM Levels') });
    await expect(lsmRow.locator('code')).toContainText('7');

    // Check for description
    await expect(lsmRow.locator('.config__setting-desc')).toContainText('SSTable levels');
  });

  test('should display Work Directory setting with correct default', async ({ page }) => {
    const configSection = page.locator('#configuration');

    // Check for Work Directory setting
    await expect(configSection.locator('.config__setting-name', { hasText: 'Work Directory' })).toBeVisible();

    // Check for default value /tmp/mirdb
    const workDirRow = configSection.locator('tr', { has: page.locator('.config__setting-name', { hasText: 'Work Directory' }) });
    await expect(workDirRow.locator('code')).toContainText('/tmp/mirdb');

    // Check for description
    await expect(workDirRow.locator('.config__setting-desc')).toContainText('Storage path');
  });

  test('should display SSTable Max Size setting with correct default', async ({ page }) => {
    const configSection = page.locator('#configuration');

    // Check for SSTable Max Size setting
    await expect(configSection.locator('text=SSTable Max Size')).toBeVisible();

    // Check for default value 100MB
    await expect(configSection.locator('text=100MB')).toBeVisible();

    // Check for description
    const sstableRow = configSection.locator('tr', { has: page.locator('text=SSTable Max Size') });
    await expect(sstableRow.locator('.config__setting-desc')).toContainText('Maximum size per SSTable');
  });

  test('should display Memtable Max Size setting with correct default', async ({ page }) => {
    const configSection = page.locator('#configuration');

    // Check for Memtable Max Size setting
    await expect(configSection.locator('text=Memtable Max Size')).toBeVisible();

    // Check for default value 4MB
    await expect(configSection.locator('text=4MB')).toBeVisible();

    // Check for description
    const memtableRow = configSection.locator('tr', { has: page.locator('text=Memtable Max Size') });
    await expect(memtableRow.locator('.config__setting-desc')).toContainText('Memory limit');
  });

  test('should display Block Size setting with correct default', async ({ page }) => {
    const configSection = page.locator('#configuration');

    // Check for Block Size setting (use exact match on the setting name cell)
    await expect(configSection.locator('.config__setting-name', { hasText: 'Block Size' })).toBeVisible();

    // Check for default value 4KB
    const blockRow = configSection.locator('tr', { has: page.locator('.config__setting-name', { hasText: 'Block Size' }) });
    await expect(blockRow.locator('code')).toContainText('4KB');

    // Check for description
    await expect(blockRow.locator('.config__setting-desc')).toContainText('block size');
  });

  test('should have organized table with proper columns structure', async ({ page }) => {
    const configSection = page.locator('#configuration');
    const table = configSection.locator('.config__table');

    // Check table exists
    await expect(table).toBeVisible();

    // Check for header columns: Setting, Default Value, Description
    const headerRow = table.locator('thead tr');
    const headers = headerRow.locator('th');

    await expect(headers).toHaveCount(3);
    await expect(headers.nth(0)).toContainText('Setting');
    await expect(headers.nth(1)).toContainText('Default Value');
    await expect(headers.nth(2)).toContainText('Description');

    // Check that we have 6 configuration settings
    const bodyRows = table.locator('tbody tr');
    await expect(bodyRows).toHaveCount(6);

    // Verify each row has 3 cells
    for (let i = 0; i < 6; i++) {
      const cells = bodyRows.nth(i).locator('td');
      await expect(cells).toHaveCount(3);
    }
  });
});
