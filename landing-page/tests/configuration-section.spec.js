// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for Configuration Section Display (Scenario 5)
 *
 * These tests verify that the configuration section displays options
 * and default settings correctly as per REQ-5.
 */

test.describe('Configuration Section Display', () => {
  test.beforeEach(async ({ page }) => {
    // Load landing page and navigate to configuration section
    await page.goto('/');
  });

  /**
   * Test Case 1: Listen address parameter with default '0.0.0.0:12333' is displayed
   * Input: Query for listen address configuration
   * Expected: Listen address parameter with default '0.0.0.0:12333' is displayed
   */
  test('TC1: should display listen address parameter with default 0.0.0.0:12333', async ({ page }) => {
    // Navigate to Configuration section
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Look for listen address in the configuration table
    const configTable = configSection.locator('.config-table');
    await expect(configTable).toBeVisible();

    // Find the row containing listen address
    const listenAddressRow = configTable.locator('tbody tr').filter({ hasText: /listen.*address|addr/i });
    await expect(listenAddressRow).toBeVisible();

    // Verify the default value is 0.0.0.0:12333
    const rowText = await listenAddressRow.textContent();
    expect(rowText).toContain('0.0.0.0:12333');
  });

  /**
   * Test Case 2: Work directory parameter with default '/tmp/mirdb' is displayed
   * Input: Query for work directory configuration
   * Expected: Work directory parameter with default '/tmp/mirdb' is displayed
   */
  test('TC2: should display work directory parameter with default /tmp/mirdb', async ({ page }) => {
    // Navigate to Configuration section
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Look for work directory in the configuration table
    const configTable = configSection.locator('.config-table');
    await expect(configTable).toBeVisible();

    // Find the row containing work directory
    const workDirRow = configTable.locator('tbody tr').filter({ hasText: /work.*dir|directory/i });
    await expect(workDirRow).toBeVisible();

    // Verify the default value is /tmp/mirdb
    const rowText = await workDirRow.textContent();
    expect(rowText).toContain('/tmp/mirdb');
  });

  /**
   * Test Case 3: Max LSM levels parameter with default '7' is displayed
   * Input: Query for max LSM levels configuration
   * Expected: Max LSM levels parameter with default '7' is displayed
   */
  test('TC3: should display max LSM levels parameter with default 7', async ({ page }) => {
    // Navigate to Configuration section
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Look for max LSM levels in the configuration table
    const configTable = configSection.locator('.config-table');
    await expect(configTable).toBeVisible();

    // Find the row containing LSM levels
    const lsmLevelsRow = configTable.locator('tbody tr').filter({ hasText: /lsm.*level|max.*level/i });
    await expect(lsmLevelsRow).toBeVisible();

    // Verify the default value is 7
    const rowText = await lsmLevelsRow.textContent();
    expect(rowText).toMatch(/\b7\b/);
  });

  /**
   * Test Case 4: Memtable size parameter with default '4MB' is displayed
   * Input: Query for memtable size configuration
   * Expected: Memtable size parameter with default '4MB' is displayed
   */
  test('TC4: should display memtable size parameter with default 4MB', async ({ page }) => {
    // Navigate to Configuration section
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Look for memtable size in the configuration table
    const configTable = configSection.locator('.config-table');
    await expect(configTable).toBeVisible();

    // Find the row containing memtable size
    const memtableRow = configTable.locator('tbody tr').filter({ hasText: /mem.*table.*size/i });
    await expect(memtableRow).toBeVisible();

    // Verify the default value is 4MB
    const rowText = await memtableRow.textContent();
    expect(rowText).toMatch(/4\s*MB|4M/i);
  });

  /**
   * Test Case 5: SSTable max size parameter with default '100MB' is displayed
   * Input: Query for SSTable max size configuration
   * Expected: SSTable max size parameter with default '100MB' is displayed
   */
  test('TC5: should display SSTable max size parameter with default 100MB', async ({ page }) => {
    // Navigate to Configuration section
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Look for SSTable size in the configuration table
    const configTable = configSection.locator('.config-table');
    await expect(configTable).toBeVisible();

    // Find the row containing SSTable size
    const sstableRow = configTable.locator('tbody tr').filter({ hasText: /sstable.*size|sst.*size/i });
    await expect(sstableRow).toBeVisible();

    // Verify the default value is 100MB
    const rowText = await sstableRow.textContent();
    expect(rowText).toMatch(/100\s*MB|100M/i);
  });

  /**
   * Test Case 6: Complete TOML configuration file example is displayed
   * Input: Query for configuration file example
   * Expected: Complete TOML configuration file example is displayed
   */
  test('TC6: should display complete TOML configuration file example', async ({ page }) => {
    // Navigate to Configuration section
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Look for TOML configuration example code block
    const configExample = configSection.locator('pre code');
    await expect(configExample).toBeVisible();

    // Verify the TOML example contains key configuration parameters
    const exampleText = await configExample.textContent();

    // Should contain addr parameter
    expect(exampleText).toMatch(/addr\s*=\s*["']?0\.0\.0\.0:12333["']?/);

    // Should contain work_dir parameter
    expect(exampleText).toMatch(/work_dir\s*=\s*["']?\/tmp\/mirdb["']?/);

    // Should contain max_level parameter
    expect(exampleText).toMatch(/max_level\s*=\s*7/);

    // Should contain mem_table_max_size parameter
    expect(exampleText).toMatch(/mem_table_max_size\s*=\s*["']?4M["']?/);

    // Should contain sst_max_size parameter
    expect(exampleText).toMatch(/sst_max_size\s*=\s*["']?100M["']?/);
  });

  /**
   * Additional test: Configuration section is navigable from navigation menu
   */
  test('Configuration section is navigable from navigation menu', async ({ page }) => {
    // Click on Configuration navigation link
    const navLink = page.locator('nav a[href="#configuration"]');
    await expect(navLink).toBeVisible();
    await navLink.click();

    // Verify section is scrolled into view
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeInViewport();
  });

  /**
   * Additional test: Configuration table has proper headers
   */
  test('Configuration table has proper headers', async ({ page }) => {
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    const configTable = configSection.locator('.config-table');
    await expect(configTable).toBeVisible();

    // Check for Parameter header
    const parameterHeader = configTable.locator('th').filter({ hasText: /parameter/i });
    await expect(parameterHeader).toBeVisible();

    // Check for Default header
    const defaultHeader = configTable.locator('th').filter({ hasText: /default/i });
    await expect(defaultHeader).toBeVisible();

    // Check for Description header
    const descriptionHeader = configTable.locator('th').filter({ hasText: /description/i });
    await expect(descriptionHeader).toBeVisible();
  });

  /**
   * Additional test: Configuration section has proper heading
   */
  test('Configuration section has proper heading', async ({ page }) => {
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    const heading = configSection.locator('h2');
    await expect(heading).toHaveText('Configuration');
  });

  /**
   * Additional test: All required configuration parameters are present
   */
  test('All required configuration parameters are present in table', async ({ page }) => {
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    const configTable = configSection.locator('.config-table tbody');
    await expect(configTable).toBeVisible();

    // Get all configuration rows
    const rows = configTable.locator('tr');
    const rowCount = await rows.count();

    // Should have at least 5 rows (listen_address, work_directory, max_lsm_levels, memtable_size, sstable_size)
    expect(rowCount).toBeGreaterThanOrEqual(5);

    // Verify all key parameters are present
    const tableText = await configTable.textContent();
    expect(tableText).toMatch(/listen|addr/i);
    expect(tableText).toMatch(/work.*dir/i);
    expect(tableText).toMatch(/lsm.*level|max.*level/i);
    expect(tableText).toMatch(/mem.*table/i);
    expect(tableText).toMatch(/sstable|sst/i);
  });
});
