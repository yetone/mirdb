// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = path.resolve(__dirname, '../index.html');

/**
 * Configuration Section Tests
 * Verify the configuration section displays all configurable parameters with default values
 */
test.describe('Configuration Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(`file://${indexPath}`);
  });

  /**
   * Test Case 1: addr parameter with default value
   * Input: Check configuration section for addr parameter
   * Expected: addr parameter listed with default '0.0.0.0:12333'
   */
  test('TC1: addr parameter listed with default 0.0.0.0:12333', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Find the configuration table
    const configTable = configSection.locator('table');
    await expect(configTable).toBeVisible();

    // Verify addr parameter is listed
    const addrRow = configTable.locator('tr', { has: page.locator('code:text("addr")') });
    await expect(addrRow).toBeVisible();

    // Verify the default value is 0.0.0.0:12333
    const addrRowText = await addrRow.textContent();
    expect(addrRowText).toContain('0.0.0.0:12333');
  });

  /**
   * Test Case 2: work_dir parameter with default value
   * Input: Check configuration section for work_dir parameter
   * Expected: work_dir parameter listed with default '/tmp/mirdb'
   */
  test('TC2: work_dir parameter listed with default /tmp/mirdb', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Find the configuration table
    const configTable = configSection.locator('table');
    await expect(configTable).toBeVisible();

    // Verify work_dir parameter is listed
    const workDirRow = configTable.locator('tr', { has: page.locator('code:text("work_dir")') });
    await expect(workDirRow).toBeVisible();

    // Verify the default value is /tmp/mirdb
    const workDirRowText = await workDirRow.textContent();
    expect(workDirRowText).toContain('/tmp/mirdb');
  });

  /**
   * Test Case 3: mem_table_max_size parameter with default value
   * Input: Check configuration section for mem_table_max_size parameter
   * Expected: mem_table_max_size parameter listed with default '4MB'
   */
  test('TC3: mem_table_max_size parameter listed with default 4MB', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Find the configuration table
    const configTable = configSection.locator('table');
    await expect(configTable).toBeVisible();

    // Verify mem_table_max_size parameter is listed
    const memTableRow = configTable.locator('tr', { has: page.locator('code:text("mem_table_max_size")') });
    await expect(memTableRow).toBeVisible();

    // Verify the default value is 4MB
    const memTableRowText = await memTableRow.textContent();
    expect(memTableRowText).toContain('4MB');
  });

  /**
   * Test Case 4: sst_max_size parameter with default value
   * Input: Check configuration section for sst_max_size parameter
   * Expected: sst_max_size parameter listed with default '100MB'
   */
  test('TC4: sst_max_size parameter listed with default 100MB', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Find the configuration table
    const configTable = configSection.locator('table');
    await expect(configTable).toBeVisible();

    // Verify sst_max_size parameter is listed
    const sstMaxRow = configTable.locator('tr', { has: page.locator('code:text("sst_max_size")') });
    await expect(sstMaxRow).toBeVisible();

    // Verify the default value is 100MB
    const sstMaxRowText = await sstMaxRow.textContent();
    expect(sstMaxRowText).toContain('100MB');
  });

  /**
   * Test Case 5: max_level parameter with default value
   * Input: Check configuration section for max_level parameter
   * Expected: max_level parameter listed with default '7'
   */
  test('TC5: max_level parameter listed with default 7', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Find the configuration table
    const configTable = configSection.locator('table');
    await expect(configTable).toBeVisible();

    // Verify max_level parameter is listed
    const maxLevelRow = configTable.locator('tr', { has: page.locator('code:text("max_level")') });
    await expect(maxLevelRow).toBeVisible();

    // Verify the default value is 7
    const maxLevelRowText = await maxLevelRow.textContent();
    // Check that the row contains the value 7 in a table cell context
    const defaultValueCell = maxLevelRow.locator('td').nth(1);
    const defaultValue = await defaultValueCell.textContent();
    expect(defaultValue.trim()).toBe('7');
  });

  /**
   * Test Case 6: TOML format documentation
   * Input: Verify TOML format documentation
   * Expected: Section mentions TOML as configuration file format
   */
  test('TC6: Section mentions TOML as configuration file format', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Verify TOML is mentioned in the configuration section
    const configSectionText = await configSection.textContent();
    expect(configSectionText.toLowerCase()).toContain('toml');
  });

  /**
   * Additional test: Configuration table has proper structure
   * Verify table has columns for parameter, default value, and description
   */
  test('Configuration table has proper structure with columns', async ({ page }) => {
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Find the configuration table
    const configTable = configSection.locator('table');
    await expect(configTable).toBeVisible();

    // Verify table headers
    const tableHeaders = configTable.locator('thead th');
    const headerCount = await tableHeaders.count();
    expect(headerCount).toBeGreaterThanOrEqual(3);

    // Verify header content
    const headerTexts = await tableHeaders.allTextContents();
    const headersLower = headerTexts.map(h => h.toLowerCase());

    expect(headersLower.some(h => h.includes('parameter'))).toBeTruthy();
    expect(headersLower.some(h => h.includes('default'))).toBeTruthy();
    expect(headersLower.some(h => h.includes('description'))).toBeTruthy();
  });

  /**
   * Additional test: Configuration section is accessible
   * Verify proper ARIA attributes and semantic structure
   */
  test('Configuration section has proper accessibility', async ({ page }) => {
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Verify section has proper role
    await expect(configSection).toHaveAttribute('role', 'region');

    // Verify section has aria-labelledby pointing to heading
    await expect(configSection).toHaveAttribute('aria-labelledby', 'config-heading');

    // Verify heading exists
    const heading = page.locator('#config-heading');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText('Configuration');
  });
});
