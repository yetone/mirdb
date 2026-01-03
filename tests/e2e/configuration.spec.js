// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for MirDB Homepage Configuration Overview Display
 * Scenario: Configuration Overview Display
 * Tests verify the homepage displays default configuration settings
 */

test.describe('Configuration Overview Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Search for configuration section
   * Expected: Page contains configuration or settings information
   */
  test('TC1: page contains configuration or settings section', async ({ page }) => {
    // Navigate to the config section
    const configSection = page.locator('#config, .config, section:has-text("Configuration")');
    await expect(configSection).toBeVisible();

    // Check that the section has a heading indicating configuration
    const configHeading = configSection.locator('h2');
    await expect(configHeading).toBeVisible();

    const headingText = await configHeading.textContent();
    expect(headingText?.toLowerCase()).toMatch(/(config|settings|default)/i);

    // Verify there's a table or list with configuration parameters
    const configTable = configSection.locator('table, .config-table');
    await expect(configTable).toBeVisible();
  });

  /**
   * Test Case 2: Check for listen address configuration
   * Expected: Default listen address 0.0.0.0:12333 is mentioned
   */
  test('TC2: default listen address 0.0.0.0:12333 is mentioned', async ({ page }) => {
    const configSection = page.locator('#config, .config');
    await expect(configSection).toBeVisible();

    // Look for the listen address in the configuration section
    const sectionContent = await configSection.textContent();

    // Check for the default listen address
    expect(sectionContent).toContain('0.0.0.0:12333');

    // Also verify there's a row/entry for listen address
    const listenAddressRow = configSection.locator('tr:has-text("Listen"), tr:has-text("Address")');
    await expect(listenAddressRow).toBeVisible();

    const rowText = await listenAddressRow.textContent();
    expect(rowText).toContain('0.0.0.0:12333');
  });

  /**
   * Test Case 3: Check for storage configuration
   * Expected: SSTable or memtable size configurations are mentioned
   */
  test('TC3: SSTable or memtable size configurations are mentioned', async ({ page }) => {
    const configSection = page.locator('#config, .config');
    await expect(configSection).toBeVisible();

    const sectionContent = await configSection.textContent();

    // Check for SSTable configuration
    const hasSSTableConfig = /sstable/i.test(sectionContent) || /100\s*MB/i.test(sectionContent);

    // Check for Memtable configuration
    const hasMemtableConfig = /memtable/i.test(sectionContent) || /4\s*MB/i.test(sectionContent);

    // At least one of SSTable or Memtable should be mentioned
    expect(hasSSTableConfig || hasMemtableConfig).toBeTruthy();

    // Verify specific size values exist
    // SSTable Max Size should be 100MB
    const hasSSTableSize = configSection.locator('td:has-text("100MB"), td:has-text("100 MB")');
    await expect(hasSSTableSize).toBeVisible();

    // Memtable Max Size should be 4MB
    const hasMemtableSize = configSection.locator('td:has-text("4MB"), td:has-text("4 MB")');
    await expect(hasMemtableSize).toBeVisible();
  });

  /**
   * Test Case 4: Check for work directory configuration
   * Expected: Default work directory /tmp/mirdb is mentioned
   */
  test('TC4: default work directory /tmp/mirdb is mentioned', async ({ page }) => {
    const configSection = page.locator('#config, .config');
    await expect(configSection).toBeVisible();

    // Look for the work directory in the configuration section
    const sectionContent = await configSection.textContent();

    // Check for the default work directory path
    expect(sectionContent).toContain('/tmp/mirdb');

    // Verify there's a row/entry for work directory
    const workDirRow = configSection.locator('tr:has-text("Work"), tr:has-text("Directory")');
    await expect(workDirRow).toBeVisible();

    const rowText = await workDirRow.textContent();
    expect(rowText).toContain('/tmp/mirdb');
  });

  /**
   * Additional test: Verify the configuration table structure
   */
  test('configuration table has proper structure with headers', async ({ page }) => {
    const configSection = page.locator('#config, .config');
    await expect(configSection).toBeVisible();

    // Check for table headers
    const tableHeaders = configSection.locator('thead th, th');
    const headerCount = await tableHeaders.count();
    expect(headerCount).toBeGreaterThanOrEqual(2);

    // Verify Parameter and Value headers exist
    const allHeaders = await tableHeaders.allTextContents();
    const joinedHeaders = allHeaders.join(' ').toLowerCase();
    expect(joinedHeaders).toMatch(/parameter/i);
    expect(joinedHeaders).toMatch(/value|default/i);
  });

  /**
   * Additional test: Verify all expected configuration parameters are present
   */
  test('all expected configuration parameters are displayed', async ({ page }) => {
    const configSection = page.locator('#config, .config');
    await expect(configSection).toBeVisible();

    const sectionContent = await configSection.textContent();

    // Check for all expected parameters based on PRD
    const expectedParams = [
      'Listen Address',
      'LSM',
      'Work Directory',
      'SSTable',
      'Memtable',
      'Block Size'
    ];

    for (const param of expectedParams) {
      expect(sectionContent?.toLowerCase()).toContain(param.toLowerCase());
    }
  });
});
