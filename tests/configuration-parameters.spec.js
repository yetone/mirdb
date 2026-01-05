// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Configuration Parameters Display Tests (REQ-6)
 *
 * This test suite verifies that default configuration parameters are displayed
 * on the landing page as specified in the product requirements document.
 */

test.describe('Configuration Parameters Display (REQ-6)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 5: Configuration Section Exists
   * Verifies that a configuration or parameters section is present on the page.
   */
  test('TC5: Configuration section is present on the page', async ({ page }) => {
    // Locate the configuration section
    const configSection = page.locator('[data-testid="configuration-section"]');
    await expect(configSection).toBeVisible();

    // Verify section has a heading
    const heading = configSection.locator('.section-title');
    await expect(heading).toBeVisible();
    await expect(heading).toContainText(/Configuration/i);
  });

  /**
   * Test Case 1: Listen Address Configuration
   * Verifies that the default listen address (0.0.0.0:12333) is displayed.
   */
  test('TC1: Default listen address (0.0.0.0:12333) is displayed', async ({ page }) => {
    // Locate the configuration section
    const configSection = page.locator('[data-testid="configuration-section"]');
    await expect(configSection).toBeVisible();

    // Find the listen address config item
    const listenAddressItem = page.locator('[data-testid="config-listen-address"]');
    await expect(listenAddressItem).toBeVisible();

    // Verify the value contains the default listen address
    const listenAddressValue = page.locator('[data-testid="config-value-listen-address"]');
    await expect(listenAddressValue).toBeVisible();
    await expect(listenAddressValue).toHaveText('0.0.0.0:12333');
  });

  /**
   * Test Case 2: Max LSM Levels Configuration
   * Verifies that max LSM levels (7) is mentioned in configuration.
   */
  test('TC2: Max LSM levels (7) is mentioned in configuration', async ({ page }) => {
    // Locate the configuration section
    const configSection = page.locator('[data-testid="configuration-section"]');
    await expect(configSection).toBeVisible();

    // Find the LSM levels config item
    const lsmLevelsItem = page.locator('[data-testid="config-max-lsm-levels"]');
    await expect(lsmLevelsItem).toBeVisible();

    // Verify the value contains 7
    const lsmLevelsValue = page.locator('[data-testid="config-value-max-lsm-levels"]');
    await expect(lsmLevelsValue).toBeVisible();
    await expect(lsmLevelsValue).toHaveText('7');
  });

  /**
   * Test Case 3: SSTable Max Size Configuration
   * Verifies that SSTable max size (100MB) is mentioned.
   */
  test('TC3: SSTable max size (100MB) is mentioned', async ({ page }) => {
    // Locate the configuration section
    const configSection = page.locator('[data-testid="configuration-section"]');
    await expect(configSection).toBeVisible();

    // Find the SSTable max size config item
    const sstableItem = page.locator('[data-testid="config-sstable-max-size"]');
    await expect(sstableItem).toBeVisible();

    // Verify the value contains 100MB
    const sstableValue = page.locator('[data-testid="config-value-sstable-max-size"]');
    await expect(sstableValue).toBeVisible();
    await expect(sstableValue).toHaveText('100MB');
  });

  /**
   * Test Case 4: Memtable Max Size Configuration
   * Verifies that memtable max size (4MB) is mentioned.
   */
  test('TC4: Memtable max size (4MB) is mentioned', async ({ page }) => {
    // Locate the configuration section
    const configSection = page.locator('[data-testid="configuration-section"]');
    await expect(configSection).toBeVisible();

    // Find the memtable max size config item
    const memtableItem = page.locator('[data-testid="config-memtable-max-size"]');
    await expect(memtableItem).toBeVisible();

    // Verify the value contains 4MB
    const memtableValue = page.locator('[data-testid="config-value-memtable-max-size"]');
    await expect(memtableValue).toBeVisible();
    await expect(memtableValue).toHaveText('4MB');
  });

  /**
   * Additional test: Block Size Configuration
   * Verifies that block size (4KB) is displayed.
   */
  test('Block size (4KB) is displayed in configuration', async ({ page }) => {
    // Locate the configuration section
    const configSection = page.locator('[data-testid="configuration-section"]');
    await expect(configSection).toBeVisible();

    // Find the block size config item
    const blockSizeItem = page.locator('[data-testid="config-block-size"]');
    await expect(blockSizeItem).toBeVisible();

    // Verify the value contains 4KB
    const blockSizeValue = page.locator('[data-testid="config-value-block-size"]');
    await expect(blockSizeValue).toBeVisible();
    await expect(blockSizeValue).toHaveText('4KB');
  });

  /**
   * Additional test: All config items have labels and values
   * Verifies that each configuration item has both a label and a value.
   */
  test('All configuration items have labels and values', async ({ page }) => {
    // Locate the configuration section
    const configSection = page.locator('[data-testid="configuration-section"]');
    await expect(configSection).toBeVisible();

    // Find all config items
    const configItems = configSection.locator('.config-item');
    const count = await configItems.count();

    // Verify there are at least 5 config items (the required ones)
    expect(count).toBeGreaterThanOrEqual(5);

    // Verify each config item has a label and value
    for (let i = 0; i < count; i++) {
      const item = configItems.nth(i);
      const label = item.locator('.config-label');
      const value = item.locator('.config-value');

      await expect(label).toBeVisible();
      await expect(value).toBeVisible();

      // Verify label and value are not empty
      const labelText = await label.textContent();
      const valueText = await value.textContent();

      expect(labelText?.trim().length).toBeGreaterThan(0);
      expect(valueText?.trim().length).toBeGreaterThan(0);
    }
  });
});
