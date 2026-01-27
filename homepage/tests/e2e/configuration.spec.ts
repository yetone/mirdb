/**
 * E2E tests for Configuration Section.
 * Owner: Scenario 15 - Configuration Display
 *
 * Tests:
 * - Configuration section loads and displays (REQ-7)
 * - Default configuration values are shown
 * - Listen address: 0.0.0.0:12333
 * - Memtable max size: 4MB
 * - SSTable max size: 100MB
 * - Block size: 4KB
 */

import { test, expect } from '@playwright/test';

test.describe('Configuration Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('configuration section is visible on page', async ({ page }) => {
    const configuration = page.locator('#configuration');
    await configuration.scrollIntoViewIfNeeded();
    await expect(configuration).toBeVisible();
  });

  test('section displays Configuration Options heading', async ({ page }) => {
    const configuration = page.locator('#configuration');
    await configuration.scrollIntoViewIfNeeded();

    const heading = configuration.locator('.section-title');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText('Configuration Options');
  });

  test('displays configuration description text', async ({ page }) => {
    const configuration = page.locator('#configuration');
    await configuration.scrollIntoViewIfNeeded();

    const description = configuration.locator('.config-description');
    await expect(description).toBeVisible();
    await expect(description).toContainText('TOML configuration files');
  });

  test('displays default listen address: 0.0.0.0:12333', async ({ page }) => {
    const configuration = page.locator('#configuration');
    await configuration.scrollIntoViewIfNeeded();

    // Find the config item for addr
    const addrItem = configuration.locator('[data-config-key="addr"]');
    await expect(addrItem).toBeVisible();

    // Check the default value
    const defaultValue = addrItem.locator('.config-default');
    await expect(defaultValue).toHaveText('0.0.0.0:12333');
  });

  test('displays default memtable max size: 4MB', async ({ page }) => {
    const configuration = page.locator('#configuration');
    await configuration.scrollIntoViewIfNeeded();

    // Find the config item for mem_table_max_size
    const memtableItem = configuration.locator('[data-config-key="mem_table_max_size"]');
    await expect(memtableItem).toBeVisible();

    // Check the default value
    const defaultValue = memtableItem.locator('.config-default');
    await expect(defaultValue).toHaveText('4MB');
  });

  test('displays default SSTable max size: 100MB', async ({ page }) => {
    const configuration = page.locator('#configuration');
    await configuration.scrollIntoViewIfNeeded();

    // Find the config item for sst_max_size
    const sstableItem = configuration.locator('[data-config-key="sst_max_size"]');
    await expect(sstableItem).toBeVisible();

    // Check the default value
    const defaultValue = sstableItem.locator('.config-default');
    await expect(defaultValue).toHaveText('100MB');
  });

  test('displays default block size: 4KB', async ({ page }) => {
    const configuration = page.locator('#configuration');
    await configuration.scrollIntoViewIfNeeded();

    // Find the config item for block_size
    const blockItem = configuration.locator('[data-config-key="block_size"]');
    await expect(blockItem).toBeVisible();

    // Check the default value
    const defaultValue = blockItem.locator('.config-default');
    await expect(defaultValue).toHaveText('4KB');
  });

  test('displays default max LSM levels: 7', async ({ page }) => {
    const configuration = page.locator('#configuration');
    await configuration.scrollIntoViewIfNeeded();

    // Find the config item for max_level
    const maxLevelItem = configuration.locator('[data-config-key="max_level"]');
    await expect(maxLevelItem).toBeVisible();

    // Check the default value
    const defaultValue = maxLevelItem.locator('.config-default');
    await expect(defaultValue).toHaveText('7');
  });

  test('each config item displays name, key, default value, and description', async ({ page }) => {
    const configuration = page.locator('#configuration');
    await configuration.scrollIntoViewIfNeeded();

    // Get all config items
    const configItems = configuration.locator('.config-item');
    const count = await configItems.count();
    expect(count).toBeGreaterThan(0);

    // Check the first config item has all required parts
    const firstItem = configItems.first();
    await expect(firstItem.locator('.config-name')).toBeVisible();
    await expect(firstItem.locator('.config-key')).toBeVisible();
    await expect(firstItem.locator('.config-default')).toBeVisible();
    await expect(firstItem.locator('.config-desc')).toBeVisible();
  });

  test('displays example configuration code block', async ({ page }) => {
    const configuration = page.locator('#configuration');
    await configuration.scrollIntoViewIfNeeded();

    const example = configuration.locator('.config-example');
    await expect(example).toBeVisible();

    const exampleTitle = configuration.locator('.config-example-title');
    await expect(exampleTitle).toHaveText('Example Configuration');

    const codeBlock = configuration.locator('.config-code');
    await expect(codeBlock).toBeVisible();
    await expect(codeBlock).toContainText('mirdb.toml');
  });

  test('section has accessible structure with proper roles', async ({ page }) => {
    const configuration = page.locator('#configuration');
    await configuration.scrollIntoViewIfNeeded();

    // Check section has aria-labelledby
    await expect(configuration).toHaveAttribute('aria-labelledby', 'configuration-title');

    // Check config grid has role="list"
    const configGrid = configuration.locator('.config-grid');
    await expect(configGrid).toHaveAttribute('role', 'list');
    await expect(configGrid).toHaveAttribute('aria-label', 'Configuration options');

    // Check config items have role="listitem"
    const configItems = configuration.locator('.config-item');
    const firstItem = configItems.first();
    await expect(firstItem).toHaveAttribute('role', 'listitem');
  });

  test('config items display in responsive grid layout', async ({ page }) => {
    const configuration = page.locator('#configuration');
    await configuration.scrollIntoViewIfNeeded();

    const configGrid = configuration.locator('.config-grid');
    await expect(configGrid).toBeVisible();

    const gridStyles = await configGrid.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        display: styles.display,
        gridTemplateColumns: styles.gridTemplateColumns
      };
    });

    expect(gridStyles.display).toBe('grid');
  });
});

test.describe('Configuration Section Mobile', () => {
  test.use({
    viewport: { width: 375, height: 667 }
  });

  test('configuration section is visible on mobile', async ({ page }) => {
    await page.goto('/');
    const configuration = page.locator('#configuration');
    await configuration.scrollIntoViewIfNeeded();
    await expect(configuration).toBeVisible();
  });

  test('config items stack vertically on mobile', async ({ page }) => {
    await page.goto('/');
    const configuration = page.locator('#configuration');
    await configuration.scrollIntoViewIfNeeded();

    const configGrid = configuration.locator('.config-grid');
    const gridStyles = await configGrid.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return styles.gridTemplateColumns;
    });

    // On mobile, should be single column
    const columnCount = gridStyles.split(' ').filter(col => col.trim() !== '' && col !== '0px').length;
    expect(columnCount).toBeLessThanOrEqual(1);
  });
});
