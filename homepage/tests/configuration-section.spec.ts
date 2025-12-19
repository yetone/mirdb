import { test, expect } from '@playwright/test';

test.describe('Configuration Options Display', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to the homepage
    await page.goto('file://' + process.cwd() + '/public/index.html');
  });

  test('TC1: Configuration section is visible with options displayed', async ({ page }) => {
    // Navigate to the configuration section by scrolling
    const configSection = page.getByTestId('configuration-section');
    await configSection.scrollIntoViewIfNeeded();

    // Verify configuration section is visible
    await expect(configSection).toBeVisible();

    // Verify the section has a proper heading
    const heading = configSection.locator('h2');
    await expect(heading).toContainText('Configuration');

    // Verify configuration options are displayed in a table
    const configTable = page.getByTestId('config-table');
    await expect(configTable).toBeVisible();

    // Verify table has headers
    const tableHeaders = configTable.locator('th');
    await expect(tableHeaders).toHaveCount(2);
    await expect(tableHeaders.first()).toContainText('Setting');
    await expect(tableHeaders.last()).toContainText('Default Value');
  });

  test('TC2: Listen Address setting is displayed with default value 0.0.0.0:12333', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.getByTestId('configuration-section');
    await configSection.scrollIntoViewIfNeeded();

    // Find the Listen Address row
    const listenAddressRow = page.getByTestId('config-listen-address');
    await expect(listenAddressRow).toBeVisible();

    // Verify the setting name
    const settingName = listenAddressRow.locator('td').first();
    await expect(settingName).toContainText('Listen Address');

    // Verify the default value
    const defaultValue = listenAddressRow.locator('td').last();
    await expect(defaultValue).toContainText('0.0.0.0:12333');
  });

  test('TC3: Work Directory setting is displayed with default value /tmp/mirdb', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.getByTestId('configuration-section');
    await configSection.scrollIntoViewIfNeeded();

    // Find the Work Directory row
    const workDirRow = page.getByTestId('config-work-directory');
    await expect(workDirRow).toBeVisible();

    // Verify the setting name
    const settingName = workDirRow.locator('td').first();
    await expect(settingName).toContainText('Work Directory');

    // Verify the default value
    const defaultValue = workDirRow.locator('td').last();
    await expect(defaultValue).toContainText('/tmp/mirdb');
  });

  test('TC4: Max LSM Levels setting is displayed with default value 7', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.getByTestId('configuration-section');
    await configSection.scrollIntoViewIfNeeded();

    // Find the Max LSM Levels row
    const maxLsmRow = page.getByTestId('config-max-lsm-levels');
    await expect(maxLsmRow).toBeVisible();

    // Verify the setting name
    const settingName = maxLsmRow.locator('td').first();
    await expect(settingName).toContainText('Max LSM Levels');

    // Verify the default value
    const defaultValue = maxLsmRow.locator('td').last();
    await expect(defaultValue).toContainText('7');
  });

  test('TC5: Memtable Size setting is displayed with default value 4MB', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.getByTestId('configuration-section');
    await configSection.scrollIntoViewIfNeeded();

    // Find the Memtable Size row
    const memtableRow = page.getByTestId('config-memtable-size');
    await expect(memtableRow).toBeVisible();

    // Verify the setting name
    const settingName = memtableRow.locator('td').first();
    await expect(settingName).toContainText('Memtable Size');

    // Verify the default value
    const defaultValue = memtableRow.locator('td').last();
    await expect(defaultValue).toContainText('4MB');
  });

  test('TC6: SSTable Size setting is displayed with default value 100MB', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.getByTestId('configuration-section');
    await configSection.scrollIntoViewIfNeeded();

    // Find the SSTable Size row
    const sstableRow = page.getByTestId('config-sstable-size');
    await expect(sstableRow).toBeVisible();

    // Verify the setting name
    const settingName = sstableRow.locator('td').first();
    await expect(settingName).toContainText('SSTable Size');

    // Verify the default value
    const defaultValue = sstableRow.locator('td').last();
    await expect(defaultValue).toContainText('100MB');
  });

  test('TC7: Block Size setting is displayed with default value 4KB', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.getByTestId('configuration-section');
    await configSection.scrollIntoViewIfNeeded();

    // Find the Block Size row
    const blockSizeRow = page.getByTestId('config-block-size');
    await expect(blockSizeRow).toBeVisible();

    // Verify the setting name
    const settingName = blockSizeRow.locator('td').first();
    await expect(settingName).toContainText('Block Size');

    // Verify the default value
    const defaultValue = blockSizeRow.locator('td').last();
    await expect(defaultValue).toContainText('4KB');
  });

  test('Configuration section is accessible via navigation link', async ({ page }) => {
    // Find the configuration navigation link
    const configLink = page.locator('a[href="#configuration"]');
    await expect(configLink).toBeVisible();

    // Click the configuration link
    await configLink.click();

    // Wait for scroll to complete
    await page.waitForTimeout(500);

    // Verify configuration section is now in view
    const configSection = page.getByTestId('configuration-section');
    await expect(configSection).toBeInViewport();
  });

  test('Configuration section is responsive on mobile viewport', async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('file://' + process.cwd() + '/public/index.html');

    // Navigate to configuration section
    const configSection = page.getByTestId('configuration-section');
    await configSection.scrollIntoViewIfNeeded();

    // Verify configuration section is visible
    await expect(configSection).toBeVisible();

    // Verify table is still visible
    const configTable = page.getByTestId('config-table');
    await expect(configTable).toBeVisible();

    // Verify section fits within mobile viewport (may scroll horizontally for table)
    const sectionBox = await configSection.boundingBox();
    expect(sectionBox).toBeTruthy();
    expect(sectionBox!.width).toBeLessThanOrEqual(375);
  });
});
