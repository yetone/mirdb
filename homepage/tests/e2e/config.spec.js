/**
 * Configuration Section E2E Tests
 * Owner: Scenario 5 - Configuration Reference Section
 *
 * Tests:
 * - Configuration options documented
 * - Default values shown
 * - addr, work_dir, mem_table_max_size, etc.
 *
 * Requirements: REQ-5
 */

const { test, expect } = require('@playwright/test');

test.describe('Configuration Reference Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('configuration section is visible and has proper structure', async ({ page }) => {
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    const sectionTitle = configSection.locator('#configuration-title');
    await expect(sectionTitle).toHaveText('Configuration Reference');
  });

  test('addr configuration option is documented with default 0.0.0.0:12333', async ({ page }) => {
    const configSection = page.locator('#configuration');
    const addrRow = configSection.locator('tr[data-config="addr"]');

    await expect(addrRow).toBeVisible();

    const optionName = addrRow.locator('td').first();
    await expect(optionName).toContainText('addr');

    const defaultValue = addrRow.locator('td').nth(1);
    await expect(defaultValue).toContainText('0.0.0.0:12333');

    const description = addrRow.locator('td').nth(2);
    await expect(description).toContainText('Listen address');
  });

  test('work_dir configuration option is documented', async ({ page }) => {
    const configSection = page.locator('#configuration');
    const workDirRow = configSection.locator('tr[data-config="work_dir"]');

    await expect(workDirRow).toBeVisible();

    const optionName = workDirRow.locator('td').first();
    await expect(optionName).toContainText('work_dir');

    const defaultValue = workDirRow.locator('td').nth(1);
    await expect(defaultValue).toContainText('/tmp/mirdb');

    const description = workDirRow.locator('td').nth(2);
    await expect(description).toContainText('Working directory');
  });

  test('mem_table_max_size configuration option is documented', async ({ page }) => {
    const configSection = page.locator('#configuration');
    const memTableRow = configSection.locator('tr[data-config="mem_table_max_size"]');

    await expect(memTableRow).toBeVisible();

    const optionName = memTableRow.locator('td').first();
    await expect(optionName).toContainText('mem_table_max_size');

    const defaultValue = memTableRow.locator('td').nth(1);
    await expect(defaultValue).toContainText('4M');

    const description = memTableRow.locator('td').nth(2);
    await expect(description).toContainText('memtable');
  });

  test('sst_max_size configuration option is documented', async ({ page }) => {
    const configSection = page.locator('#configuration');
    const sstRow = configSection.locator('tr[data-config="sst_max_size"]');

    await expect(sstRow).toBeVisible();

    const optionName = sstRow.locator('td').first();
    await expect(optionName).toContainText('sst_max_size');

    const defaultValue = sstRow.locator('td').nth(1);
    await expect(defaultValue).toContainText('100M');

    const description = sstRow.locator('td').nth(2);
    await expect(description).toContainText('SSTable');
  });

  test('l0_compaction_trigger and max_level configuration options are documented', async ({ page }) => {
    const configSection = page.locator('#configuration');

    // Check l0_compaction_trigger
    const l0Row = configSection.locator('tr[data-config="l0_compaction_trigger"]');
    await expect(l0Row).toBeVisible();

    const l0Option = l0Row.locator('td').first();
    await expect(l0Option).toContainText('l0_compaction_trigger');

    const l0Default = l0Row.locator('td').nth(1);
    await expect(l0Default).toContainText('4');

    const l0Description = l0Row.locator('td').nth(2);
    await expect(l0Description).toContainText('compaction');

    // Check max_level
    const maxLevelRow = configSection.locator('tr[data-config="max_level"]');
    await expect(maxLevelRow).toBeVisible();

    const maxLevelOption = maxLevelRow.locator('td').first();
    await expect(maxLevelOption).toContainText('max_level');

    const maxLevelDefault = maxLevelRow.locator('td').nth(1);
    await expect(maxLevelDefault).toContainText('7');

    const maxLevelDescription = maxLevelRow.locator('td').nth(2);
    await expect(maxLevelDescription).toContainText('level');
  });

  test('configuration table has all expected configuration options', async ({ page }) => {
    const configSection = page.locator('#configuration');
    const configTable = configSection.locator('[data-testid="config-table"]');

    await expect(configTable).toBeVisible();

    // Verify all expected configuration options are present
    const expectedOptions = [
      'addr',
      'work_dir',
      'mem_table_max_size',
      'sst_max_size',
      'max_level',
      'l0_compaction_trigger',
      'mem_table_max_height',
      'imm_mem_table_max_count',
      'block_size',
      'block_restart_interval',
      'thread_sleep_ms'
    ];

    for (const option of expectedOptions) {
      const row = configTable.locator(`tr[data-config="${option}"]`);
      await expect(row).toBeVisible();
    }
  });

  test('configuration example code block is present', async ({ page }) => {
    const configSection = page.locator('#configuration');
    const codeBlock = configSection.locator('.config-example pre code');

    await expect(codeBlock).toBeVisible();

    // Verify the code block contains key configuration values
    const codeContent = await codeBlock.textContent();
    expect(codeContent).toContain('addr = "0.0.0.0:12333"');
    expect(codeContent).toContain('work_dir');
    expect(codeContent).toContain('mem_table_max_size');
    expect(codeContent).toContain('sst_max_size');
  });

  test('configuration section has accessible table structure', async ({ page }) => {
    const configSection = page.locator('#configuration');

    // Check table has proper header row
    const tableHeaders = configSection.locator('.config-table thead th');
    await expect(tableHeaders).toHaveCount(3);
    await expect(tableHeaders.nth(0)).toHaveText('Option');
    await expect(tableHeaders.nth(1)).toHaveText('Default');
    await expect(tableHeaders.nth(2)).toHaveText('Description');

    // Check table wrapper has role and label for accessibility
    const tableWrapper = configSection.locator('.config-table-wrapper');
    await expect(tableWrapper).toHaveAttribute('role', 'region');
    await expect(tableWrapper).toHaveAttribute('aria-label', 'Configuration options table');
  });

  test('configuration section can be navigated to via anchor', async ({ page }) => {
    // Navigate directly to the configuration section via hash
    await page.goto('/#configuration');

    const configSection = page.locator('#configuration');
    await expect(configSection).toBeInViewport();
  });
});
