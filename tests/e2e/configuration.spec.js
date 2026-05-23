/**
 * E2E tests for Configuration Section.
 * Owner: Scenario 6 - Configuration Section
 *
 * Tests:
 * - Configuration section renders with correct heading
 * - Default configuration values are displayed correctly
 * - Table structure is used for semantic markup
 * - All additional config values are present
 */

const { test, expect } = require('@playwright/test');

const filePath = 'file://' + process.cwd() + '/index.html';

test.describe('Configuration Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(filePath);
  });

  test('Configuration section exists with h2 heading "Configuration"', async ({ page }) => {
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    const heading = configSection.locator('h2');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText('Configuration');
  });

  test('listen address default is shown as 0.0.0.0:12333', async ({ page }) => {
    const row = page.locator('[data-testid="config-addr"]');
    await expect(row).toBeVisible();

    const rowText = await row.textContent();
    expect(rowText).toContain('0.0.0.0:12333');
  });

  test('max_level default is shown as 7', async ({ page }) => {
    const row = page.locator('[data-testid="config-max-level"]');
    await expect(row).toBeVisible();

    const rowText = await row.textContent();
    expect(rowText).toContain('7');
  });

  test('work_dir default is shown as /tmp/mirdb', async ({ page }) => {
    const row = page.locator('[data-testid="config-work-dir"]');
    await expect(row).toBeVisible();

    const rowText = await row.textContent();
    expect(rowText).toContain('/tmp/mirdb');
  });

  test('sst_max_size default is shown as 100MB', async ({ page }) => {
    const row = page.locator('[data-testid="config-sst-max-size"]');
    await expect(row).toBeVisible();

    const rowText = await row.textContent();
    expect(rowText).toContain('100MB');
  });

  test('mem_table_max_size default is shown as 4MB', async ({ page }) => {
    const row = page.locator('[data-testid="config-mem-table-max-size"]');
    await expect(row).toBeVisible();

    const rowText = await row.textContent();
    expect(rowText).toContain('4MB');
  });

  test('block_size default is shown as 4KB', async ({ page }) => {
    const row = page.locator('[data-testid="config-block-size"]');
    await expect(row).toBeVisible();

    const rowText = await row.textContent();
    expect(rowText).toContain('4KB');
  });

  test('configuration uses table element for semantic structure', async ({ page }) => {
    const configSection = page.locator('#configuration');
    const table = configSection.locator('table.config-table');
    await expect(table).toBeVisible();

    const tagName = await table.evaluate(el => el.tagName.toLowerCase());
    expect(tagName).toBe('table');

    const thead = table.locator('thead');
    await expect(thead).toBeVisible();

    const tbody = table.locator('tbody');
    await expect(tbody).toBeVisible();
  });

  test('all additional config values are present', async ({ page }) => {
    const configSection = page.locator('#configuration');
    const tableText = await configSection.locator('table.config-table').textContent();

    expect(tableText).toContain('mem_table_max_height');
    expect(tableText).toContain('32');
    expect(tableText).toContain('imm_mem_table_max_count');
    expect(tableText).toContain('16');
    expect(tableText).toContain('block_restart_interval');
    expect(tableText).toContain('l0_compaction_trigger');
    expect(tableText).toContain('4');
    expect(tableText).toContain('thread_sleep_ms');
    expect(tableText).toContain('500');
  });
});
