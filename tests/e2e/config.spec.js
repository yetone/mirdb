/**
 * E2E Tests: Configuration Reference
 * Owner: Scenario 7 - Configuration Reference
 *
 * Test coverage:
 * - Configuration table/list is present
 * - Key parameters (addr, work_dir, mem_table_max_size, etc.) are listed
 * - Default values are shown
 * - Link to full documentation is present and valid
 * - Parameters are organized by category (Network, Storage, Memory Tables, SSTables, Compaction)
 */

const { test, expect } = require('@playwright/test');

test.describe('Configuration Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.evaluate(() => {
      const section = document.getElementById('configuration');
      if (section) section.scrollIntoView({ behavior: 'instant' });
    });
  });

  test('configuration section heading is present', async ({ page }) => {
    const heading = page.locator('section#configuration h2');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText(/Configuration/i);
  });

  test('configuration section has introductory text', async ({ page }) => {
    const intro = page.locator('section#configuration .section-intro');
    await expect(intro).toBeVisible();
    await expect(intro).toContainText(/configurable|tune|parameter/i);
  });

  test('configuration parameters are organized in categories', async ({ page }) => {
    const categories = page.locator('section#configuration .config-category');
    await expect(categories).toHaveCount(5);

    const headings = page.locator('section#configuration .config-category h3');
    const texts = await headings.allTextContents();
    expect(texts).toContain('Network');
    expect(texts).toContain('Storage');
    expect(texts).toContain('Memory Tables');
    expect(texts).toContain('SSTables');
    expect(texts).toContain('Compaction');
  });

  test('each category has a configuration table with headers', async ({ page }) => {
    const tables = page.locator('section#configuration .config-table');
    const count = await tables.count();
    expect(count).toBeGreaterThanOrEqual(1);

    for (let i = 0; i < count; i++) {
      const headers = tables.nth(i).locator('thead th');
      await expect(headers.nth(0)).toHaveText(/Parameter/i);
      await expect(headers.nth(1)).toHaveText(/Description/i);
      await expect(headers.nth(2)).toHaveText(/Default/i);
    }
  });

  test('addr parameter is listed with default 0.0.0.0:12333 and description', async ({ page }) => {
    const table = page.locator('section#configuration .config-category:has-text("Network") .config-table');
    const row = table.locator('tbody tr:has-text("addr")');
    await expect(row).toBeVisible();
    await expect(row).toContainText('0.0.0.0:12333');
    await expect(row).toContainText(/address|port|listen/i);
  });

  test('work_dir parameter is listed with default and description', async ({ page }) => {
    const table = page.locator('section#configuration .config-category:has-text("Storage") .config-table');
    const row = table.locator('tbody tr:has-text("work_dir")');
    await expect(row).toBeVisible();
    await expect(row).toContainText('/tmp/mirdb');
    await expect(row).toContainText(/directory|path|data/i);
  });

  test('max_level parameter is listed with default 7 and description', async ({ page }) => {
    const table = page.locator('section#configuration .config-category:has-text("Storage") .config-table');
    const row = table.locator('tbody tr:has-text("max_level")');
    await expect(row).toBeVisible();
    await expect(row).toContainText('7');
    await expect(row).toContainText(/level|LSM|tree/i);
  });

  test('mem_table_max_size is listed with default 4M and description', async ({ page }) => {
    const table = page.locator('section#configuration .config-category:has-text("Memory Tables") .config-table');
    const row = table.locator('tbody tr:has-text("mem_table_max_size")');
    await expect(row).toBeVisible();
    await expect(row).toContainText(/4M|4194304/);
    await expect(row).toContainText(/memtable|flush|size/i);
  });

  test('mem_table_max_height is listed with default 32 and description', async ({ page }) => {
    const table = page.locator('section#configuration .config-category:has-text("Memory Tables") .config-table');
    const row = table.locator('tbody tr:has-text("mem_table_max_height")');
    await expect(row).toBeVisible();
    await expect(row).toContainText('32');
    await expect(row).toContainText(/skip list|height|index/i);
  });

  test('imm_mem_table_max_count is listed with default 16 and description', async ({ page }) => {
    const table = page.locator('section#configuration .config-category:has-text("Memory Tables") .config-table');
    const row = table.locator('tbody tr:has-text("imm_mem_table_max_count")');
    await expect(row).toBeVisible();
    await expect(row).toContainText('16');
    await expect(row).toContainText(/immutable|queue|memtable/i);
  });

  test('sst_max_size is listed with default 100M and description', async ({ page }) => {
    const table = page.locator('section#configuration .config-category:has-text("SSTables") .config-table');
    const row = table.locator('tbody tr:has-text("sst_max_size")');
    await expect(row).toBeVisible();
    await expect(row).toContainText(/100M|104857600/);
    await expect(row).toContainText(/SSTable|file|size/i);
  });

  test('block_size is listed with default 4K and description', async ({ page }) => {
    const table = page.locator('section#configuration .config-category:has-text("SSTables") .config-table');
    const row = table.locator('tbody tr:has-text("block_size")');
    await expect(row).toBeVisible();
    await expect(row).toContainText(/4K|4096/);
    await expect(row).toContainText(/block|data/i);
  });

  test('block_restart_interval is listed with default 16 and description', async ({ page }) => {
    const table = page.locator('section#configuration .config-category:has-text("SSTables") .config-table');
    const row = table.locator('tbody tr:has-text("block_restart_interval")');
    await expect(row).toBeVisible();
    await expect(row).toContainText('16');
    await expect(row).toContainText(/restart|interval|prefix|compression/i);
  });

  test('l0_compaction_trigger is listed with default 4 and description', async ({ page }) => {
    const table = page.locator('section#configuration .config-category:has-text("Compaction") .config-table');
    const row = table.locator('tbody tr:has-text("l0_compaction_trigger")');
    await expect(row).toBeVisible();
    await expect(row).toContainText('4');
    await expect(row).toContainText(/Level 0|compaction|trigger/i);
  });

  test('thread_sleep_ms is listed with default 500 and description', async ({ page }) => {
    const table = page.locator('section#configuration .config-category:has-text("Compaction") .config-table');
    const row = table.locator('tbody tr:has-text("thread_sleep_ms")');
    await expect(row).toBeVisible();
    await expect(row).toContainText('500');
    await expect(row).toContainText(/background|thread|sleep|interval/i);
  });

  test('link to full configuration documentation is present', async ({ page }) => {
    const docsLink = page.locator('section#configuration .config-docs-link a');
    await expect(docsLink).toBeVisible();
    await expect(docsLink).toContainText(/full configuration|documentation|configuration reference/i);
  });

  test('documentation link has a valid href', async ({ page }) => {
    const docsLink = page.locator('section#configuration .config-docs-link a');
    const href = await docsLink.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href).toMatch(/github\.com|documentation|config/i);
  });

  test('size suffix notation is explained', async ({ page }) => {
    const noteBox = page.locator('section#configuration .config-note-box');
    await expect(noteBox).toBeVisible();
    await expect(noteBox).toContainText(/K|kilobytes/i);
    await expect(noteBox).toContainText(/M|megabytes/i);
  });

  test('configuration tables use semantic HTML', async ({ page }) => {
    const tables = page.locator('section#configuration .config-table');
    const count = await tables.count();
    expect(count).toBeGreaterThanOrEqual(1);

    for (let i = 0; i < count; i++) {
      const thead = tables.nth(i).locator('thead');
      const tbody = tables.nth(i).locator('tbody');
      await expect(thead).toBeVisible();
      await expect(tbody).toBeVisible();

      const headerCells = tables.nth(i).locator('thead th');
      await expect(headerCells.nth(0)).toHaveAttribute('scope', 'col');
    }
  });

  test('configuration section is visible on mobile viewport', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 });
    const section = page.locator('section#configuration');
    await expect(section).toBeVisible();
    const heading = section.locator('h2');
    await expect(heading).toBeVisible();
    const tables = section.locator('.config-table');
    await expect(tables.first()).toBeVisible();
  });
});
