import { test, expect } from '@playwright/test';

test.describe('Configuration Reference Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Configuration parameters are displayed in a table with parameter name, description, and default value columns', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Check for configuration table
    const configTable = configSection.locator('[data-testid="config-table"]');
    await expect(configTable).toBeVisible();

    // Verify table has proper header columns
    const headerRow = configTable.locator('thead tr');
    await expect(headerRow).toBeVisible();

    // Check for Parameter column header
    const parameterHeader = headerRow.locator('th').filter({ hasText: /parameter/i });
    await expect(parameterHeader).toBeVisible();

    // Check for Description column header
    const descriptionHeader = headerRow.locator('th').filter({ hasText: /description/i });
    await expect(descriptionHeader).toBeVisible();

    // Check for Default column header
    const defaultHeader = headerRow.locator('th').filter({ hasText: /default/i });
    await expect(defaultHeader).toBeVisible();

    // Verify table has data rows
    const dataRows = configTable.locator('tbody tr');
    const rowCount = await dataRows.count();
    expect(rowCount).toBeGreaterThan(0);
  });

  test('TC2: memtable_size parameter is documented with description and default value', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    const configTable = configSection.locator('[data-testid="config-table"]');
    await expect(configTable).toBeVisible();

    // Find the row containing memtable_size or mem_table_max_size parameter
    const memtableRow = configTable.locator('tbody tr').filter({
      has: page.locator('code').filter({ hasText: /mem_table_max_size|memtable_size|memtable/i })
    });
    await expect(memtableRow).toBeVisible();

    // Verify it has a description (non-empty text in description cell)
    const cells = memtableRow.locator('td');
    const cellCount = await cells.count();
    expect(cellCount).toBeGreaterThanOrEqual(3);

    // Check that description cell is not empty
    const descriptionCell = cells.nth(2); // 3rd column (0-indexed)
    const descriptionText = await descriptionCell.textContent();
    expect(descriptionText?.trim().length).toBeGreaterThan(0);

    // Check that default value cell is not empty
    const defaultCell = cells.nth(1); // 2nd column
    const defaultText = await defaultCell.textContent();
    expect(defaultText?.trim().length).toBeGreaterThan(0);
  });

  test('TC3: Size unit formatting guide (K, M, G, T) is documented', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Check for size units guide section
    const sizeUnitsGuide = configSection.locator('[data-testid="size-units-guide"]');
    await expect(sizeUnitsGuide).toBeVisible();

    // Verify K = kilobytes is documented
    const content = await sizeUnitsGuide.textContent();
    expect(content?.toLowerCase()).toContain('k');
    expect(content?.toLowerCase()).toMatch(/kilobyte|kb|1024/i);

    // Verify M = megabytes is documented
    expect(content?.toLowerCase()).toContain('m');
    expect(content?.toLowerCase()).toMatch(/megabyte|mb/i);

    // Verify G = gigabytes is documented
    expect(content?.toLowerCase()).toContain('g');
    expect(content?.toLowerCase()).toMatch(/gigabyte|gb/i);

    // Verify T = terabytes is documented
    expect(content?.toLowerCase()).toContain('t');
    expect(content?.toLowerCase()).toMatch(/terabyte|tb/i);
  });

  test('TC4: Configuration examples show TOML format with proper syntax', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Check for TOML configuration example
    const tomlExample = configSection.locator('[data-testid="toml-example"]');
    await expect(tomlExample).toBeVisible();

    // Verify TOML content has proper syntax (key = value format)
    const tomlContent = await tomlExample.textContent();

    // Check for TOML-style key-value pairs
    expect(tomlContent).toMatch(/\w+\s*=\s*["']?[^"'\n]+["']?/);

    // Verify common TOML patterns are present
    expect(tomlContent).toMatch(/addr|work_dir|max_level/i);
  });

  test('Configuration section is accessible via navigation', async ({ page }) => {
    // Check that navigation link to configuration exists (use desktop nav which is visible by default on larger viewports)
    const navLink = page.locator('.nav-links a[href="#configuration"]');
    await expect(navLink).toBeVisible();

    // Click navigation link
    await navLink.click();

    // Verify configuration section is in viewport
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeInViewport();
  });
});
