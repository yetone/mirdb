// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Test Suite: Configuration Documentation
 * Scenario: Verify configuration parameters are documented with descriptions and defaults
 */

test.describe('Configuration Documentation', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to the homepage before each test
    await page.goto('/');
  });

  /**
   * Test Case 1: Check configuration section for addr parameter
   * Input: Check configuration section for addr parameter
   * Expected: addr parameter documented with default '0.0.0.0:12333'
   */
  test('TC1: addr parameter documented with default 0.0.0.0:12333', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('#configuration');
    await configSection.scrollIntoViewIfNeeded();
    await expect(configSection).toBeVisible();

    // Check for the configuration parameters table
    const configTable = configSection.locator('.config-params-table');
    await expect(configTable).toBeVisible();

    // Find the row for addr parameter
    const addrRow = configTable.locator('[data-param="addr"]');
    await expect(addrRow).toBeVisible();

    // Verify the parameter name
    const paramName = addrRow.locator('.param-name');
    await expect(paramName).toContainText('addr');

    // Verify the default value
    const defaultValue = addrRow.locator('.param-default');
    await expect(defaultValue).toContainText('0.0.0.0:12333');

    // Verify description exists
    const description = addrRow.locator('.param-description');
    await expect(description).toBeVisible();
  });

  /**
   * Test Case 2: Check configuration section for max_level parameter
   * Input: Check configuration section for max_level parameter
   * Expected: max_level parameter documented with default '7'
   */
  test('TC2: max_level parameter documented with default 7', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('#configuration');
    await configSection.scrollIntoViewIfNeeded();
    await expect(configSection).toBeVisible();

    // Check for the configuration parameters table
    const configTable = configSection.locator('.config-params-table');
    await expect(configTable).toBeVisible();

    // Find the row for max_level parameter
    const maxLevelRow = configTable.locator('[data-param="max_level"]');
    await expect(maxLevelRow).toBeVisible();

    // Verify the parameter name
    const paramName = maxLevelRow.locator('.param-name');
    await expect(paramName).toContainText('max_level');

    // Verify the default value
    const defaultValue = maxLevelRow.locator('.param-default');
    await expect(defaultValue).toContainText('7');

    // Verify description exists
    const description = maxLevelRow.locator('.param-description');
    await expect(description).toBeVisible();
  });

  /**
   * Test Case 3: Check configuration section for work_dir parameter
   * Input: Check configuration section for work_dir parameter
   * Expected: work_dir parameter documented with default '/tmp/mirdb'
   */
  test('TC3: work_dir parameter documented with default /tmp/mirdb', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('#configuration');
    await configSection.scrollIntoViewIfNeeded();
    await expect(configSection).toBeVisible();

    // Check for the configuration parameters table
    const configTable = configSection.locator('.config-params-table');
    await expect(configTable).toBeVisible();

    // Find the row for work_dir parameter
    const workDirRow = configTable.locator('[data-param="work_dir"]');
    await expect(workDirRow).toBeVisible();

    // Verify the parameter name
    const paramName = workDirRow.locator('.param-name');
    await expect(paramName).toContainText('work_dir');

    // Verify the default value
    const defaultValue = workDirRow.locator('.param-default');
    await expect(defaultValue).toContainText('/tmp/mirdb');

    // Verify description exists
    const description = workDirRow.locator('.param-description');
    await expect(description).toBeVisible();
  });

  /**
   * Test Case 4: Check configuration section for size parameters
   * Input: Check configuration section for size parameters
   * Expected: sst_max_size, mem_table_max_size, block_size parameters documented with defaults
   */
  test('TC4: size parameters (sst_max_size, mem_table_max_size, block_size) documented with defaults', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('#configuration');
    await configSection.scrollIntoViewIfNeeded();
    await expect(configSection).toBeVisible();

    // Check for the configuration parameters table
    const configTable = configSection.locator('.config-params-table');
    await expect(configTable).toBeVisible();

    // Verify sst_max_size parameter
    const sstMaxSizeRow = configTable.locator('[data-param="sst_max_size"]');
    await expect(sstMaxSizeRow).toBeVisible();
    await expect(sstMaxSizeRow.locator('.param-name')).toContainText('sst_max_size');
    await expect(sstMaxSizeRow.locator('.param-default')).toContainText('100M');
    await expect(sstMaxSizeRow.locator('.param-description')).toBeVisible();

    // Verify mem_table_max_size parameter
    const memTableMaxSizeRow = configTable.locator('[data-param="mem_table_max_size"]');
    await expect(memTableMaxSizeRow).toBeVisible();
    await expect(memTableMaxSizeRow.locator('.param-name')).toContainText('mem_table_max_size');
    await expect(memTableMaxSizeRow.locator('.param-default')).toContainText('4M');
    await expect(memTableMaxSizeRow.locator('.param-description')).toBeVisible();

    // Verify block_size parameter
    const blockSizeRow = configTable.locator('[data-param="block_size"]');
    await expect(blockSizeRow).toBeVisible();
    await expect(blockSizeRow.locator('.param-name')).toContainText('block_size');
    await expect(blockSizeRow.locator('.param-default')).toContainText('4K');
    await expect(blockSizeRow.locator('.param-description')).toBeVisible();
  });

  /**
   * Test Case 5: Verify configuration example in TOML format
   * Input: Verify configuration example in TOML format
   * Expected: Complete TOML configuration example is displayed with syntax highlighting
   */
  test('TC5: Complete TOML configuration example with syntax highlighting', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('#configuration');
    await configSection.scrollIntoViewIfNeeded();
    await expect(configSection).toBeVisible();

    // Find the TOML code block
    const codeBlock = configSection.locator('.code-block[data-language="toml"]');
    await expect(codeBlock).toBeVisible();

    // Verify the code element has the language-toml class for syntax highlighting
    const codeElement = codeBlock.locator('code.language-toml');
    await expect(codeElement).toBeVisible();

    // Get the TOML content
    const tomlContent = await codeElement.textContent();

    // Verify all required configuration parameters are present in the example
    expect(tomlContent).toContain('addr');
    expect(tomlContent).toContain('0.0.0.0:12333');
    expect(tomlContent).toContain('max_level');
    expect(tomlContent).toContain('7');
    expect(tomlContent).toContain('work_dir');
    expect(tomlContent).toContain('/tmp/mirdb');
    expect(tomlContent).toContain('sst_max_size');
    expect(tomlContent).toContain('100M');
    expect(tomlContent).toContain('mem_table_max_size');
    expect(tomlContent).toContain('4M');
    expect(tomlContent).toContain('block_size');
    expect(tomlContent).toContain('4K');

    // Verify the code block uses monospace font
    const fontFamily = await codeElement.evaluate((el) => {
      return window.getComputedStyle(el).fontFamily;
    });

    const isMonospace = fontFamily.toLowerCase().includes('mono') ||
                       fontFamily.toLowerCase().includes('courier') ||
                       fontFamily.toLowerCase().includes('consolas');
    expect(isMonospace).toBe(true);
  });

  /**
   * Additional test: Configuration section is accessible from navigation
   */
  test('Configuration section is accessible via navigation', async ({ page }) => {
    // Find the Configuration link in navigation (if exists) or scroll to section
    const configSection = page.locator('#configuration');
    await configSection.scrollIntoViewIfNeeded();

    // Verify the configuration section is visible
    await expect(configSection).toBeVisible();

    // Verify it has a heading
    const heading = configSection.locator('h2');
    await expect(heading).toHaveText('Configuration');
  });
});
