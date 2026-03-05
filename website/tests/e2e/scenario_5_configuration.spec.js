/**
 * E2E Tests for Scenario 5: Configuration Section
 *
 * Tests verify that the configuration section displays mirdb.toml example
 * with proper formatting and documents key configuration options.
 */
const { test, expect } = require('@playwright/test');

test.describe('Configuration Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: Configuration section is present with appropriate heading', async ({ page }) => {
    // Locate the configuration section
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Check for the "Configuration" heading
    const heading = configSection.locator('h2');
    await expect(heading).toBeVisible();
    await expect(heading).toContainText('Configuration');
  });

  test('Test Case 2: Code block contains mirdb.toml configuration example', async ({ page }) => {
    const configSection = page.locator('#configuration');

    // Check for TOML code block presence
    const tomlCodeBlock = configSection.locator('[data-testid="toml-code-block"]');
    await expect(tomlCodeBlock).toBeVisible();

    // Verify key TOML configuration parameters are present
    const tomlCode = configSection.locator('[data-testid="toml-code"]');
    await expect(tomlCode).toContainText('addr');
    await expect(tomlCode).toContainText('0.0.0.0:12333');
    await expect(tomlCode).toContainText('work_dir');
    await expect(tomlCode).toContainText('/tmp/mirdb');
    await expect(tomlCode).toContainText('sst_max_size');
    await expect(tomlCode).toContainText('mem_table_max_size');
    await expect(tomlCode).toContainText('block_size');
    await expect(tomlCode).toContainText('l0_compaction_trigger');
  });

  test('Test Case 3: TOML code block has proper syntax highlighting', async ({ page }) => {
    const configSection = page.locator('#configuration');

    // Check for language-toml class indicating TOML syntax
    const tomlCode = configSection.locator('[data-testid="toml-code"]');
    await expect(tomlCode).toHaveClass(/language-toml/);

    // Verify syntax highlighting spans are present with different colors
    // Check for key highlighting (blue)
    const keyHighlights = configSection.locator('[data-testid="toml-code"] span.text-blue-400');
    await expect(keyHighlights.first()).toBeVisible();

    // Check for string value highlighting (green)
    const stringHighlights = configSection.locator('[data-testid="toml-code"] span.text-green-400');
    await expect(stringHighlights.first()).toBeVisible();

    // Check for numeric value highlighting (orange)
    const numericHighlights = configSection.locator('[data-testid="toml-code"] span.text-orange-400');
    await expect(numericHighlights.first()).toBeVisible();

    // Check for comment highlighting (gray)
    const commentHighlights = configSection.locator('[data-testid="toml-code"] span.text-gray-400');
    await expect(commentHighlights.first()).toBeVisible();
  });

  test('Test Case 4: Key configuration options are explained', async ({ page }) => {
    const configSection = page.locator('#configuration');

    // Check for configuration options documentation section
    const configDocs = configSection.locator('[data-testid="config-options-docs"]');
    await expect(configDocs).toBeVisible();

    // Verify Network section is documented
    const networkSection = configSection.locator('[data-testid="config-section-network"]');
    await expect(networkSection).toBeVisible();
    await expect(networkSection).toContainText('Network');
    await expect(networkSection).toContainText('addr');
    await expect(networkSection).toContainText('Listen address');

    // Verify Storage section is documented
    const storageSection = configSection.locator('[data-testid="config-section-storage"]');
    await expect(storageSection).toBeVisible();
    await expect(storageSection).toContainText('Storage');
    await expect(storageSection).toContainText('work_dir');
    await expect(storageSection).toContainText('max_level');

    // Verify Memory Tables section is documented
    const memtableSection = configSection.locator('[data-testid="config-section-memtable"]');
    await expect(memtableSection).toBeVisible();
    await expect(memtableSection).toContainText('Memory Tables');
    await expect(memtableSection).toContainText('mem_table_max_size');

    // Verify SSTables section is documented
    const sstableSection = configSection.locator('[data-testid="config-section-sstable"]');
    await expect(sstableSection).toBeVisible();
    await expect(sstableSection).toContainText('SSTables');
    await expect(sstableSection).toContainText('sst_max_size');
    await expect(sstableSection).toContainText('block_size');

    // Verify Compaction section is documented
    const compactionSection = configSection.locator('[data-testid="config-section-compaction"]');
    await expect(compactionSection).toBeVisible();
    await expect(compactionSection).toContainText('Compaction');
    await expect(compactionSection).toContainText('l0_compaction_trigger');
  });

  test('Configuration file path reference is provided', async ({ page }) => {
    const configSection = page.locator('#configuration');

    // Check that config file path is mentioned
    const configPath = configSection.locator('[data-testid="config-path"]');
    await expect(configPath).toBeVisible();
    await expect(configPath).toContainText('/etc/mirdb.toml');
  });

  test('Size units documentation is present', async ({ page }) => {
    const configSection = page.locator('#configuration');

    // Check for size units note
    const sizeUnitsNote = configSection.locator('[data-testid="size-units-note"]');
    await expect(sizeUnitsNote).toBeVisible();
    await expect(sizeUnitsNote).toContainText('Size Units');
    await expect(sizeUnitsNote).toContainText('K');
    await expect(sizeUnitsNote).toContainText('M');
    await expect(sizeUnitsNote).toContainText('G');
    await expect(sizeUnitsNote).toContainText('T');
  });

  test('Configuration example heading is present', async ({ page }) => {
    const configSection = page.locator('#configuration');

    // Check for example configuration heading
    const exampleHeading = configSection.locator('[data-testid="config-example-heading"]');
    await expect(exampleHeading).toBeVisible();
    await expect(exampleHeading).toContainText('Example Configuration');
    await expect(exampleHeading).toContainText('mirdb.toml');
  });
});
