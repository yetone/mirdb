/**
 * Configuration Examples Tests
 * Owner: Scenario 7 - Configuration Examples
 *
 * Test cases:
 * - Configuration section exists
 * - TOML config example shown
 * - CLI options documented
 */

import { test, expect } from '@playwright/test';

test.describe('Configuration Examples', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Configuration section heading exists', async ({ page }) => {
    // Check for configuration section
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Check for heading containing 'Config' or 'Configuration'
    const heading = configSection.locator('h2');
    await expect(heading).toBeVisible();
    const headingText = await heading.textContent();
    expect(headingText).toMatch(/config(uration)?/i);
  });

  test('TC2: TOML config example with addr and work_dir is present', async ({ page }) => {
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Check for TOML configuration code block
    const tomlCodeBlock = configSection.locator('#code-config-toml');
    await expect(tomlCodeBlock).toBeVisible();

    // Verify the code block contains TOML syntax with addr option
    const codeContent = await tomlCodeBlock.textContent();
    expect(codeContent).toContain('addr');
    expect(codeContent).toContain('work_dir');
    expect(codeContent).toContain('=');

    // Verify it shows TOML format (key = "value" pattern)
    expect(codeContent).toMatch(/addr\s*=\s*"[^"]+"/);
    expect(codeContent).toMatch(/work_dir\s*=\s*"[^"]+"/);
  });

  test('TC3: CLI options or command-line flags are documented', async ({ page }) => {
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Check for CLI Options subsection
    const cliHeading = configSection.locator('h3:has-text("CLI")');
    await expect(cliHeading).toBeVisible();

    // Check for CLI code block
    const cliCodeBlock = configSection.locator('#code-cli-options');
    await expect(cliCodeBlock).toBeVisible();

    // Verify CLI options are documented (looking for common flag patterns)
    const cliContent = await cliCodeBlock.textContent();
    expect(cliContent).toMatch(/-c|--config/);
    expect(cliContent).toMatch(/-h|--help/);

    // Check for Available Flags section
    const flagsHeading = configSection.locator('h4:has-text("Available Flags")');
    await expect(flagsHeading).toBeVisible();

    // Check for definition list with CLI options
    const optionsList = configSection.locator('.options-definition');
    await expect(optionsList).toBeVisible();

    // Verify config flag is documented
    const configFlag = optionsList.locator('dt:has-text("--config")');
    await expect(configFlag).toBeVisible();
  });

  test('Configuration section has proper structure', async ({ page }) => {
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Check for main heading
    const mainHeading = configSection.locator('h2');
    await expect(mainHeading).toBeVisible();

    // Check for configuration file subsection
    const configFileHeading = configSection.locator('h3:has-text("Configuration File")');
    await expect(configFileHeading).toBeVisible();

    // Check for configuration options subsection (table)
    const optionsHeading = configSection.locator('h3:has-text("Configuration Options")');
    await expect(optionsHeading).toBeVisible();

    // Check for configuration options table
    const configTable = configSection.locator('.config-table');
    await expect(configTable).toBeVisible();

    // Verify table has headers
    const tableHeaders = configTable.locator('th');
    await expect(tableHeaders).toHaveCount(3);
  });

  test('Configuration options table shows parameter details', async ({ page }) => {
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Check for configuration table
    const configTable = configSection.locator('.config-table');
    await expect(configTable).toBeVisible();

    // Verify table contains key configuration parameters
    const tableContent = await configTable.textContent();
    expect(tableContent).toContain('addr');
    expect(tableContent).toContain('work_dir');
    expect(tableContent).toContain('Parameter');
    expect(tableContent).toContain('Description');
    expect(tableContent).toContain('Default');
  });

  test('Configuration section has TOML code block with copy button', async ({ page }) => {
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Find the code block containing TOML config
    const codeBlock = configSection.locator('.code-block').filter({ has: page.locator('#code-config-toml') });
    await expect(codeBlock).toBeVisible();

    // Check for copy button
    const copyButton = codeBlock.locator('.copy-button');
    await expect(copyButton).toBeVisible();
    await expect(copyButton).toHaveAttribute('aria-label', 'Copy code to clipboard');
  });

  test('Configuration section documents size units', async ({ page }) => {
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Check for Size Units subsection
    const sizeUnitsHeading = configSection.locator('h3:has-text("Size Units")');
    await expect(sizeUnitsHeading).toBeVisible();

    // Check for size units list
    const sizeUnitsList = configSection.locator('.size-units-list');
    await expect(sizeUnitsList).toBeVisible();

    // Verify size unit suffixes are documented
    const sizeUnitsContent = await sizeUnitsList.textContent();
    expect(sizeUnitsContent).toContain('K');
    expect(sizeUnitsContent).toContain('M');
    expect(sizeUnitsContent).toContain('G');
  });

  test('Configuration section can be scrolled into view', async ({ page }) => {
    // Navigate to top of page first
    await page.goto('/');

    // Scroll to configuration section
    const configSection = page.locator('#configuration');
    await configSection.scrollIntoViewIfNeeded();
    await expect(configSection).toBeVisible();
    await expect(configSection).toBeInViewport();
  });

  test('Configuration TOML example shows all major settings', async ({ page }) => {
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Check for TOML code content
    const tomlCodeBlock = configSection.locator('#code-config-toml');
    const codeContent = await tomlCodeBlock.textContent();

    // Verify major configuration categories are shown
    expect(codeContent).toContain('addr'); // Network
    expect(codeContent).toContain('work_dir'); // Storage
    expect(codeContent).toContain('mem_table'); // Memory tables
    expect(codeContent).toContain('sst_max_size'); // SSTables
    expect(codeContent).toContain('compaction'); // Compaction
  });
});
