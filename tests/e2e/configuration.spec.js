// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = 'file://' + path.resolve(__dirname, '../../index.html');

/**
 * Configuration Options Display E2E Tests
 * Tests REQ-6 from PRD - Show configuration options and parameters in a readable format
 */

test.describe('Configuration Options Display (REQ-6)', () => {

  test.beforeEach(async ({ page }) => {
    await page.goto(indexPath);
  });

  // Test Case 1: Configuration section exists and is visible
  test('TC1: Configuration section exists with proper heading', async ({ page }) => {
    // Find the configuration section
    const configSection = page.locator('#configuration, .configuration, [data-testid="configuration-section"]');
    await expect(configSection).toBeVisible();

    // Check for heading containing 'Configuration'
    const heading = configSection.locator('h2, h1');
    await expect(heading).toBeVisible();

    const headingText = await heading.textContent();
    expect(headingText.toLowerCase()).toContain('configuration');
  });

  // Test Case 2: TOML configuration example is displayed
  test('TC2: TOML configuration example is displayed with key parameters', async ({ page }) => {
    const configSection = page.locator('#configuration, .configuration, [data-testid="configuration-section"]');
    await expect(configSection).toBeVisible();

    // Find code block with TOML configuration
    const codeBlock = configSection.locator('code');
    await expect(codeBlock.first()).toBeVisible();

    const codeText = await codeBlock.first().textContent();

    // Check for TOML format indicators (key = value pairs)
    expect(codeText).toMatch(/\w+\s*=\s*["'\d]/);

    // Check for key configuration parameters mentioned in PRD
    // Memory tables
    expect(codeText).toContain('mem_table');

    // Block sizes
    expect(codeText).toContain('block_size');

    // Compaction triggers
    expect(codeText).toContain('compaction_trigger');
  });

  // Test Case 3: Configuration parameters include memory table settings
  test('TC3: Memory table configuration parameters are documented', async ({ page }) => {
    const configSection = page.locator('#configuration, .configuration, [data-testid="configuration-section"]');
    await expect(configSection).toBeVisible();

    const sectionText = await configSection.textContent();

    // Check for memory table related parameters
    expect(sectionText).toContain('mem_table_max_size');
    expect(sectionText.toLowerCase()).toContain('memtable');
  });

  // Test Case 4: Configuration parameters include block size settings
  test('TC4: Block size configuration parameters are documented', async ({ page }) => {
    const configSection = page.locator('#configuration, .configuration, [data-testid="configuration-section"]');
    await expect(configSection).toBeVisible();

    const sectionText = await configSection.textContent();

    // Check for block size related parameters
    expect(sectionText).toContain('block_size');
  });

  // Test Case 5: Configuration parameters include compaction trigger settings
  test('TC5: Compaction trigger configuration parameters are documented', async ({ page }) => {
    const configSection = page.locator('#configuration, .configuration, [data-testid="configuration-section"]');
    await expect(configSection).toBeVisible();

    const sectionText = await configSection.textContent();

    // Check for compaction trigger related parameters
    expect(sectionText).toContain('l0_compaction_trigger');
    expect(sectionText.toLowerCase()).toContain('compaction');
  });

  // Test Case 6: Configuration is displayed in readable format (tables or structured layout)
  test('TC6: Configuration is displayed in a readable structured format', async ({ page }) => {
    const configSection = page.locator('#configuration, .configuration, [data-testid="configuration-section"]');
    await expect(configSection).toBeVisible();

    // Check for tables or structured layout
    const tables = configSection.locator('table, .config-table');
    const tableCount = await tables.count();

    // Should have at least one table for parameter documentation
    expect(tableCount).toBeGreaterThanOrEqual(1);

    // Check that tables have headers
    const tableHeaders = configSection.locator('th');
    const headerCount = await tableHeaders.count();
    expect(headerCount).toBeGreaterThanOrEqual(1);
  });

  // Test Case 7: Configuration example shows .toml file format
  test('TC7: Configuration example references TOML format', async ({ page }) => {
    const configSection = page.locator('#configuration, .configuration, [data-testid="configuration-section"]');
    await expect(configSection).toBeVisible();

    // Check for .toml reference in section or code block
    const sectionText = await configSection.textContent();
    const hasTomlReference = sectionText.toLowerCase().includes('toml') ||
                            sectionText.includes('.toml');

    expect(hasTomlReference).toBeTruthy();
  });

  // Test Case 8: Configuration parameters have descriptions
  test('TC8: Configuration parameters have descriptions', async ({ page }) => {
    const configSection = page.locator('#configuration, .configuration, [data-testid="configuration-section"]');
    await expect(configSection).toBeVisible();

    // Check for description column or descriptive text
    const sectionText = await configSection.textContent();

    // Should have description-like content
    const hasDescriptions = sectionText.toLowerCase().includes('description') ||
                           sectionText.toLowerCase().includes('max') ||
                           sectionText.toLowerCase().includes('size');

    expect(hasDescriptions).toBeTruthy();
  });

});
