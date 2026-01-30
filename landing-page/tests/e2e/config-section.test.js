/**
 * MirDB Landing Page - Configuration Section E2E Tests
 * Owner: Scenario 5 - Configuration Section
 *
 * Tests for:
 * - TOML configuration example display
 * - Parameter documentation table
 * - Collapsible/expandable functionality
 */

const { test, expect } = require('@playwright/test');
const { setupPage } = require('../helpers/test-utils');

test.describe('Configuration Section', () => {
  test.beforeEach(async ({ page }) => {
    await setupPage(page);
  });

  test('TC1: TOML configuration example is displayed with proper syntax', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('#configuration');
    await configSection.scrollIntoViewIfNeeded();

    // Check for code block with TOML syntax
    const configCode = page.locator('#config-toml');
    await expect(configCode).toBeVisible();

    // Verify it contains TOML syntax elements
    const codeContent = await configCode.textContent();
    expect(codeContent).toContain('=');
    expect(codeContent).toContain('#'); // TOML comments start with #
  });

  test('TC2: Configuration shows addr parameter with correct value', async ({ page }) => {
    const configSection = page.locator('#configuration');
    await configSection.scrollIntoViewIfNeeded();

    // Check for addr parameter in the TOML example
    const configCode = page.locator('#config-toml');
    const codeContent = await configCode.textContent();

    // Verify addr = "0.0.0.0:12333" is present
    expect(codeContent).toContain('addr');
    expect(codeContent).toContain('0.0.0.0:12333');
  });

  test('TC3: Configuration includes work_dir parameter', async ({ page }) => {
    const configSection = page.locator('#configuration');
    await configSection.scrollIntoViewIfNeeded();

    const configCode = page.locator('#config-toml');
    const codeContent = await configCode.textContent();

    // Verify work_dir parameter is present
    expect(codeContent).toContain('work_dir');
  });

  test('TC4: Configuration includes mem_table_max_size parameter', async ({ page }) => {
    const configSection = page.locator('#configuration');
    await configSection.scrollIntoViewIfNeeded();

    const configCode = page.locator('#config-toml');
    const codeContent = await configCode.textContent();

    // Verify mem_table_max_size parameter is present
    expect(codeContent).toContain('mem_table_max_size');
  });

  test('TC5: Configuration includes sst_max_size parameter', async ({ page }) => {
    const configSection = page.locator('#configuration');
    await configSection.scrollIntoViewIfNeeded();

    const configCode = page.locator('#config-toml');
    const codeContent = await configCode.textContent();

    // Verify sst_max_size parameter is present
    expect(codeContent).toContain('sst_max_size');
  });

  test('TC6: Parameter descriptions table exists and documents parameters', async ({ page }) => {
    const configSection = page.locator('#configuration');
    await configSection.scrollIntoViewIfNeeded();

    // Check for parameter table
    const configTable = page.locator('.config-table');
    await expect(configTable).toBeVisible();

    // Check table has headers
    const tableHeaders = configTable.locator('th');
    await expect(tableHeaders).toHaveCount(3);

    // Check for Parameter, Description, Default columns
    await expect(tableHeaders.nth(0)).toContainText('Parameter');
    await expect(tableHeaders.nth(1)).toContainText('Description');
    await expect(tableHeaders.nth(2)).toContainText('Default');

    // Check that table has rows with parameter documentation
    const tableRows = configTable.locator('tbody tr');
    const rowCount = await tableRows.count();
    expect(rowCount).toBeGreaterThan(0);

    // Verify some key parameters are documented
    const tableContent = await configTable.textContent();
    expect(tableContent).toContain('addr');
    expect(tableContent).toContain('work_dir');
    expect(tableContent).toContain('mem_table_max_size');
    expect(tableContent).toContain('sst_max_size');
  });

  test('TC7: Configuration section is collapsible and expandable', async ({ page }) => {
    const configSection = page.locator('#configuration');
    await configSection.scrollIntoViewIfNeeded();

    // Find the toggle button
    const toggleButton = page.locator('#config-toggle-btn');
    await expect(toggleButton).toBeVisible();

    // Verify initial state is expanded
    const initialState = await toggleButton.getAttribute('aria-expanded');
    expect(initialState).toBe('true');

    // Find the content container
    const configContent = page.locator('#config-content');
    await expect(configContent).toBeVisible();

    // Click to collapse
    await toggleButton.click();

    // Wait for animation
    await page.waitForTimeout(300);

    // Verify collapsed state
    const collapsedState = await toggleButton.getAttribute('aria-expanded');
    expect(collapsedState).toBe('false');

    // Content should have collapsed class
    await expect(configContent).toHaveClass(/collapsed/);

    // Click to expand again
    await toggleButton.click();

    // Wait for animation
    await page.waitForTimeout(300);

    // Verify expanded state
    const expandedState = await toggleButton.getAttribute('aria-expanded');
    expect(expandedState).toBe('true');

    // Content should not have collapsed class
    await expect(configContent).not.toHaveClass(/collapsed/);
  });

  test('Configuration section has proper accessibility attributes', async ({ page }) => {
    const configSection = page.locator('#configuration');
    await configSection.scrollIntoViewIfNeeded();

    // Check section has aria-labelledby
    await expect(configSection).toHaveAttribute('aria-labelledby', 'configuration-title');

    // Check toggle button has proper ARIA attributes
    const toggleButton = page.locator('#config-toggle-btn');
    await expect(toggleButton).toHaveAttribute('aria-expanded');
    await expect(toggleButton).toHaveAttribute('aria-controls', 'config-content');

    // Check table has aria-labelledby
    const configTable = page.locator('.config-table');
    await expect(configTable).toHaveAttribute('aria-labelledby', 'config-params-title');
  });

  test('Configuration section displays file name indicator', async ({ page }) => {
    const configSection = page.locator('#configuration');
    await configSection.scrollIntoViewIfNeeded();

    // Check for file name in code header
    const fileName = page.locator('.config-file-name');
    await expect(fileName).toBeVisible();
    await expect(fileName).toContainText('mirdb.toml');
  });

  test('Configuration has copy button for TOML example', async ({ page }) => {
    const configSection = page.locator('#configuration');
    await configSection.scrollIntoViewIfNeeded();

    // Check for copy button
    const copyButton = page.locator('.config-code-wrapper .copy-btn');
    await expect(copyButton).toBeVisible();
    await expect(copyButton).toHaveAttribute('data-copy-target', 'config-toml');
  });
});
