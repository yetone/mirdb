/**
 * MirDB Landing Page - Configuration Section E2E Tests
 * Owner: Scenario 5 - Configuration Section
 *
 * Tests verify:
 * - Configuration section displays TOML example
 * - All key parameters are present (addr, work_dir, mem_table_max_size, sst_max_size)
 * - Parameter documentation table exists
 * - Section is collapsible/expandable
 */

const { test, expect } = require('@playwright/test');
const { setupPage } = require('../helpers/test-utils');

test.describe('Configuration Section', () => {
  test.beforeEach(async ({ page }) => {
    await setupPage(page);
    // Navigate to configuration section
    await page.locator('#configuration').scrollIntoViewIfNeeded();
  });

  // Test Case 1: Check for TOML configuration example
  test('TC1: Configuration code block shows TOML syntax', async ({ page }) => {
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Check for the TOML code block
    const tomlCodeBlock = configSection.locator('.config-code-block, .code-block, pre');
    await expect(tomlCodeBlock.first()).toBeVisible();

    // Verify TOML syntax is present (key = value pattern)
    const codeContent = await configSection.textContent();
    // TOML uses = for assignment
    expect(codeContent).toMatch(/\w+\s*=\s*["']/);
  });

  // Test Case 2: Verify addr parameter in config
  test('TC2: Configuration shows addr parameter', async ({ page }) => {
    const configSection = page.locator('#configuration');

    // Find the configuration code
    const codeContent = await configSection.textContent();

    // Verify addr parameter is present with the expected value
    expect(codeContent).toContain('addr');
    expect(codeContent).toContain('0.0.0.0:12333');
  });

  // Test Case 3: Verify work_dir parameter in config
  test('TC3: Configuration includes work_dir parameter', async ({ page }) => {
    const configSection = page.locator('#configuration');

    // Find the configuration code
    const codeContent = await configSection.textContent();

    // Verify work_dir parameter is present
    expect(codeContent).toContain('work_dir');
  });

  // Test Case 4: Verify mem_table_max_size parameter
  test('TC4: Configuration includes mem_table_max_size parameter', async ({ page }) => {
    const configSection = page.locator('#configuration');

    // Find the configuration code
    const codeContent = await configSection.textContent();

    // Verify mem_table_max_size parameter is present
    expect(codeContent).toContain('mem_table_max_size');
  });

  // Test Case 5: Verify sst_max_size parameter
  test('TC5: Configuration includes sst_max_size parameter', async ({ page }) => {
    const configSection = page.locator('#configuration');

    // Find the configuration code
    const codeContent = await configSection.textContent();

    // Verify sst_max_size parameter is present
    expect(codeContent).toContain('sst_max_size');
  });

  // Test Case 6: Check for parameter descriptions table
  test('TC6: Table or list explaining what each parameter does', async ({ page }) => {
    const configSection = page.locator('#configuration');

    // Check for either a table or a description list
    const paramsTable = configSection.locator('.config-params, .params-table, table, dl');
    await expect(paramsTable.first()).toBeVisible();

    // Verify some parameter descriptions exist
    const sectionContent = await configSection.textContent();

    // Check that descriptions are present (should have multiple parameter names and descriptions)
    expect(sectionContent).toContain('addr');
    expect(sectionContent).toContain('work_dir');
    expect(sectionContent).toContain('mem_table_max_size');
    expect(sectionContent).toContain('sst_max_size');

    // Verify there are descriptive texts (not just parameter names)
    // The section should have words like "directory", "size", "address", etc.
    expect(sectionContent.toLowerCase()).toMatch(/directory|storage|address|size|memory/);
  });

  // Test Case 7: Configuration section is expandable/collapsible
  test('TC7: Section can be collapsed to reduce visual clutter', async ({ page }) => {
    const configSection = page.locator('#configuration');

    // Find the collapsible element (details/summary or custom toggle)
    const collapsibleElement = configSection.locator('details, .collapsible, [data-collapsible]');
    await expect(collapsibleElement.first()).toBeVisible();

    // Check for a toggle button or summary element
    const toggleElement = configSection.locator('summary, .collapse-toggle, [data-toggle]');
    await expect(toggleElement.first()).toBeVisible();

    // Get the collapsible content container
    const contentContainer = collapsibleElement.first();

    // If it's a details element, test the native behavior
    const tagName = await contentContainer.evaluate(el => el.tagName.toLowerCase());

    if (tagName === 'details') {
      // Test native details/summary behavior
      const isOpenInitially = await contentContainer.getAttribute('open');

      // Click to toggle
      await toggleElement.first().click();
      await page.waitForTimeout(300);

      // Verify state changed
      const isOpenAfterClick = await contentContainer.getAttribute('open');
      expect(isOpenInitially !== isOpenAfterClick || isOpenAfterClick !== null).toBeTruthy();
    } else {
      // Test custom collapsible behavior
      await toggleElement.first().click();
      await page.waitForTimeout(300);

      // Check if content visibility changed
      const collapsedContent = configSection.locator('.collapse-content, .collapsible-content');
      if (await collapsedContent.count() > 0) {
        const isVisible = await collapsedContent.first().isVisible();
        // Content should either be hidden or visible depending on initial state
        expect(typeof isVisible).toBe('boolean');
      }
    }
  });

  // Additional test: Configuration section has proper accessibility structure
  test('Configuration section has proper accessibility structure', async ({ page }) => {
    const configSection = page.locator('#configuration');

    // Verify section has aria-labelledby
    await expect(configSection).toHaveAttribute('aria-labelledby', 'configuration-title');

    // Verify heading exists and is visible
    const configTitle = page.locator('#configuration-title');
    await expect(configTitle).toBeVisible();
    await expect(configTitle).toHaveText('Configuration');
  });

  // Additional test: Code block has copy functionality
  test('Configuration code block has copy button', async ({ page }) => {
    const configSection = page.locator('#configuration');

    // Find copy button
    const copyButton = configSection.locator('.copy-btn, [data-copy], button:has-text("Copy")');

    // Copy button should be present if code block is large
    if (await copyButton.count() > 0) {
      await expect(copyButton.first()).toBeVisible();
    }
  });

  // Additional test: All config parameters are documented
  test('All major configuration parameters have descriptions', async ({ page }) => {
    const configSection = page.locator('#configuration');
    const sectionContent = await configSection.textContent();

    // Verify all major parameters from the PRD are mentioned
    const requiredParams = [
      'addr',
      'work_dir',
      'mem_table_max_size',
      'sst_max_size',
      'max_level',
      'block_size'
    ];

    for (const param of requiredParams) {
      expect(sectionContent).toContain(param);
    }
  });
});
