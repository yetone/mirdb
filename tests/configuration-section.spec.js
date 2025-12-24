// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for Configuration Section Display (Scenario 7)
 * Verifies that default configuration parameters and customization options
 * are shown as specified in REQ-7
 */

test.describe('Configuration Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Check for configuration parameters table/list
   * Expected: Configuration section displays key parameters with descriptions
   */
  test('should display configuration parameters with descriptions', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Check that the section has a heading
    const configHeading = configSection.locator('h2');
    await expect(configHeading).toBeVisible();
    const headingText = await configHeading.textContent();
    expect(headingText.toLowerCase()).toContain('configuration');

    // Check for configuration table or list
    const configTable = configSection.locator('table.config-table, table');
    const configTableExists = await configTable.count() > 0;

    if (configTableExists) {
      await expect(configTable.first()).toBeVisible();

      // Verify table has headers for parameter, description, and default
      const tableHeaders = configTable.first().locator('thead th, th');
      const headerCount = await tableHeaders.count();
      expect(headerCount).toBeGreaterThanOrEqual(2);

      // Check for key parameters in the table
      const tableBody = configTable.first().locator('tbody');
      await expect(tableBody).toBeVisible();

      // Verify key parameters are listed
      const tableText = await configTable.first().textContent();
      expect(tableText).toContain('addr');
      expect(tableText).toContain('work_dir');
    } else {
      // If no table, check for a list or code block with parameters
      const configContent = await configSection.textContent();
      expect(configContent).toContain('addr');
      expect(configContent).toContain('work_dir');
    }
  });

  /**
   * Test Case 2: Verify default values are shown
   * Expected: Default values like '12333', '7', '4M', '100M' are displayed
   */
  test('should display default configuration values', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Get the full text content of the configuration section
    const configText = await configSection.textContent();

    // Check for default port value (12333)
    expect(configText).toContain('12333');

    // Check for max_level default value (7)
    expect(configText).toContain('7');

    // Check for mem_table_max_size default value (4M)
    expect(configText).toContain('4M');

    // Check for sst_max_size default value (100M)
    expect(configText).toContain('100M');
  });

  /**
   * Test Case 3: Check for configuration documentation link
   * Expected: Link to detailed configuration docs is present
   */
  test('should have link to detailed configuration documentation', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Check for documentation link within config section or related area
    const docLink = configSection.locator('a[href*="doc"], a[href*="github"], a[href*="config"]');
    const hasDocLink = await docLink.count() > 0;

    if (hasDocLink) {
      await expect(docLink.first()).toBeVisible();
    } else {
      // Check for a note or text pointing to documentation
      const configText = await configSection.textContent();
      const hasDocReference =
        configText.toLowerCase().includes('documentation') ||
        configText.toLowerCase().includes('more details') ||
        configText.toLowerCase().includes('full configuration') ||
        configText.toLowerCase().includes('see') ||
        configText.toLowerCase().includes('refer to');

      // If no explicit link, check if there's a documentation reference in footer
      const footerDocLink = page.locator('footer a[href*="doc"], footer a[href*="github"]');
      const hasFooterDocLink = await footerDocLink.count() > 0;

      expect(hasDocReference || hasFooterDocLink).toBeTruthy();
    }
  });

  /**
   * Additional test: Verify configuration section is navigable
   */
  test('should be able to navigate to configuration section', async ({ page }) => {
    // Check if navigation link to configuration exists
    const configNavLink = page.locator('a[href="#configuration"]');
    const hasNavLink = await configNavLink.count() > 0;

    if (hasNavLink) {
      await configNavLink.first().click();
    }

    // Verify configuration section is visible
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();
  });

  /**
   * Additional test: Verify key configuration parameters are present
   */
  test('should display all key configuration parameters', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Get the full text content
    const configText = await configSection.textContent();

    // Verify all key parameters from PRD are present
    const keyParameters = [
      'addr',
      'max_level',
      'work_dir',
      'sst_max_size',
      'mem_table_max_size',
      'block_size',
      'l0_compaction_trigger'
    ];

    for (const param of keyParameters) {
      expect(configText).toContain(param);
    }
  });
});
