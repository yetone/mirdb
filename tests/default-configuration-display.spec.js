// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for Default Configuration Display - Scenario: Default Configuration Display
 *
 * These tests verify that all default configuration parameters are displayed
 * for reference in the configuration section of the MirDB homepage.
 */

test.describe('Default Configuration Display', () => {

  test.beforeEach(async ({ page }) => {
    // Navigate to the homepage
    await page.goto('/');
    // Wait for the page to be fully loaded
    await page.waitForLoadState('domcontentloaded');
  });

  /**
   * Test Case 1: Check for addr configuration
   *
   * Verifies that the default address '0.0.0.0:12333' is displayed
   * in the configuration section.
   */
  test('should display default addr configuration value', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Verify the configuration table exists
    const configTable = page.locator('[data-testid="config-table"]');
    await expect(configTable).toBeVisible();

    // Verify addr row exists
    const addrRow = page.locator('[data-testid="config-addr"]');
    await expect(addrRow).toBeVisible();

    // Verify addr value is displayed correctly
    const addrValue = page.locator('[data-testid="config-addr-value"]');
    await expect(addrValue).toBeVisible();
    await expect(addrValue).toHaveText('0.0.0.0:12333');
  });

  /**
   * Test Case 2: Check for max_level configuration
   *
   * Verifies that the default max_level '7' is displayed
   * in the configuration section.
   */
  test('should display default max_level configuration value', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Verify max_level row exists
    const maxLevelRow = page.locator('[data-testid="config-max-level"]');
    await expect(maxLevelRow).toBeVisible();

    // Verify max_level value is displayed correctly
    const maxLevelValue = page.locator('[data-testid="config-max-level-value"]');
    await expect(maxLevelValue).toBeVisible();
    await expect(maxLevelValue).toHaveText('7');
  });

  /**
   * Test Case 3: Check for work_dir configuration
   *
   * Verifies that the default work_dir '/tmp/mirdb' is displayed
   * in the configuration section.
   */
  test('should display default work_dir configuration value', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Verify work_dir row exists
    const workDirRow = page.locator('[data-testid="config-work-dir"]');
    await expect(workDirRow).toBeVisible();

    // Verify work_dir value is displayed correctly
    const workDirValue = page.locator('[data-testid="config-work-dir-value"]');
    await expect(workDirValue).toBeVisible();
    await expect(workDirValue).toHaveText('/tmp/mirdb');
  });

  /**
   * Test Case 4: Check for sst_max_size configuration
   *
   * Verifies that the default sst_max_size '100M' is displayed
   * in the configuration section.
   */
  test('should display default sst_max_size configuration value', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Verify sst_max_size row exists
    const sstMaxSizeRow = page.locator('[data-testid="config-sst-max-size"]');
    await expect(sstMaxSizeRow).toBeVisible();

    // Verify sst_max_size value is displayed correctly
    const sstMaxSizeValue = page.locator('[data-testid="config-sst-max-size-value"]');
    await expect(sstMaxSizeValue).toBeVisible();
    await expect(sstMaxSizeValue).toHaveText('100M');
  });

  /**
   * Test Case 5: Check for mem_table_max_size configuration
   *
   * Verifies that the default mem_table_max_size '4M' is displayed
   * in the configuration section.
   */
  test('should display default mem_table_max_size configuration value', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Verify mem_table_max_size row exists
    const memTableMaxSizeRow = page.locator('[data-testid="config-mem-table-max-size"]');
    await expect(memTableMaxSizeRow).toBeVisible();

    // Verify mem_table_max_size value is displayed correctly
    const memTableMaxSizeValue = page.locator('[data-testid="config-mem-table-max-size-value"]');
    await expect(memTableMaxSizeValue).toBeVisible();
    await expect(memTableMaxSizeValue).toHaveText('4M');
  });

  /**
   * Test Case 6: Check for block_size configuration
   *
   * Verifies that the default block_size '4K' is displayed
   * in the configuration section.
   */
  test('should display default block_size configuration value', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Verify block_size row exists
    const blockSizeRow = page.locator('[data-testid="config-block-size"]');
    await expect(blockSizeRow).toBeVisible();

    // Verify block_size value is displayed correctly
    const blockSizeValue = page.locator('[data-testid="config-block-size-value"]');
    await expect(blockSizeValue).toBeVisible();
    await expect(blockSizeValue).toHaveText('4K');
  });

  /**
   * Additional test: Configuration section structure and accessibility
   */
  test('should have accessible configuration section with proper structure', async ({ page }) => {
    // Verify section exists and is accessible
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Verify section title
    const sectionTitle = configSection.locator('h2');
    await expect(sectionTitle).toContainText('Default Configuration');

    // Verify introductory text
    const introText = configSection.locator('.configuration-intro');
    await expect(introText).toBeVisible();

    // Verify table has proper header structure
    const tableHeaders = configSection.locator('th');
    await expect(tableHeaders).toHaveCount(3);

    // Verify all 6 configuration rows are present
    const configRows = configSection.locator('tbody tr');
    await expect(configRows).toHaveCount(6);
  });

  /**
   * Additional test: Navigation link to configuration section
   */
  test('should have navigation link to configuration section', async ({ page }) => {
    // Verify navigation link to configuration exists
    const navLink = page.locator('nav a[href="#configuration"]');
    await expect(navLink).toBeVisible();
    await expect(navLink).toHaveText('Configuration');

    // Click the link and verify navigation
    await navLink.click();

    // Verify the configuration section is in view
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeInViewport();
  });
});
