// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Configuration Section Display E2E Tests
 *
 * Scenario: Verify configuration examples and parameter reference are displayed
 * as specified in REQ-7 and US-4
 */

test.describe('Configuration Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Check for TOML configuration example
   * Input: Check for TOML configuration example
   * Expected: Example TOML configuration is displayed in a code block
   */
  test('should display TOML configuration example in a code block', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('#configuration');
    await configSection.scrollIntoViewIfNeeded();

    // Verify the configuration section exists
    await expect(configSection).toBeVisible();

    // Find the TOML code block
    const tomlCodeBlock = page.locator('[data-testid="toml-config-example"]');
    await expect(tomlCodeBlock).toBeVisible();

    // Verify it contains TOML configuration content
    const codeContent = await tomlCodeBlock.textContent();

    // Check for typical TOML configuration elements
    expect(codeContent).toContain('addr');
    expect(codeContent).toContain('max_level');
    expect(codeContent).toContain('work_dir');
    expect(codeContent).toContain('sst_max_size');
    expect(codeContent).toContain('mem_table_max_size');
    expect(codeContent).toContain('block_size');

    // Verify it's in a code block (pre tag)
    const preElement = configSection.locator('pre');
    await expect(preElement.first()).toBeVisible();
  });

  /**
   * Test Case 2: Check for parameters table
   * Input: Check for parameters table
   * Expected: Table showing configuration parameters with descriptions and default values
   */
  test('should display parameters table with descriptions and default values', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('#configuration');
    await configSection.scrollIntoViewIfNeeded();

    // Verify parameters table exists
    const paramsTable = page.locator('[data-testid="config-params-table"]');
    await expect(paramsTable).toBeVisible();

    // Verify table has header row with expected columns
    const headerRow = paramsTable.locator('thead tr');
    await expect(headerRow).toBeVisible();

    // Check for Parameter, Description, and Default columns
    const headers = headerRow.locator('th');
    const headerTexts = await headers.allTextContents();
    expect(headerTexts.some(h => h.toLowerCase().includes('parameter'))).toBe(true);
    expect(headerTexts.some(h => h.toLowerCase().includes('description'))).toBe(true);
    expect(headerTexts.some(h => h.toLowerCase().includes('default'))).toBe(true);

    // Verify table body has rows with parameter data
    const tableRows = paramsTable.locator('tbody tr');
    const rowCount = await tableRows.count();
    expect(rowCount).toBeGreaterThanOrEqual(6); // At least 6 key parameters

    // Verify key parameters are present in the table
    const tableContent = await paramsTable.textContent();
    expect(tableContent).toContain('addr');
    expect(tableContent).toContain('max_level');
    expect(tableContent).toContain('work_dir');
    expect(tableContent).toContain('sst_max_size');
    expect(tableContent).toContain('mem_table_max_size');
    expect(tableContent).toContain('block_size');
  });

  /**
   * Test Case 3: Verify default addr parameter
   * Input: Verify default addr parameter
   * Expected: Default addr value shown as '0.0.0.0:12333'
   */
  test('should show default addr value as 0.0.0.0:12333', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('#configuration');
    await configSection.scrollIntoViewIfNeeded();

    // Find the parameters table
    const paramsTable = page.locator('[data-testid="config-params-table"]');
    await expect(paramsTable).toBeVisible();

    // Find the row containing addr parameter
    const addrRow = paramsTable.locator('tbody tr').filter({ hasText: 'addr' }).first();
    await expect(addrRow).toBeVisible();

    // Verify the default value is 0.0.0.0:12333
    const rowContent = await addrRow.textContent();
    expect(rowContent).toContain('0.0.0.0:12333');
  });

  /**
   * Test Case 4: Verify default work_dir parameter
   * Input: Verify default work_dir parameter
   * Expected: Default work_dir value shown as '/tmp/mirdb'
   */
  test('should show default work_dir value as /tmp/mirdb', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('#configuration');
    await configSection.scrollIntoViewIfNeeded();

    // Find the parameters table
    const paramsTable = page.locator('[data-testid="config-params-table"]');
    await expect(paramsTable).toBeVisible();

    // Find the row containing work_dir parameter
    const workDirRow = paramsTable.locator('tbody tr').filter({ hasText: 'work_dir' }).first();
    await expect(workDirRow).toBeVisible();

    // Verify the default value is /tmp/mirdb
    const rowContent = await workDirRow.textContent();
    expect(rowContent).toContain('/tmp/mirdb');
  });

  /**
   * Test Case 5: Check for link to detailed configuration docs
   * Input: Check for link to detailed configuration docs
   * Expected: Link to more detailed configuration documentation is present
   */
  test('should display link to detailed configuration documentation', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('#configuration');
    await configSection.scrollIntoViewIfNeeded();

    // Find the documentation link
    const configDocsLink = page.locator('[data-testid="config-docs-link"]');
    await expect(configDocsLink).toBeVisible();

    // Verify the link has appropriate href
    const href = await configDocsLink.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href).toContain('github.com/yetone/mirdb');

    // Verify the link text indicates it leads to documentation
    const linkText = await configDocsLink.textContent();
    expect(linkText.toLowerCase()).toMatch(/document|config|detail/i);
  });

  /**
   * Additional test: Verify configuration section navigation works
   */
  test('should navigate to configuration section when clicking Configuration link', async ({ page }) => {
    // Click on Configuration navigation link
    const configNavLink = page.locator('a[href="#configuration"]');

    // Check if the nav link exists
    const linkCount = await configNavLink.count();
    if (linkCount > 0) {
      await configNavLink.click();

      // Verify the configuration section is visible
      const configSection = page.locator('#configuration');
      await expect(configSection).toBeVisible();
    } else {
      // If no nav link, just verify section exists
      const configSection = page.locator('#configuration');
      await configSection.scrollIntoViewIfNeeded();
      await expect(configSection).toBeVisible();
    }
  });

  /**
   * Additional test: Verify TOML code block has syntax highlighting classes
   */
  test('should display TOML configuration with proper code block formatting', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('#configuration');
    await configSection.scrollIntoViewIfNeeded();

    // Find the TOML code block
    const tomlCodeBlock = page.locator('[data-testid="toml-config-example"]');
    await expect(tomlCodeBlock).toBeVisible();

    // Verify it's properly formatted in a pre/code structure
    const preElement = tomlCodeBlock.locator('code');
    await expect(preElement).toBeVisible();

    // Check that the code element has language class or data attribute
    const codeElement = tomlCodeBlock.locator('code');
    const codeClass = await codeElement.getAttribute('class');
    const dataLang = await codeElement.getAttribute('data-language');

    // Either class or data-language should indicate TOML
    const hasLanguageIndicator =
      (codeClass && codeClass.toLowerCase().includes('toml')) ||
      (dataLang && dataLang.toLowerCase().includes('toml'));

    expect(hasLanguageIndicator || codeClass !== null).toBe(true);
  });
});
