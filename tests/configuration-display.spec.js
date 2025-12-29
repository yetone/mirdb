// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Configuration Display E2E Tests
 *
 * These tests verify that the page displays configuration options and defaults
 * for MirDB, including the default port and TOML configuration example.
 */

test.describe('Configuration Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Locate configuration information
   * Expected: Configuration options are displayed on the page
   */
  test('TC1: should display configuration options on the page', async ({ page }) => {
    // Find configuration section - may be part of Quick Start or separate section
    const configSection = page.locator('#getting-started, #configuration, [data-testid="configuration-section"]');
    await expect(configSection).toBeVisible();

    // Find the configuration code block
    const configCodeBlock = page.locator('[data-testid="configuration-code-block"]');
    await expect(configCodeBlock).toBeVisible();

    // Verify essential configuration options are displayed
    const codeContent = configCodeBlock.locator('code');

    // Check for key configuration parameters
    await expect(codeContent).toContainText('addr');
    await expect(codeContent).toContainText('work_dir');

    // Verify storage configuration options are shown
    await expect(codeContent).toContainText('max_level');
    await expect(codeContent).toContainText('sst_max_size');
    await expect(codeContent).toContainText('mem_table_max_size');
    await expect(codeContent).toContainText('block_size');
  });

  /**
   * Test Case 2: Verify default port is mentioned
   * Expected: Default port 12333 is documented
   */
  test('TC2: should display default port 12333', async ({ page }) => {
    // Check that the default port 12333 is mentioned on the page
    const pageContent = page.locator('body');

    // Verify 12333 appears in the page content
    await expect(pageContent).toContainText('12333');

    // Check specifically in the configuration code block
    const configCodeBlock = page.locator('[data-testid="configuration-code-block"]');
    await expect(configCodeBlock).toBeVisible();

    const codeContent = configCodeBlock.locator('code');
    await expect(codeContent).toContainText('12333');

    // Verify it's associated with the addr configuration
    await expect(codeContent).toContainText('0.0.0.0:12333');
  });

  /**
   * Test Case 3: Check for configuration example
   * Expected: TOML or similar configuration example is shown
   */
  test('TC3: should display TOML configuration example', async ({ page }) => {
    // Find the configuration code block
    const configCodeBlock = page.locator('[data-testid="configuration-code-block"]');
    await expect(configCodeBlock).toBeVisible();

    // Verify it's labeled as TOML format
    const codeBlockTitle = configCodeBlock.locator('.code-block-title');
    await expect(codeBlockTitle).toContainText(/toml/i);

    // Verify TOML-style configuration format
    const codeContent = configCodeBlock.locator('code');

    // Check for TOML key-value pairs format (key = value)
    await expect(codeContent).toContainText('addr =');
    await expect(codeContent).toContainText('work_dir =');
    await expect(codeContent).toContainText('max_level =');

    // Verify string values are in quotes (TOML format)
    await expect(codeContent).toContainText('"0.0.0.0:12333"');
    await expect(codeContent).toContainText('"/var/lib/mirdb"');

    // Verify size values with units (TOML format supports this)
    await expect(codeContent).toContainText('"100M"');
    await expect(codeContent).toContainText('"4M"');
    await expect(codeContent).toContainText('"4K"');
  });

  /**
   * Additional test: Verify configuration section is properly structured
   */
  test('should have properly structured configuration code block', async ({ page }) => {
    const configCodeBlock = page.locator('[data-testid="configuration-code-block"]');
    await expect(configCodeBlock).toBeVisible();

    // Verify code block has a header
    const header = configCodeBlock.locator('.code-block-header');
    await expect(header).toBeVisible();

    // Verify code block has a title
    const title = configCodeBlock.locator('.code-block-title');
    await expect(title).toBeVisible();

    // Verify code block has pre and code elements
    const pre = configCodeBlock.locator('pre');
    const code = configCodeBlock.locator('code');
    await expect(pre).toBeVisible();
    await expect(code).toBeVisible();
  });

  /**
   * Additional test: Verify configuration has copy functionality
   */
  test('should have copy button for configuration example', async ({ page }) => {
    const configCodeBlock = page.locator('[data-testid="configuration-code-block"]');
    await expect(configCodeBlock).toBeVisible();

    // Verify copy button exists
    const copyButton = configCodeBlock.locator('.copy-btn');
    await expect(copyButton).toBeVisible();

    // Verify copy button has data-copy attribute with configuration content
    await expect(copyButton).toHaveAttribute('data-copy');

    const copyData = await copyButton.getAttribute('data-copy');
    expect(copyData).toContain('12333');
    expect(copyData).toContain('addr');
  });

  /**
   * Additional test: Verify configuration shows multiple parameter categories
   */
  test('should display configuration for network, storage, and SSTable settings', async ({ page }) => {
    const configCodeBlock = page.locator('[data-testid="configuration-code-block"]');
    const codeContent = configCodeBlock.locator('code');

    // Network settings
    await expect(codeContent).toContainText('addr');

    // Storage settings
    await expect(codeContent).toContainText('work_dir');
    await expect(codeContent).toContainText('max_level');

    // SSTable configuration
    await expect(codeContent).toContainText('sst_max_size');
    await expect(codeContent).toContainText('mem_table_max_size');
    await expect(codeContent).toContainText('block_size');
  });
});
