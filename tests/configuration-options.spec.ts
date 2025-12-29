import { test, expect } from '@playwright/test';

test.describe('Configuration Options Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('page contains configuration section with TOML format', async ({ page }) => {
    // Test Case 1: Check for configuration section or code block
    // Expected: Page contains configuration options in TOML format

    const configSection = page.locator('[data-testid="configuration-section"]');
    await expect(configSection).toBeVisible();

    // Check that the section contains a code block with TOML configuration
    const configCodeBlock = configSection.locator('[data-testid="config-toml"]');
    await expect(configCodeBlock).toBeVisible();

    // Verify the code block contains TOML-style configuration (key = value format)
    const codeContent = await configCodeBlock.textContent();
    expect(codeContent).toContain('=');
    expect(codeContent).toContain('addr');
    expect(codeContent).toContain('max_level');
  });

  test('configuration shows default address addr = 0.0.0.0:12333', async ({ page }) => {
    // Test Case 2: Verify default address configuration
    // Expected: Configuration shows addr = '0.0.0.0:12333'

    const configCodeBlock = page.locator('[data-testid="config-toml"]');
    await expect(configCodeBlock).toBeVisible();

    const codeContent = await configCodeBlock.textContent();
    expect(codeContent).toContain('addr');
    expect(codeContent).toContain('0.0.0.0:12333');
  });

  test('configuration shows max_level = 7', async ({ page }) => {
    // Test Case 3: Verify default max_level configuration
    // Expected: Configuration shows max_level = 7

    const configCodeBlock = page.locator('[data-testid="config-toml"]');
    await expect(configCodeBlock).toBeVisible();

    const codeContent = await configCodeBlock.textContent();
    expect(codeContent).toContain('max_level');
    expect(codeContent).toContain('7');
  });

  test('configuration shows mem_table_max_size = 4M', async ({ page }) => {
    // Test Case 4: Verify default memtable size
    // Expected: Configuration shows mem_table_max_size = '4M'

    const configCodeBlock = page.locator('[data-testid="config-toml"]');
    await expect(configCodeBlock).toBeVisible();

    const codeContent = await configCodeBlock.textContent();
    expect(codeContent).toContain('mem_table_max_size');
    expect(codeContent).toContain('4M');
  });

  test('configuration section has proper heading', async ({ page }) => {
    // Additional test: Verify section has appropriate heading
    const configSection = page.locator('[data-testid="configuration-section"]');
    await expect(configSection).toBeVisible();

    const heading = configSection.locator('h2');
    await expect(heading).toBeVisible();
    await expect(heading).toContainText(/configuration/i);
  });

  test('configuration shows all default values from mirdb.toml format', async ({ page }) => {
    // Verify all key default configuration values are displayed
    const configCodeBlock = page.locator('[data-testid="config-toml"]');
    await expect(configCodeBlock).toBeVisible();

    const codeContent = await configCodeBlock.textContent();

    // Check all required configuration parameters
    expect(codeContent).toContain('addr');
    expect(codeContent).toContain('0.0.0.0:12333');
    expect(codeContent).toContain('max_level');
    expect(codeContent).toContain('7');
    expect(codeContent).toContain('work_dir');
    expect(codeContent).toContain('/tmp/mirdb');
    expect(codeContent).toContain('sst_max_size');
    expect(codeContent).toContain('100M');
    expect(codeContent).toContain('mem_table_max_size');
    expect(codeContent).toContain('4M');
    expect(codeContent).toContain('block_size');
    expect(codeContent).toContain('4K');
  });
});
