// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = path.resolve(__dirname, '../index.html');

test.describe('Configuration Example Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(`file://${indexPath}`);
  });

  test('Test Case 1: Configuration code block exists showing TOML format', async ({ page }) => {
    // Locate configuration example on homepage
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    // Look for code blocks containing configuration
    const codeBlocks = quickStartSection.locator('.code-block');
    const allCodeText = await codeBlocks.allTextContents();

    // Check if any code block contains TOML-style configuration
    const hasConfigBlock = allCodeText.some(text =>
      text.includes('config') ||
      text.includes('.toml') ||
      (text.includes('=') && text.includes('"'))
    );
    expect(hasConfigBlock).toBe(true);

    // Verify TOML language class exists for configuration block
    const tomlCodeBlock = quickStartSection.locator('code.language-toml');
    await expect(tomlCodeBlock).toBeVisible();
  });

  test('Test Case 2: Configuration includes addr setting with port (e.g., 12333)', async ({ page }) => {
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    // Look for TOML configuration block
    const tomlCodeBlock = quickStartSection.locator('code.language-toml');
    await expect(tomlCodeBlock).toBeVisible();

    const configText = await tomlCodeBlock.textContent();

    // Check for addr configuration with port 12333
    expect(configText).toContain('addr');
    expect(configText).toMatch(/addr\s*=\s*["'].*12333["']/);
  });

  test('Test Case 3: Configuration includes work_dir or similar directory setting', async ({ page }) => {
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    // Look for TOML configuration block
    const tomlCodeBlock = quickStartSection.locator('code.language-toml');
    await expect(tomlCodeBlock).toBeVisible();

    const configText = await tomlCodeBlock.textContent();

    // Check for work_dir setting
    expect(configText).toContain('work_dir');
    expect(configText).toMatch(/work_dir\s*=\s*["'].*["']/);
  });

  test('Test Case 4: Configuration uses valid TOML syntax with key = value format', async ({ page }) => {
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    // Look for TOML configuration block
    const tomlCodeBlock = quickStartSection.locator('code.language-toml');
    await expect(tomlCodeBlock).toBeVisible();

    const configText = await tomlCodeBlock.textContent();

    // Verify TOML syntax patterns exist
    // 1. Key = "string_value" pattern
    expect(configText).toMatch(/\w+\s*=\s*["'].*["']/);

    // 2. Key = number pattern
    expect(configText).toMatch(/\w+\s*=\s*\d+/);

    // 3. Multiple key-value pairs
    const keyValuePattern = /(\w+)\s*=\s*(?:["'][^"']*["']|\d+)/g;
    const matches = configText.match(keyValuePattern);
    expect(matches).not.toBeNull();
    expect(matches.length).toBeGreaterThanOrEqual(3); // Should have at least 3 config options

    // 4. Verify specific config keys follow TOML conventions (snake_case or lowercase)
    const keys = ['addr', 'work_dir', 'max_level', 'mem_table_max_size', 'sst_max_size'];
    const foundKeys = keys.filter(key => configText.includes(key));
    expect(foundKeys.length).toBeGreaterThanOrEqual(2); // At least 2 key settings should be present
  });
});
