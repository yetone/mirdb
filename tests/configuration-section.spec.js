// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Configuration Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: Configuration section exists with TOML code example', async ({ page }) => {
    // Find the configuration section
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Check that it has a heading
    await expect(configSection.locator('h2')).toContainText('Configuration');

    // Check that TOML code example exists
    const codeBlock = configSection.locator('pre code');
    await expect(codeBlock).toBeVisible();

    // Verify it's TOML format (contains key = value pairs)
    const codeContent = await codeBlock.textContent();
    expect(codeContent).toMatch(/\w+\s*=\s*["'][^"']+["']/);
  });

  test('Test Case 2: Configuration shows addr = "0.0.0.0:12333" as default', async ({ page }) => {
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Find the code block with TOML configuration
    const codeBlock = configSection.locator('pre code');
    const codeContent = await codeBlock.textContent();

    // Verify default address is shown
    expect(codeContent).toContain('addr');
    expect(codeContent).toContain('0.0.0.0:12333');
  });

  test('Test Case 3: Configuration shows work_dir option with example path', async ({ page }) => {
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Find the code block with TOML configuration
    const codeBlock = configSection.locator('pre code');
    const codeContent = await codeBlock.textContent();

    // Verify work_dir option with example path
    expect(codeContent).toContain('work_dir');
    // Should have a path example
    expect(codeContent).toMatch(/work_dir\s*=\s*["']\/[^"']+["']/);
  });

  test('Test Case 4: Configuration shows mem_table_max_size option (4M default)', async ({ page }) => {
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Find the code block with TOML configuration
    const codeBlock = configSection.locator('pre code');
    const codeContent = await codeBlock.textContent();

    // Verify mem_table_max_size option with 4M default
    expect(codeContent).toContain('mem_table_max_size');
    expect(codeContent).toContain('4M');
  });

  test('Configuration section provides customization guidance', async ({ page }) => {
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Check for introductory text or guidance about customization
    const introText = configSection.locator('p, .config-description, .config-intro');
    await expect(introText.first()).toBeVisible();

    // The guidance should mention how to configure or customize
    const sectionText = await configSection.textContent();
    const hasGuidance = sectionText.toLowerCase().includes('config') ||
                        sectionText.toLowerCase().includes('customize') ||
                        sectionText.toLowerCase().includes('option') ||
                        sectionText.toLowerCase().includes('setting');
    expect(hasGuidance).toBe(true);
  });

  test('Configuration section shows sst_max_size option', async ({ page }) => {
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Find the code block with TOML configuration
    const codeBlock = configSection.locator('pre code');
    const codeContent = await codeBlock.textContent();

    // Verify sst_max_size option is shown
    expect(codeContent).toContain('sst_max_size');
  });
});
