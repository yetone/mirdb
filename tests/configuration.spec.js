const { test, expect } = require('@playwright/test');

test.describe('Configuration Options Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: TOML configuration example is displayed with syntax highlighting', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('#configuration');
    await configSection.scrollIntoViewIfNeeded();
    await expect(configSection).toBeVisible();

    // Query for configuration code block
    const configCodeBlock = page.locator('[data-testid="config-toml-block"]');
    await expect(configCodeBlock).toBeVisible();

    // Verify TOML content is present
    const codeText = await configCodeBlock.textContent();
    expect(codeText).toContain('addr');
    expect(codeText).toContain('work_dir');
    expect(codeText).toContain('mem_table_max_size');

    // Verify syntax highlighting is applied (code block has specific styling class)
    const codeBlockClasses = await configCodeBlock.getAttribute('class');
    expect(codeBlockClasses).toContain('code-block');

    // Verify code element is present inside
    const codeElement = configCodeBlock.locator('code');
    await expect(codeElement).toBeVisible();
  });

  test('TC2: Default address 0.0.0.0:12333 is shown in configuration', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('#configuration');
    await configSection.scrollIntoViewIfNeeded();
    await expect(configSection).toBeVisible();

    // Find the configuration code block
    const configCodeBlock = page.locator('[data-testid="config-toml-block"]');
    await expect(configCodeBlock).toBeVisible();

    // Verify addr configuration option is displayed with default value
    const codeText = await configCodeBlock.textContent();
    expect(codeText).toContain('addr');
    expect(codeText).toContain('0.0.0.0:12333');
  });

  test('TC3: work_dir parameter is shown with default value', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('#configuration');
    await configSection.scrollIntoViewIfNeeded();
    await expect(configSection).toBeVisible();

    // Find the configuration code block
    const configCodeBlock = page.locator('[data-testid="config-toml-block"]');
    await expect(configCodeBlock).toBeVisible();

    // Verify work_dir configuration option is displayed with default value
    const codeText = await configCodeBlock.textContent();
    expect(codeText).toContain('work_dir');
    expect(codeText).toContain('/tmp/mirdb');
  });

  test('TC4: mem_table_max_size parameter is shown with default 4M', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('#configuration');
    await configSection.scrollIntoViewIfNeeded();
    await expect(configSection).toBeVisible();

    // Find the configuration code block
    const configCodeBlock = page.locator('[data-testid="config-toml-block"]');
    await expect(configCodeBlock).toBeVisible();

    // Verify mem_table_max_size configuration option is displayed with default value
    const codeText = await configCodeBlock.textContent();
    expect(codeText).toContain('mem_table_max_size');
    expect(codeText).toContain('4M');
  });
});
