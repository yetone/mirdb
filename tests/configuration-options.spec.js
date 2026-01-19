// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E tests for Configuration Options Display
 * Scenario: Verify that configuration options and defaults are shown as specified in REQ-7
 */

test.describe('Configuration Options Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Check for default port configuration
   * Input: Check for default port configuration
   * Expected: Configuration shows default port 12333 (addr = '0.0.0.0:12333')
   */
  test('TC1: Configuration shows default port 12333 (addr = "0.0.0.0:12333")', async ({ page }) => {
    // Navigate to the getting started section
    const gettingStartedSection = page.locator('#getting-started');
    await gettingStartedSection.scrollIntoViewIfNeeded();
    await expect(gettingStartedSection).toBeVisible();

    // Find the configuration code block (step 2. Configure)
    const configStep = gettingStartedSection.locator('.step:has(h3:text("Configure"))');
    await expect(configStep).toBeVisible();

    // Find the code block within the configure step
    const codeBlock = configStep.locator('pre code');
    await expect(codeBlock).toBeVisible();

    // Verify that the configuration shows the default port
    const codeContent = await codeBlock.textContent();
    expect(codeContent).toContain('addr');
    expect(codeContent).toContain('0.0.0.0:12333');
  });

  /**
   * Test Case 2: Check for storage configuration options
   * Input: Check for storage configuration options
   * Expected: Configuration shows max_level, sst_max_size, and mem_table_max_size options
   */
  test('TC2: Configuration shows storage options (max_level, sst_max_size, mem_table_max_size)', async ({ page }) => {
    // Navigate to the getting started section
    const gettingStartedSection = page.locator('#getting-started');
    await gettingStartedSection.scrollIntoViewIfNeeded();
    await expect(gettingStartedSection).toBeVisible();

    // Find the configuration code block (step 2. Configure)
    const configStep = gettingStartedSection.locator('.step:has(h3:text("Configure"))');
    await expect(configStep).toBeVisible();

    // Find the code block within the configure step
    const codeBlock = configStep.locator('pre code');
    await expect(codeBlock).toBeVisible();

    // Verify that the configuration shows storage configuration options
    const codeContent = await codeBlock.textContent();

    // Check for max_level option
    expect(codeContent).toContain('max_level');

    // Check for sst_max_size option
    expect(codeContent).toContain('sst_max_size');

    // Check for mem_table_max_size option
    expect(codeContent).toContain('mem_table_max_size');
  });

  /**
   * Test Case 3: Check for work_dir configuration
   * Input: Check for work_dir configuration
   * Expected: Configuration shows work_dir option with default value
   */
  test('TC3: Configuration shows work_dir option with default value', async ({ page }) => {
    // Navigate to the getting started section
    const gettingStartedSection = page.locator('#getting-started');
    await gettingStartedSection.scrollIntoViewIfNeeded();
    await expect(gettingStartedSection).toBeVisible();

    // Find the configuration code block (step 2. Configure)
    const configStep = gettingStartedSection.locator('.step:has(h3:text("Configure"))');
    await expect(configStep).toBeVisible();

    // Find the code block within the configure step
    const codeBlock = configStep.locator('pre code');
    await expect(codeBlock).toBeVisible();

    // Verify that the configuration shows work_dir with a default value
    const codeContent = await codeBlock.textContent();
    expect(codeContent).toContain('work_dir');

    // Verify it has a default value (path-like string)
    expect(codeContent).toMatch(/work_dir\s*=\s*"[^"]+"/);
  });

  /**
   * Additional test: Verify all configuration options are displayed together
   */
  test('Configuration section displays all required options together', async ({ page }) => {
    // Navigate to the getting started section
    const gettingStartedSection = page.locator('#getting-started');
    await gettingStartedSection.scrollIntoViewIfNeeded();

    // Find the configuration code block
    const configStep = gettingStartedSection.locator('.step:has(h3:text("Configure"))');
    const codeBlock = configStep.locator('pre code');
    const codeContent = await codeBlock.textContent();

    // Verify all required configuration options from REQ-7 are present
    const requiredOptions = [
      'addr',
      'max_level',
      'work_dir',
      'sst_max_size',
      'mem_table_max_size'
    ];

    for (const option of requiredOptions) {
      expect(codeContent).toContain(option);
    }

    // Verify default values are present
    expect(codeContent).toContain('12333'); // default port
    expect(codeContent).toContain('7');     // max_level default
  });

  /**
   * Additional test: Configuration section is within getting-started section
   */
  test('Configuration is part of getting-started section', async ({ page }) => {
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Verify the getting started section contains configuration step
    const configStep = gettingStartedSection.locator('.step:has(h3:text("Configure"))');
    await expect(configStep).toBeVisible();

    // Verify it's positioned after the clone/build step
    const steps = gettingStartedSection.locator('.step');
    const stepCount = await steps.count();
    expect(stepCount).toBeGreaterThanOrEqual(2);
  });
});
