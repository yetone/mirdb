// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Quick Start Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('should have quick-start section visible', async ({ page }) => {
    const quickStartSection = page.locator('#quickstart');
    await expect(quickStartSection).toBeVisible();
  });

  test('Test Case 1: Installation commands are present', async ({ page }) => {
    // Navigate to quick-start section
    await page.click('a[href="#quickstart"]');

    // Wait for the section to be visible
    const quickStartSection = page.locator('#quickstart');
    await expect(quickStartSection).toBeVisible();

    // Check for installation step
    const installationStep = quickStartSection.locator('.quickstart-step').first();
    await expect(installationStep.locator('h3')).toContainText('Installation');

    // Check for cargo install command
    const codeBlock = installationStep.locator('pre code');
    await expect(codeBlock).toBeVisible();
    const codeContent = await codeBlock.textContent();

    // Verify cargo installation instructions are present
    expect(codeContent).toContain('cargo');
    expect(codeContent).toContain('install');
  });

  test('Test Case 2: Configuration example with addr and work_dir', async ({ page }) => {
    // Navigate to quick-start section
    await page.click('a[href="#quickstart"]');

    const quickStartSection = page.locator('#quickstart');
    await expect(quickStartSection).toBeVisible();

    // Find the configuration step (second step)
    const configStep = quickStartSection.locator('.quickstart-step').nth(1);
    await expect(configStep.locator('h3')).toContainText('Configuration');

    // Check for TOML configuration code block
    const codeBlock = configStep.locator('pre code');
    await expect(codeBlock).toBeVisible();
    const codeContent = await codeBlock.textContent();

    // Verify TOML configuration contains required fields
    expect(codeContent).toContain('addr = "0.0.0.0:12333"');
    expect(codeContent).toContain('work_dir');
  });

  test('Test Case 3: SET command example with STORED response', async ({ page }) => {
    // Navigate to quick-start section
    await page.click('a[href="#quickstart"]');

    const quickStartSection = page.locator('#quickstart');
    await expect(quickStartSection).toBeVisible();

    // Find the usage step (third step)
    const usageStep = quickStartSection.locator('.quickstart-step').nth(2);
    await expect(usageStep.locator('h3')).toContainText('Connect');

    // Check for SET command code block
    const codeBlock = usageStep.locator('pre code');
    await expect(codeBlock).toBeVisible();
    const codeContent = await codeBlock.textContent();

    // Verify SET command example is present
    expect(codeContent).toContain('set mykey 0 0 5');
    expect(codeContent).toContain('STORED');
  });

  test('Test Case 4: GET command example with VALUE response', async ({ page }) => {
    // Navigate to quick-start section
    await page.click('a[href="#quickstart"]');

    const quickStartSection = page.locator('#quickstart');
    await expect(quickStartSection).toBeVisible();

    // Find the usage step (third step)
    const usageStep = quickStartSection.locator('.quickstart-step').nth(2);
    await expect(usageStep.locator('h3')).toContainText('Connect');

    // Check for GET command code block
    const codeBlock = usageStep.locator('pre code');
    await expect(codeBlock).toBeVisible();
    const codeContent = await codeBlock.textContent();

    // Verify GET command example is present
    expect(codeContent).toContain('get mykey');
    expect(codeContent).toContain('VALUE');
  });

  test('Quick Start section has proper structure', async ({ page }) => {
    const quickStartSection = page.locator('#quickstart');

    // Check main heading
    await expect(quickStartSection.locator('h2')).toContainText('Quick Start');

    // Check all three steps are present
    const steps = quickStartSection.locator('.quickstart-step');
    await expect(steps).toHaveCount(3);

    // Verify each step has a heading and code block
    for (let i = 0; i < 3; i++) {
      const step = steps.nth(i);
      await expect(step.locator('h3')).toBeVisible();
      await expect(step.locator('pre code')).toBeVisible();
    }
  });
});
