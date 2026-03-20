/**
 * Quick Start Section E2E Tests
 * Owner: Scenario 5 - Quick Start Installation Guide
 *
 * Test cases:
 * - Installation instructions visible
 * - Clone/install commands displayed
 * - Build/run commands displayed
 * - Configuration options listed
 * - Copy buttons functional
 */

const { test, expect } = require('@playwright/test');

test.describe('Quick Start Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('quick start section is visible with installation instructions', async ({ page }) => {
    // Navigate to quick start section
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeVisible();

    // Check for headline
    const headline = page.locator('#quickstart-headline');
    await expect(headline).toBeVisible();
    await expect(headline).toHaveText('Quick Start');

    // Check for subtitle
    const subtitle = page.locator('.quickstart__subtitle');
    await expect(subtitle).toBeVisible();
    await expect(subtitle).toContainText('MirDB');
  });

  test('displays git clone command', async ({ page }) => {
    const cloneCodeBlock = page.locator('[data-testid="code-block-clone"]');
    await expect(cloneCodeBlock).toBeVisible();

    const cloneCode = cloneCodeBlock.locator('code');
    await expect(cloneCode).toContainText('git clone');
    await expect(cloneCode).toContainText('mirdb');
  });

  test('displays cargo install command', async ({ page }) => {
    const cargoCodeBlock = page.locator('[data-testid="code-block-cargo"]');
    await expect(cargoCodeBlock).toBeVisible();

    const cargoCode = cargoCodeBlock.locator('code');
    await expect(cargoCode).toContainText('cargo install mirdb');
  });

  test('displays build command', async ({ page }) => {
    const buildCodeBlock = page.locator('[data-testid="code-block-build"]');
    await expect(buildCodeBlock).toBeVisible();

    const buildCode = buildCodeBlock.locator('code');
    await expect(buildCode).toContainText('cargo build');
  });

  test('displays run command', async ({ page }) => {
    const runCodeBlock = page.locator('[data-testid="code-block-run"]');
    await expect(runCodeBlock).toBeVisible();

    const runCode = runCodeBlock.locator('code');
    await expect(runCode).toContainText('mirdb');
  });

  test('displays configuration options table with default values', async ({ page }) => {
    const configSection = page.locator('[data-testid="quickstart-config"]');
    await expect(configSection).toBeVisible();

    const configTable = page.locator('[data-testid="config-table"]');
    await expect(configTable).toBeVisible();

    // Check for key configuration parameters
    await expect(configTable).toContainText('addr');
    await expect(configTable).toContainText('0.0.0.0:12333');
    await expect(configTable).toContainText('work_dir');
    await expect(configTable).toContainText('/tmp/mirdb');
    await expect(configTable).toContainText('max_level');
    await expect(configTable).toContainText('mem_table_max_size');
    await expect(configTable).toContainText('4M');
    await expect(configTable).toContainText('sst_max_size');
    await expect(configTable).toContainText('100M');
    await expect(configTable).toContainText('block_size');
    await expect(configTable).toContainText('4K');
  });

  test('copy button works for installation commands', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    // Find the clone code block copy button
    const cloneCodeBlock = page.locator('[data-testid="code-block-clone"]');
    const copyButton = cloneCodeBlock.locator('.quickstart__copy-btn');

    await expect(copyButton).toBeVisible();

    // Click copy button
    await copyButton.click();

    // Check for visual feedback (button text changes to "Copied!")
    const copyText = copyButton.locator('.quickstart__copy-text');
    await expect(copyText).toHaveText('Copied!');

    // Verify the button has the copied class
    await expect(copyButton).toHaveClass(/copied/);

    // Read clipboard content and verify
    const clipboardContent = await page.evaluate(() => navigator.clipboard.readText());
    expect(clipboardContent).toContain('git clone');
    expect(clipboardContent).toContain('mirdb');

    // Wait for the feedback to reset (2 seconds)
    await expect(copyText).toHaveText('Copy', { timeout: 3000 });
  });

  test('copy button works for cargo install command', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    const cargoCodeBlock = page.locator('[data-testid="code-block-cargo"]');
    const copyButton = cargoCodeBlock.locator('.quickstart__copy-btn');

    await copyButton.click();

    // Verify clipboard content
    const clipboardContent = await page.evaluate(() => navigator.clipboard.readText());
    expect(clipboardContent).toContain('cargo install mirdb');
  });

  test('copy button works for configuration example', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    const configCodeBlock = page.locator('[data-testid="code-block-config"]');
    const copyButton = configCodeBlock.locator('.quickstart__copy-btn');

    await copyButton.click();

    // Verify clipboard content
    const clipboardContent = await page.evaluate(() => navigator.clipboard.readText());
    expect(clipboardContent).toContain('addr');
    expect(clipboardContent).toContain('0.0.0.0:12333');
  });

  test('step numbers are displayed correctly', async ({ page }) => {
    const step1 = page.locator('[data-testid="quickstart-step-1"] .quickstart__step-number');
    const step2 = page.locator('[data-testid="quickstart-step-2"] .quickstart__step-number');
    const step3 = page.locator('[data-testid="quickstart-step-3"] .quickstart__step-number');

    await expect(step1).toHaveText('1');
    await expect(step2).toHaveText('2');
    await expect(step3).toHaveText('3');
  });

  test('quick start section has proper accessibility attributes', async ({ page }) => {
    const section = page.locator('#quickstart');
    await expect(section).toHaveAttribute('aria-labelledby', 'quickstart-headline');

    // Check that copy buttons have aria-labels
    const copyButtons = page.locator('.quickstart__copy-btn');
    const count = await copyButtons.count();

    for (let i = 0; i < count; i++) {
      const button = copyButtons.nth(i);
      await expect(button).toHaveAttribute('aria-label');
    }
  });

  test('can navigate to quick start section from hero CTA', async ({ page }) => {
    // Click the "Get Started" CTA button in hero section
    const getStartedBtn = page.locator('[data-testid="cta-get-started"]');
    await expect(getStartedBtn).toBeVisible();

    await getStartedBtn.click();

    // Wait for smooth scroll
    await page.waitForTimeout(500);

    // Verify quick start section is in view
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeInViewport();
  });
});
