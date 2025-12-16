// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for MirDB Homepage Installation Instructions Section
 *
 * Scenario: Installation Instructions Section
 * Description: Verify installation/getting started instructions are clear and complete
 *
 * Test Case 1: Installation section exists on the page
 * Test Case 2: Prerequisites (Rust/Cargo) are mentioned
 * Test Case 3: Clear installation commands are provided
 * Test Case 4: Configuration file location (etc/mirdb.toml) is mentioned
 */

test.describe('Installation Instructions Section', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to homepage before each test
    await page.goto('/');
  });

  test('Test Case 1: Installation/Getting Started section is present', async ({ page }) => {
    // Look for installation-related sections by various identifiers
    // The homepage has a "Quick Start" section that serves as installation instructions
    const installationSection = page.locator('#code-example, [data-testid="code-example-section"], section:has-text("Quick Start"), section:has-text("Installation"), section:has-text("Getting Started")').first();
    await expect(installationSection).toBeVisible();

    // Verify the section has a heading related to installation/getting started
    const sectionHeading = installationSection.locator('h2, h3').first();
    await expect(sectionHeading).toBeVisible();

    const headingText = await sectionHeading.textContent();
    // The heading should contain relevant keywords
    expect(headingText?.toLowerCase()).toMatch(/quick\s*start|installation|getting\s*started/);
  });

  test('Test Case 2: Prerequisites (Rust/Cargo) are mentioned', async ({ page }) => {
    // Get all text content from the page
    const pageContent = await page.locator('body').textContent();
    const contentLower = pageContent?.toLowerCase() || '';

    // Look for code blocks that contain cargo commands
    const codeBlocks = page.locator('pre, code, [data-testid="code-block"]');
    const codeContent = await codeBlocks.allTextContents();
    const fullCodeText = codeContent.join(' ').toLowerCase();

    // Verify that cargo (Rust package manager) is mentioned in installation instructions
    // This implies Rust is a prerequisite
    const cargoMentioned = fullCodeText.includes('cargo') || contentLower.includes('cargo');
    const rustMentioned = fullCodeText.includes('rust') || contentLower.includes('rust');

    // At minimum, cargo should be mentioned as it's the installation method
    expect(cargoMentioned).toBe(true);

    // Additionally check for installation commands
    const hasInstallCommand = fullCodeText.includes('cargo install') || fullCodeText.includes('cargo build');
    expect(hasInstallCommand).toBe(true);
  });

  test('Test Case 3: Clear installation commands are provided', async ({ page }) => {
    // Scroll to the installation/quick start section
    const installationSection = page.locator('[data-testid="code-example-section"], section:has-text("Quick Start")').first();
    await installationSection.scrollIntoViewIfNeeded();

    // Get all code blocks
    const codeBlocks = page.locator('[data-testid="code-block"], pre code');
    const count = await codeBlocks.count();

    // Should have at least one code block with installation instructions
    expect(count).toBeGreaterThanOrEqual(1);

    // Get all code content
    const codeContent = await codeBlocks.allTextContents();
    const fullCodeText = codeContent.join('\n').toLowerCase();

    // Verify installation command is present
    expect(fullCodeText).toContain('cargo install');

    // Verify the specific package name is mentioned
    expect(fullCodeText).toContain('mirdb');

    // Verify server start command is included
    expect(fullCodeText).toContain('mirdb-server');
  });

  test('Test Case 4: Configuration file location (etc/mirdb.toml) is mentioned', async ({ page }) => {
    // Get all text content from the page, particularly from code blocks
    const codeBlocks = page.locator('[data-testid="code-block"], pre code, pre');
    const codeContent = await codeBlocks.allTextContents();
    const fullCodeText = codeContent.join('\n');

    // Also check the general page content
    const pageContent = await page.locator('body').textContent();

    // Check for the configuration file path
    const configMentioned = fullCodeText.includes('etc/mirdb.toml') ||
                           fullCodeText.includes('mirdb.toml') ||
                           pageContent?.includes('etc/mirdb.toml') ||
                           pageContent?.includes('mirdb.toml');

    expect(configMentioned).toBe(true);

    // Additionally verify the config flag is shown
    const configFlagPresent = fullCodeText.includes('--config') || fullCodeText.includes('-c');
    expect(configFlagPresent).toBe(true);
  });

  test('Installation section has clear visual structure', async ({ page }) => {
    // Navigate to the installation section
    const installationSection = page.locator('[data-testid="code-example-section"]').first();
    await installationSection.scrollIntoViewIfNeeded();

    // Verify there's a clear label for installation code block
    const installationLabel = page.locator('text=Installation').first();
    await expect(installationLabel).toBeVisible();

    // Verify code blocks are visually distinct
    const codeBlocks = page.locator('[data-testid="code-block"]');
    const firstCodeBlock = codeBlocks.first();

    // Check that code block has appropriate styling
    const blockStyles = await firstCodeBlock.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      return {
        fontFamily: computed.fontFamily,
        display: computed.display
      };
    });

    // Should use monospace font for code
    expect(blockStyles.fontFamily.toLowerCase()).toMatch(/mono|courier|consolas/);
  });

  test('Installation commands are copy-friendly', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    // Navigate to installation section
    const installationSection = page.locator('[data-testid="code-example-section"]');
    await installationSection.scrollIntoViewIfNeeded();

    // Find copy button for the installation code
    const copyButton = page.getByTestId('copy-button').first();
    await expect(copyButton).toBeVisible();

    // Click copy and verify content
    await copyButton.click();

    // Verify clipboard has the installation content
    const clipboardText = await page.evaluate(async () => {
      return await navigator.clipboard.readText();
    });

    // The copied content should contain the installation command
    expect(clipboardText.toLowerCase()).toContain('cargo');
  });
});
