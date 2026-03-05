/**
 * E2E Tests for Scenario 4: Quick Start Installation
 *
 * Tests verify the quick-start section provides installation instructions
 * and usage examples for MirDB.
 */
const { test, expect } = require('@playwright/test');

test.describe('Quick Start Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: Quick-start section is present with appropriate heading', async ({ page }) => {
    // Locate the quickstart section by id
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeVisible();

    // Check for the "Quick Start" heading
    const heading = quickstartSection.locator('[data-testid="quickstart-heading"]');
    await expect(heading).toBeVisible();
    await expect(heading).toContainText('Quick Start');
  });

  test('Test Case 2: Prerequisites mention Rust toolchain requirement', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart');

    // Check for prerequisites section
    const prerequisites = quickstartSection.locator('[data-testid="prerequisites"]');
    await expect(prerequisites).toBeVisible();

    // Verify Rust toolchain is mentioned
    const prereqText = await prerequisites.textContent();
    expect(prereqText).toMatch(/Rust|cargo|rustup/i);
  });

  test('Test Case 3: Code block contains cargo install mirdb command', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart');

    // Check for installation code block
    const installCodeBlock = quickstartSection.locator('[data-testid="install-command"]');
    await expect(installCodeBlock).toBeVisible();

    // Verify the command is present
    const codeText = await installCodeBlock.textContent();
    expect(codeText).toContain('cargo install mirdb');
  });

  test('Test Case 4: Instructions for running the server are provided', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart');

    // Check for run server command
    const runCommand = quickstartSection.locator('[data-testid="run-command"]');
    await expect(runCommand).toBeVisible();

    // Verify the run command is present
    const commandText = await runCommand.textContent();
    expect(commandText).toMatch(/mirdb/);
  });

  test('Test Case 5: Usage examples show GET and SET operations with telnet or nc', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart');

    // Check for usage examples section
    const usageExamples = quickstartSection.locator('[data-testid="usage-examples"]');
    await expect(usageExamples).toBeVisible();

    // Verify GET and SET operations are shown
    const usageText = await usageExamples.textContent();
    expect(usageText).toMatch(/GET|get/);
    expect(usageText).toMatch(/SET|set/);
    expect(usageText).toMatch(/telnet|nc|netcat/i);
  });

  test('Test Case 6: Code blocks use monospace font and have copy functionality', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart');

    // Check all code blocks have monospace styling
    const codeBlocks = quickstartSection.locator('[data-testid="code-block"]');
    const count = await codeBlocks.count();
    expect(count).toBeGreaterThan(0);

    // Verify code blocks have monospace font class (on the pre element inside)
    for (let i = 0; i < count; i++) {
      const codeBlock = codeBlocks.nth(i);
      const preElement = codeBlock.locator('pre');
      const classes = await preElement.getAttribute('class');
      expect(classes).toMatch(/font-mono|monospace/);
    }

    // Check for copy button functionality
    const copyButtons = quickstartSection.locator('[data-testid="copy-button"]');
    const buttonCount = await copyButtons.count();
    expect(buttonCount).toBeGreaterThan(0);

    // Verify copy button is functional (has onclick or is a button)
    const firstButton = copyButtons.first();
    await expect(firstButton).toBeVisible();
  });

  test('Quick-start section has proper structure with all required elements', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeVisible();

    // Verify section has all required subsections
    const heading = quickstartSection.locator('[data-testid="quickstart-heading"]');
    const prerequisites = quickstartSection.locator('[data-testid="prerequisites"]');
    const installCommand = quickstartSection.locator('[data-testid="install-command"]');
    const runCommand = quickstartSection.locator('[data-testid="run-command"]');
    const usageExamples = quickstartSection.locator('[data-testid="usage-examples"]');

    await expect(heading).toBeVisible();
    await expect(prerequisites).toBeVisible();
    await expect(installCommand).toBeVisible();
    await expect(runCommand).toBeVisible();
    await expect(usageExamples).toBeVisible();
  });
});
