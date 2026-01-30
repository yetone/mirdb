/**
 * MirDB Landing Page - Getting Started Section E2E Tests
 * Owner: Scenario 6 - Getting Started Section
 *
 * Tests verify:
 * - Installation instructions for cargo install
 * - Build from source instructions (git clone, cargo build)
 * - Quick start commands to run MirDB
 * - Link to comprehensive documentation
 * - Steps are numbered/ordered
 */

const { test, expect } = require('@playwright/test');
const { setupPage } = require('../helpers/test-utils');

test.describe('Getting Started Section', () => {
  test.beforeEach(async ({ page }) => {
    await setupPage(page);
  });

  // Test Case 1: Check for cargo install instructions
  test('TC1: Instructions show how to install via cargo', async ({ page }) => {
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Look for cargo install instructions
    const sectionText = await gettingStartedSection.textContent();

    // Verify cargo install command is present
    expect(sectionText.toLowerCase()).toContain('cargo');
    expect(sectionText.toLowerCase()).toContain('install');

    // Check for the actual cargo install command in a code block
    const cargoInstallCode = gettingStartedSection.locator('code, pre');
    const codeBlocks = await cargoInstallCode.allTextContents();
    const hasCargoInstall = codeBlocks.some(code =>
      code.includes('cargo install') || code.includes('cargo build')
    );
    expect(hasCargoInstall).toBe(true);
  });

  // Test Case 2: Check for build from source instructions
  test('TC2: Instructions show git clone and cargo build commands', async ({ page }) => {
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    const sectionText = await gettingStartedSection.textContent();

    // Verify git clone and cargo build commands are mentioned
    expect(sectionText.toLowerCase()).toContain('git clone');
    expect(sectionText.toLowerCase()).toContain('cargo build');

    // Check for code blocks containing these commands
    const codeBlocks = gettingStartedSection.locator('code, pre');
    const allCode = await codeBlocks.allTextContents();

    const hasGitClone = allCode.some(code => code.includes('git clone'));
    const hasCargoBuild = allCode.some(code => code.includes('cargo build'));

    expect(hasGitClone).toBe(true);
    expect(hasCargoBuild).toBe(true);
  });

  // Test Case 3: Check for quick start commands
  test('TC3: Minimal commands to run MirDB are provided', async ({ page }) => {
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Look for quick start section or run commands
    const sectionText = await gettingStartedSection.textContent();

    // Verify there are commands to run MirDB
    const hasRunCommand =
      sectionText.toLowerCase().includes('run') ||
      sectionText.toLowerCase().includes('start') ||
      sectionText.toLowerCase().includes('./mirdb') ||
      sectionText.toLowerCase().includes('cargo run');

    expect(hasRunCommand).toBe(true);

    // Check for code blocks with execution commands
    const codeBlocks = gettingStartedSection.locator('code, pre');
    const allCode = await codeBlocks.allTextContents();

    // Should have at least one command for running MirDB
    const hasExecutionCommand = allCode.some(code =>
      code.includes('cargo run') ||
      code.includes('./mirdb') ||
      code.includes('mirdb')
    );
    expect(hasExecutionCommand).toBe(true);
  });

  // Test Case 4: Check for documentation link
  test('TC4: Link to comprehensive documentation is present', async ({ page }) => {
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Look for documentation link
    const docLink = gettingStartedSection.locator('a').filter({
      hasText: /documentation|docs|readme|github/i
    });

    // Should have at least one documentation link
    const linkCount = await docLink.count();
    expect(linkCount).toBeGreaterThan(0);

    // Verify the link has an href attribute
    const firstLink = docLink.first();
    await expect(firstLink).toHaveAttribute('href');

    const href = await firstLink.getAttribute('href');
    expect(href).toBeTruthy();
  });

  // Test Case 5: Installation steps are numbered/ordered
  test('TC5: Steps are presented in clear, sequential order', async ({ page }) => {
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Check for ordered list elements or numbered steps
    const orderedLists = gettingStartedSection.locator('ol');
    const numberedSteps = gettingStartedSection.locator('[class*="step"], .step');
    const stepHeaders = gettingStartedSection.locator('h3, h4');

    const olCount = await orderedLists.count();
    const stepsCount = await numberedSteps.count();
    const headersCount = await stepHeaders.count();

    // Should have either ordered lists, numbered step elements, or sequential headers
    const hasSequentialStructure = olCount > 0 || stepsCount > 0 || headersCount >= 2;
    expect(hasSequentialStructure).toBe(true);

    // Verify the section has clear structure with multiple steps
    const sectionText = await gettingStartedSection.textContent();

    // Check for step indicators (numbers, "Step 1", "Option 1", etc.)
    const hasStepIndicators =
      /step\s*\d/i.test(sectionText) ||
      /option\s*\d/i.test(sectionText) ||
      /\d\.\s+\w/m.test(sectionText) ||
      olCount > 0;

    expect(hasStepIndicators).toBe(true);
  });

  // Additional test: Section accessibility
  test('Getting Started section has proper accessibility attributes', async ({ page }) => {
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Verify section has aria-labelledby
    await expect(gettingStartedSection).toHaveAttribute('aria-labelledby', 'getting-started-title');

    // Verify heading exists
    const sectionTitle = page.locator('#getting-started-title');
    await expect(sectionTitle).toBeVisible();

    // Verify the heading text is correct
    const titleText = await sectionTitle.textContent();
    expect(titleText.toLowerCase()).toContain('getting started');
  });

  // Additional test: Code blocks are properly formatted
  test('Code blocks are properly formatted and readable', async ({ page }) => {
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Check for code blocks
    const codeBlocks = gettingStartedSection.locator('pre code, .code-block code');
    const codeCount = await codeBlocks.count();

    // Should have code blocks for commands
    expect(codeCount).toBeGreaterThan(0);

    // Verify code blocks have content
    const firstCodeBlock = codeBlocks.first();
    const codeText = await firstCodeBlock.textContent();
    expect(codeText.trim().length).toBeGreaterThan(0);
  });
});
