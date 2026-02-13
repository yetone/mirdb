/**
 * Unit Tests for Quick Start Section
 * Owner: Scenario 4 - Quick Start Section Implementation
 *
 * Tests HTML structure, content requirements, and accessibility
 */

const { test, expect } = require('@playwright/test');

test.describe('Quick Start Section Unit Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: Quick start section exists with visible heading', async ({ page }) => {
    // Check for quick start section
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeVisible();

    // Check for visible heading
    const title = quickstartSection.locator('.quickstart__title');
    await expect(title).toBeVisible();
    await expect(title).toContainText('Quick Start');

    // Verify aria-labelledby is set correctly
    await expect(quickstartSection).toHaveAttribute('aria-labelledby', 'quickstart-title');
  });

  test('Test Case 2: Section mentions Rust installation requirement', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart');

    // Check for prerequisites section
    const prerequisites = quickstartSection.locator('.quickstart__prerequisites');
    await expect(prerequisites).toBeVisible();

    // Verify Rust is mentioned
    const prerequisitesText = await prerequisites.textContent();
    expect(prerequisitesText.toLowerCase()).toContain('rust');
  });

  test('Test Case 3: At least 3 steps - clone, run, connect', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart');

    // Count the steps
    const steps = quickstartSection.locator('.quickstart__step');
    const stepCount = await steps.count();
    expect(stepCount).toBeGreaterThanOrEqual(3);

    // Verify step content covers clone, run, connect
    const allStepsText = await quickstartSection.locator('.quickstart__steps').textContent();
    const lowerText = allStepsText.toLowerCase();

    // Check for clone step
    expect(lowerText).toMatch(/clone|git/);

    // Check for run step
    expect(lowerText).toMatch(/run|cargo/);

    // Check for connect step
    expect(lowerText).toMatch(/connect|client|telnet/);
  });

  test('Test Case 4: Command cargo run is present in terminal-styled display', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart');

    // Find command blocks
    const commandBlocks = quickstartSection.locator('.quickstart__command');
    const commandBlockCount = await commandBlocks.count();
    expect(commandBlockCount).toBeGreaterThan(0);

    // Check that cargo run command exists
    const allCommandsText = await quickstartSection.textContent();
    expect(allCommandsText).toContain('cargo run');
  });

  test('Test Case 5: Git clone command for MirDB repository is displayed', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart');

    // Check for git clone command
    const allCommandsText = await quickstartSection.textContent();
    expect(allCommandsText).toContain('git clone');
    expect(allCommandsText).toContain('mirdb');
  });

  test('Test Case 7: Each step has a brief explanation', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart');

    // Get all steps
    const steps = quickstartSection.locator('.quickstart__step');
    const stepCount = await steps.count();

    for (let i = 0; i < stepCount; i++) {
      const step = steps.nth(i);

      // Each step should have a description
      const description = step.locator('.quickstart__step-description');
      await expect(description).toBeVisible();

      const descText = await description.textContent();
      expect(descText.length).toBeGreaterThan(10); // Should be a meaningful description
    }
  });

  test('Steps are numbered correctly', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart');

    // Get all step numbers
    const stepNumbers = quickstartSection.locator('.quickstart__step-number');
    const count = await stepNumbers.count();

    for (let i = 0; i < count; i++) {
      const numberText = await stepNumbers.nth(i).textContent();
      expect(numberText.trim()).toBe(String(i + 1));
    }
  });

  test('Each command block has a copy button', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart');

    // Get all command blocks
    const commandBlocks = quickstartSection.locator('.quickstart__command');
    const commandCount = await commandBlocks.count();

    for (let i = 0; i < commandCount; i++) {
      const commandBlock = commandBlocks.nth(i);
      const copyButton = commandBlock.locator('.quickstart__copy, .code-block__copy');
      await expect(copyButton).toBeVisible();
      await expect(copyButton).toHaveAttribute('data-copy-target');
    }
  });

  test('Quick start section has proper semantic structure', async ({ page }) => {
    // Verify quickstart is a section element
    const quickstartSection = page.locator('section#quickstart');
    await expect(quickstartSection).toBeVisible();

    // Verify it has quickstart class
    await expect(quickstartSection).toHaveClass(/quickstart/);

    // Verify steps are in an ordered list
    const orderedList = quickstartSection.locator('ol.quickstart__steps');
    await expect(orderedList).toBeVisible();
  });

  test('Prerequisites section links to Rust installation', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart');
    const prerequisites = quickstartSection.locator('.quickstart__prerequisites');

    // Check for link to Rust installation
    const rustLink = prerequisites.locator('a[href*="rust"]');
    await expect(rustLink).toBeVisible();
    await expect(rustLink).toHaveAttribute('target', '_blank');
  });
});
