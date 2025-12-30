const { test, expect } = require('@playwright/test');

/**
 * US-3 Quick Start User Story Validation Tests
 * Scenario: Validate acceptance criteria for US-3: New developer can quickly get started
 *
 * Acceptance Criteria:
 * - Given I am on the homepage
 * - When I scroll to the Quick Start section
 * - Then I see clear installation instructions
 * - And I see example commands I can copy and run
 */

test.describe('US-3: Quick Start - User Story Validation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Check quick start section content
   * Expected: Section contains clear installation instructions
   */
  test('Test Case 1: Quick start section contains clear installation instructions', async ({ page }) => {
    // Step 1: Navigate to quick start section
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeVisible();

    // Step 2: Verify instructions clarity - Check that installation steps are clear and complete

    // Verify section has a clear header
    const sectionHeader = quickstartSection.locator('h2');
    await expect(sectionHeader).toBeVisible();
    await expect(sectionHeader).toHaveText('Quick Start');

    // Verify there is a description that indicates quick setup
    const headerDescription = quickstartSection.locator('.quickstart-header p');
    await expect(headerDescription).toContainText('Get up and running');

    // Verify Step 1: Clone and Build instructions exist
    const step1 = quickstartSection.locator('.quickstart-step').first();
    await expect(step1).toBeVisible();
    const step1Title = step1.locator('h3');
    await expect(step1Title).toContainText('Clone and Build');

    // Verify git clone command is present
    const step1Code = step1.locator('.code-block');
    await expect(step1Code).toBeVisible();
    const step1Text = await step1Code.textContent();
    expect(step1Text).toContain('git clone');
    expect(step1Text).toContain('cargo build');

    // Verify Step 2: Start the Server instructions exist
    const step2 = quickstartSection.locator('.quickstart-step').nth(1);
    await expect(step2).toBeVisible();
    const step2Title = step2.locator('h3');
    await expect(step2Title).toContainText('Start the Server');

    // Verify server start command is present
    const step2Code = step2.locator('.code-block');
    await expect(step2Code).toBeVisible();
    const step2Text = await step2Code.textContent();
    expect(step2Text).toContain('mirdb-server');

    // Verify Step 3: Connect and Use instructions exist
    const step3 = quickstartSection.locator('.quickstart-step').nth(2);
    await expect(step3).toBeVisible();
    const step3Title = step3.locator('h3');
    await expect(step3Title).toContainText('Connect and Use');

    // Verify connection command is present
    const step3Code = step3.locator('.code-block');
    await expect(step3Code).toBeVisible();
    const step3Text = await step3Code.textContent();
    expect(step3Text).toContain('telnet');

    // Verify all three steps have step numbers (numbered instructions)
    const stepNumbers = quickstartSection.locator('.step-number');
    const stepCount = await stepNumbers.count();
    expect(stepCount).toBe(3);
  });

  /**
   * Test Case 2: Check code block selectability
   * Expected: Code examples can be selected and copied
   */
  test('Test Case 2: Code examples can be selected and copied', async ({ page }) => {
    // Step 1: Navigate to quick start section
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeVisible();

    // Step 3: Verify copyable examples - Code blocks should be selectable

    // Find all code blocks in the quick start section
    const codeBlocks = quickstartSection.locator('.code-block');
    const codeBlockCount = await codeBlocks.count();

    // Should have at least one code block
    expect(codeBlockCount).toBeGreaterThan(0);

    // Test each code block for selectability
    for (let i = 0; i < codeBlockCount; i++) {
      const codeBlock = codeBlocks.nth(i);

      // Verify code block is visible
      await expect(codeBlock).toBeVisible();

      // Verify code block contains a pre and/or code element (standard for copyable code)
      const isPreElement = await codeBlock.evaluate(el => el.tagName.toLowerCase() === 'pre');
      const hasCodeElement = await codeBlock.locator('code').count() > 0;
      expect(isPreElement || hasCodeElement).toBeTruthy();

      // Test that the code block text can be selected (not empty)
      const textContent = await codeBlock.textContent();
      expect(textContent.trim().length).toBeGreaterThan(0);

      // Verify the code block has appropriate styling for selection (monospace font family)
      const fontFamily = await codeBlock.evaluate(el => {
        const style = window.getComputedStyle(el);
        return style.fontFamily;
      });
      expect(fontFamily.toLowerCase()).toMatch(/mono|monaco|menlo|consolas|courier/i);

      // Verify user-select CSS property allows text selection (not 'none')
      const userSelect = await codeBlock.evaluate(el => {
        const style = window.getComputedStyle(el);
        return style.userSelect || style.webkitUserSelect || 'auto';
      });
      expect(userSelect).not.toBe('none');

      // Verify the code element within is also selectable
      const codeElement = codeBlock.locator('code');
      if (await codeElement.count() > 0) {
        const codeUserSelect = await codeElement.first().evaluate(el => {
          const style = window.getComputedStyle(el);
          return style.userSelect || style.webkitUserSelect || 'auto';
        });
        expect(codeUserSelect).not.toBe('none');
      }
    }
  });

  /**
   * Additional test: Verify code blocks contain example commands (SET, GET, DELETE)
   * This ensures the examples are actually useful for new developers
   */
  test('Test Case 2b: Code examples contain runnable commands', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeVisible();

    // Get all code block content
    const codeBlocks = quickstartSection.locator('.code-block');
    let allCodeContent = '';

    const count = await codeBlocks.count();
    for (let i = 0; i < count; i++) {
      const text = await codeBlocks.nth(i).textContent();
      allCodeContent += ' ' + text;
    }

    // Verify essential commands are present for a complete quick start experience
    expect(allCodeContent).toContain('git clone');
    expect(allCodeContent).toContain('cargo build');
    expect(allCodeContent.toLowerCase()).toContain('set ');
    expect(allCodeContent.toLowerCase()).toContain('get ');
    expect(allCodeContent.toLowerCase()).toContain('delete ');
  });
});
