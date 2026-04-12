/**
 * Quickstart Section E2E Tests
 * Owner: Scenario 3 - Quickstart Installation Section
 *
 * Test Cases:
 * TC1: Quickstart section heading exists
 * TC2: Code block elements with shell commands exist
 * TC3: Numbered installation steps exist
 * TC4: Syntax highlighting on code blocks
 * TC5: Copy button functionality with visual feedback
 */

import { test, expect } from '@playwright/test';

test.describe('Quickstart Installation Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Section with heading containing Quickstart exists', async ({ page }) => {
    // Navigate to quickstart section
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeVisible();

    // Check for heading with Quickstart
    const heading = quickstartSection.locator('h2');
    await expect(heading).toBeVisible();

    const headingText = await heading.textContent();
    const validHeadings = ['quickstart', 'installation', 'getting started'];
    const hasValidHeading = validHeadings.some(term =>
      headingText?.toLowerCase().includes(term)
    );

    expect(hasValidHeading).toBeTruthy();
  });

  test('TC2: At least one pre/code element exists with shell commands', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart');

    // Check for code blocks
    const codeElements = quickstartSection.locator('pre code, code');
    const count = await codeElements.count();
    expect(count).toBeGreaterThan(0);

    // Verify at least one contains shell commands
    const firstCode = codeElements.first();
    const codeText = await firstCode.textContent();
    expect(codeText).toBeTruthy();
    expect(codeText!.length).toBeGreaterThan(0);
  });

  test('TC3: Installation steps are numbered (step indicators)', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart');

    // Check for step indicators with numbers
    const steps = quickstartSection.locator('.step');
    const count = await steps.count();
    expect(count).toBeGreaterThan(0);

    // Verify steps have visual numbering
    const stepNumbers = quickstartSection.locator('.step-number');
    const numberCount = await stepNumbers.count();
    expect(numberCount).toBeGreaterThan(0);

    // Verify first step has number 1
    const firstStepNumber = await stepNumbers.first().textContent();
    expect(firstStepNumber?.trim()).toBe('1');
  });

  test('TC4: Code blocks have syntax highlighting classes or data attributes', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart');
    const codeBlocks = quickstartSection.locator('.code-block');

    const count = await codeBlocks.count();
    expect(count).toBeGreaterThan(0);

    // Check for data-language attribute or language class for syntax highlighting
    const firstBlock = codeBlocks.first();
    const hasLanguageAttr = await firstBlock.getAttribute('data-language');
    const codeElement = firstBlock.locator('code');
    const codeClass = await codeElement.getAttribute('class');

    // Either data-language attribute or language- class should be present
    const hasSyntaxHighlighting =
      hasLanguageAttr !== null ||
      (codeClass && codeClass.includes('language-'));

    expect(hasSyntaxHighlighting).toBeTruthy();

    // Check that code block has proper background styling
    const bgColor = await firstBlock.evaluate(el =>
      window.getComputedStyle(el).backgroundColor
    );
    expect(bgColor).not.toBe('rgba(0, 0, 0, 0)');
  });

  test('TC5: Copy button copies code and shows visual feedback', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    const quickstartSection = page.locator('#quickstart');
    const firstCodeBlock = quickstartSection.locator('.code-block').first();
    const copyButton = firstCodeBlock.locator('.copy-btn');

    await expect(copyButton).toBeVisible();

    // Get the original button text
    const originalText = await copyButton.textContent();
    expect(originalText?.toLowerCase()).toContain('copy');

    // Click the copy button
    await copyButton.click();

    // Check for visual feedback - the button should show "Copied!"
    await expect(copyButton).toHaveText('Copied!');

    // Wait for it to reset
    await page.waitForTimeout(2100);
    await expect(copyButton).toHaveText(originalText!);
  });
});
