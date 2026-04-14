/**
 * Quick Start Section E2E Tests
 * Owner: Scenario 3 - Quick Start Section
 *
 * Tests for:
 * - Installation instructions display
 * - Configuration examples
 * - Usage examples with SET/GET/DELETE commands
 * - Code block copy functionality
 * - Syntax highlighting
 */
import { test, expect } from '@playwright/test';
import { SELECTORS, CONTENT } from '../fixtures/test-data';

test.describe('Quick Start Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Scroll to quick start section
    await page.locator(SELECTORS.quickStart).scrollIntoViewIfNeeded();
  });

  test('TC1: Installation code block with cargo install is present', async ({ page }) => {
    const quickStartSection = page.locator(SELECTORS.quickStart);
    await expect(quickStartSection).toBeVisible();

    // Check for installation step
    const installStep = quickStartSection.locator(SELECTORS.quickStartStep).first();
    await expect(installStep).toBeVisible();

    // Verify installation code block contains cargo install command
    const codeBlocks = quickStartSection.locator(SELECTORS.codeBlock);
    const firstCodeBlock = codeBlocks.first();
    await expect(firstCodeBlock).toBeVisible();

    const codeContent = firstCodeBlock.locator(SELECTORS.codeContent);
    await expect(codeContent).toContainText(CONTENT.installationCommand);
  });

  test('TC2: Configuration example shows server startup and basic options', async ({ page }) => {
    const quickStartSection = page.locator(SELECTORS.quickStart);

    // Find the configuration/server start step (second step)
    const steps = quickStartSection.locator(SELECTORS.quickStartStep);
    const configStep = steps.nth(1);
    await expect(configStep).toBeVisible();

    // Check step title mentions starting the server
    const stepTitle = configStep.locator(SELECTORS.stepTitle);
    await expect(stepTitle).toContainText('Start');

    // Verify code block contains server command with options
    const codeBlock = configStep.locator(SELECTORS.codeBlock);
    const codeContent = codeBlock.locator(SELECTORS.codeContent);
    await expect(codeContent).toContainText(CONTENT.serverStartCommand);
    await expect(codeContent).toContainText('--port');
    await expect(codeContent).toContainText('--data-dir');
  });

  test('TC3: Usage example shows SET, GET, DELETE commands with expected output', async ({ page }) => {
    const quickStartSection = page.locator(SELECTORS.quickStart);

    // Find the usage step (third step)
    const steps = quickStartSection.locator(SELECTORS.quickStartStep);
    const usageStep = steps.nth(2);
    await expect(usageStep).toBeVisible();

    // Verify code block contains all basic commands
    const codeBlock = usageStep.locator(SELECTORS.codeBlock);
    const codeContent = codeBlock.locator(SELECTORS.codeContent);

    // Check SET command and output
    await expect(codeContent).toContainText(CONTENT.setCommand);
    await expect(codeContent).toContainText('STORED');

    // Check GET command and output
    await expect(codeContent).toContainText(CONTENT.getCommand);
    await expect(codeContent).toContainText('VALUE');
    await expect(codeContent).toContainText('END');

    // Check DELETE command and output
    await expect(codeContent).toContainText(CONTENT.deleteCommand);
    await expect(codeContent).toContainText('DELETED');
  });

  test('TC4: Copy button on code block copies code content', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    const quickStartSection = page.locator(SELECTORS.quickStart);
    const firstCodeBlock = quickStartSection.locator(SELECTORS.codeBlock).first();
    const copyButton = firstCodeBlock.locator(SELECTORS.copyButton);

    await expect(copyButton).toBeVisible();

    // Get the code content text for comparison
    const codeContent = firstCodeBlock.locator(SELECTORS.codeContent);
    const originalCode = await codeContent.textContent();

    // Click copy button
    await copyButton.click();

    // Check clipboard content
    const clipboardContent = await page.evaluate(async () => {
      return await navigator.clipboard.readText();
    });

    // Verify clipboard contains the code (trim whitespace for comparison)
    expect(clipboardContent.trim()).toBeTruthy();
    expect(originalCode?.trim()).toContain(clipboardContent.trim().split('\n')[0]);
  });

  test('TC5: Code blocks have proper syntax highlighting applied', async ({ page }) => {
    const quickStartSection = page.locator(SELECTORS.quickStart);
    const codeBlocks = quickStartSection.locator(SELECTORS.codeBlock);

    // Verify at least 3 code blocks exist (install, config, usage)
    await expect(codeBlocks).toHaveCount(3);

    // Check first code block has syntax highlighting tokens
    const firstCodeBlock = codeBlocks.first();
    const highlightedTokens = firstCodeBlock.locator('.token');
    const tokenCount = await highlightedTokens.count();

    // Verify syntax highlighting is applied (should have multiple tokens)
    expect(tokenCount).toBeGreaterThan(0);

    // Check for specific token types
    const commentTokens = firstCodeBlock.locator('.token.comment');
    await expect(commentTokens.first()).toBeVisible();

    // Check language label is displayed
    const codeLanguage = firstCodeBlock.locator(SELECTORS.codeLanguage);
    await expect(codeLanguage).toBeVisible();
  });

  test('TC6: Quick Start component renders with all code blocks and copy buttons', async ({ page }) => {
    const quickStartSection = page.locator(SELECTORS.quickStart);
    await expect(quickStartSection).toBeVisible();

    // Verify section title (use .section-title directly since we're already in quickStartSection)
    const sectionTitle = quickStartSection.locator('.section-title');
    await expect(sectionTitle).toBeVisible();
    await expect(sectionTitle).toContainText(CONTENT.quickStartTitle);

    // Verify all 3 steps are present
    const steps = quickStartSection.locator(SELECTORS.quickStartStep);
    await expect(steps).toHaveCount(3);

    // Verify each step has a number, title, and code block
    for (let i = 0; i < 3; i++) {
      const step = steps.nth(i);
      const stepNumber = step.locator(SELECTORS.stepNumber);
      const stepTitle = step.locator(SELECTORS.stepTitle);
      const codeBlock = step.locator(SELECTORS.codeBlock);
      const copyButton = step.locator(SELECTORS.copyButton);

      await expect(stepNumber).toBeVisible();
      await expect(stepNumber).toContainText(String(i + 1));
      await expect(stepTitle).toBeVisible();
      await expect(codeBlock).toBeVisible();
      await expect(copyButton).toBeVisible();
    }

    // Verify code headers with language labels
    const codeHeaders = quickStartSection.locator(SELECTORS.codeHeader);
    await expect(codeHeaders).toHaveCount(3);

    // Verify all copy buttons are functional (have correct aria-label)
    const copyButtons = quickStartSection.locator(SELECTORS.copyButton);
    await expect(copyButtons).toHaveCount(3);
    for (let i = 0; i < 3; i++) {
      await expect(copyButtons.nth(i)).toHaveAttribute('aria-label', 'Copy code to clipboard');
    }
  });
});
