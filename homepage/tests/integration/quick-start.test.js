/**
 * Quick Start Section Integration Tests
 * Owner: Scenario 4 - Quick Start Section
 *
 * Test cases:
 * - Installation command is present
 * - Code block has syntax highlighting
 * - Copy button is functional
 * - Usage example shows set/get commands
 */

import { test, expect } from '@playwright/test';

test.describe('Quick Start Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Scroll to quickstart section
    await page.locator('#quickstart').scrollIntoViewIfNeeded();
  });

  test('TC1: Installation command block is present with cargo command', async ({ page }) => {
    // Check quickstart section exists
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeVisible();

    // Check that installation code block exists with cargo command
    const installCode = page.locator('#install-code');
    await expect(installCode).toBeVisible();
    await expect(installCode).toContainText('cargo install mirdb');
  });

  test('TC2: Code block has syntax highlighting applied', async ({ page }) => {
    // Check that code blocks have the data-language attribute for styling
    const codeBlocks = page.locator('.code-block[data-language]');
    await expect(codeBlocks.first()).toBeVisible();

    // Verify the code block has proper structure
    const codeBlockHeader = page.locator('.code-block-header').first();
    await expect(codeBlockHeader).toBeVisible();

    // Check that language indicator is shown
    const langIndicator = page.locator('.code-block-lang').first();
    await expect(langIndicator).toBeVisible();
    await expect(langIndicator).toHaveText('bash');

    // Check for syntax highlighting tokens (comments should be styled)
    const commentToken = page.locator('.token.comment').first();
    await expect(commentToken).toBeVisible();
    await expect(commentToken).toContainText('#');
  });

  test('TC3: Copy button copies command text to clipboard', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    // Find the copy button for install command
    const copyBtn = page.locator('[data-copy-target="install-code"]');
    await expect(copyBtn).toBeVisible();

    // Click the copy button
    await copyBtn.click();

    // Read from clipboard and verify content
    const clipboardText = await page.evaluate(async () => {
      return await navigator.clipboard.readText();
    });

    expect(clipboardText).toContain('cargo install mirdb');
  });

  test('TC4: Visual feedback confirms copy action', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    const copyBtn = page.locator('[data-copy-target="install-code"]');
    const copyBtnText = copyBtn.locator('.copy-btn-text');
    const copyIcon = copyBtn.locator('.copy-icon');
    const checkIcon = copyBtn.locator('.check-icon');

    // Initial state
    await expect(copyBtnText).toHaveText('Copy');

    // Click copy
    await copyBtn.click();

    // Check for visual feedback - button should show "Copied!" and checkmark
    await expect(copyBtnText).toHaveText('Copied!');
    await expect(copyBtn).toHaveClass(/copied/);

    // Check icon change - copy icon hidden, check icon visible
    await expect(copyIcon).toHaveCSS('display', 'none');
    await expect(checkIcon).toHaveCSS('display', 'block');

    // Aria label should update
    await expect(copyBtn).toHaveAttribute('aria-label', 'Copied to clipboard');

    // Wait for reset (2 seconds)
    await page.waitForTimeout(2100);

    // Should be back to original state
    await expect(copyBtnText).toHaveText('Copy');
    await expect(copyBtn).not.toHaveClass(/copied/);
    await expect(copyBtn).toHaveAttribute('aria-label', 'Copy to clipboard');
  });

  test('TC5: Usage example shows set and get commands', async ({ page }) => {
    // Check usage code block contains set command
    const usageCode = page.locator('#usage-code');
    await expect(usageCode).toBeVisible();
    await expect(usageCode).toContainText('set mykey');
    await expect(usageCode).toContainText('get mykey');
  });

  test('Quick Start section has proper heading', async ({ page }) => {
    const heading = page.locator('#quickstart-title');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText('Quick Start');
  });

  test('Quick Start section has step numbers', async ({ page }) => {
    // Check for step numbers (3 steps: Install, Use, Rust example)
    const stepNumbers = page.locator('.quickstart-step-number');
    await expect(stepNumbers).toHaveCount(3);

    // First step should be "1"
    await expect(stepNumbers.nth(0)).toHaveText('1');
    // Second step should be "2"
    await expect(stepNumbers.nth(1)).toHaveText('2');
    // Third step should be "3"
    await expect(stepNumbers.nth(2)).toHaveText('3');
  });

  test('Quick Start section has step titles', async ({ page }) => {
    const stepTitles = page.locator('.quickstart-step-title');
    await expect(stepTitles).toHaveCount(3);

    // First step: Install
    await expect(stepTitles.nth(0)).toContainText('Install MirDB');
    // Second step: Start Using
    await expect(stepTitles.nth(1)).toContainText('Start Using MirDB');
    // Third step: Use with Rust
    await expect(stepTitles.nth(2)).toContainText('Use with Rust');
  });

  test('Both copy buttons are functional', async ({ page, context }) => {
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    // Copy install command
    const installCopyBtn = page.locator('[data-copy-target="install-code"]');
    await installCopyBtn.click();

    let clipboardText = await page.evaluate(async () => {
      return await navigator.clipboard.readText();
    });
    expect(clipboardText).toContain('cargo install mirdb');

    // Wait for reset
    await page.waitForTimeout(2100);

    // Copy usage example
    const usageCopyBtn = page.locator('[data-copy-target="usage-code"]');
    await usageCopyBtn.click();

    clipboardText = await page.evaluate(async () => {
      return await navigator.clipboard.readText();
    });
    expect(clipboardText).toContain('set mykey');
    expect(clipboardText).toContain('get mykey');
  });

  test('Code blocks are scrollable on small screens', async ({ page }) => {
    // Set viewport to mobile size
    await page.setViewportSize({ width: 375, height: 667 });

    // Scroll to quickstart
    await page.locator('#quickstart').scrollIntoViewIfNeeded();

    // Code block should have overflow-x: auto
    const codeBlockPre = page.locator('.code-block pre').first();
    await expect(codeBlockPre).toHaveCSS('overflow-x', 'auto');
  });

  test('Copy buttons are keyboard accessible', async ({ page, context }) => {
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    const copyBtn = page.locator('[data-copy-target="install-code"]');

    // Focus the button
    await copyBtn.focus();
    await expect(copyBtn).toBeFocused();

    // Press Enter to activate
    await page.keyboard.press('Enter');

    // Verify copy happened
    const copyBtnText = copyBtn.locator('.copy-btn-text');
    await expect(copyBtnText).toHaveText('Copied!');
  });

  test('Quick Start section uses semantic HTML', async ({ page }) => {
    // Section should exist
    const section = page.locator('section#quickstart');
    await expect(section).toBeVisible();

    // Should have aria-labelledby pointing to title
    await expect(section).toHaveAttribute('aria-labelledby', 'quickstart-title');

    // Heading should be h2
    const heading = page.locator('#quickstart-title');
    const tagName = await heading.evaluate(el => el.tagName);
    expect(tagName).toBe('H2');
  });
});
