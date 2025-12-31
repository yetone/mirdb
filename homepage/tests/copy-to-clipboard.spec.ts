import { test, expect } from '@playwright/test';

test.describe('Copy to Clipboard Functionality', () => {
  test.beforeEach(async ({ page, context }) => {
    // Grant clipboard permissions for the browser context
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);
    await page.goto('/');
  });

  test('TC1: Each code block has a copy button visible', async ({ page }) => {
    // Navigate to quick start section where code blocks are located
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    // Find all code blocks in the quick start section
    const codeBlocks = quickStartSection.locator('.code-block');
    const codeBlockCount = await codeBlocks.count();

    // Verify there is at least one code block
    expect(codeBlockCount).toBeGreaterThan(0);

    // Verify each code block has a copy button
    for (let i = 0; i < codeBlockCount; i++) {
      const codeBlock = codeBlocks.nth(i);
      const copyButton = codeBlock.locator('.copy-button, [data-testid="copy-button"], button[aria-label*="copy" i], button[title*="copy" i]');
      await expect(copyButton).toBeVisible();
    }
  });

  test('TC2: Click copy button on installation command copies cargo install mirdb', async ({ page }) => {
    // Navigate to quick start section
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    // Find the code block with the installation command
    const codeBlock = quickStartSection.locator('.code-block').first();
    await expect(codeBlock).toBeVisible();

    // Find and click the copy button
    const copyButton = codeBlock.locator('.copy-button, [data-testid="copy-button"], button[aria-label*="copy" i], button[title*="copy" i]');
    await expect(copyButton).toBeVisible();
    await copyButton.click();

    // Read clipboard content and verify it contains the installation command
    const clipboardContent = await page.evaluate(() => navigator.clipboard.readText());
    expect(clipboardContent).toContain('cargo install mirdb');
  });

  test('TC3: Copy button shows visual feedback after clicking', async ({ page }) => {
    // Navigate to quick start section
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    // Find the code block
    const codeBlock = quickStartSection.locator('.code-block').first();
    await expect(codeBlock).toBeVisible();

    // Find the copy button
    const copyButton = codeBlock.locator('.copy-button, [data-testid="copy-button"], button[aria-label*="copy" i], button[title*="copy" i]');
    await expect(copyButton).toBeVisible();

    // Click the copy button
    await copyButton.click();

    // Wait a moment for the DOM to update
    await page.waitForTimeout(100);

    // Check for visual feedback - any of these conditions indicates success:
    // 1. Button has "copied" class
    // 2. Button text contains "Copied"
    // 3. Checkmark icon is visible

    const hasCopiedClass = await copyButton.evaluate(el => el.classList.contains('copied'));
    const hasSuccessClass = await copyButton.evaluate(el => el.classList.contains('success'));
    const buttonText = await copyButton.textContent();
    const hasCopiedText = buttonText?.toLowerCase().includes('copied');
    const checkIconVisible = await codeBlock.locator('.check-icon').isVisible();

    // At least one form of visual feedback should be present
    const feedbackPresent = hasCopiedClass || hasSuccessClass || hasCopiedText || checkIconVisible;
    expect(feedbackPresent).toBeTruthy();
  });
});
