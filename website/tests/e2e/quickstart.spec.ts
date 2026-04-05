import { test, expect } from '@playwright/test';

test.describe('Quick Start Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Quick Start section contains at least one pre-formatted code block', async ({ page }) => {
    // Navigate to Quick Start section
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeVisible();

    // Check for pre-formatted code blocks
    const codeBlocks = quickstartSection.locator('pre.quickstart__code');
    const count = await codeBlocks.count();
    expect(count).toBeGreaterThanOrEqual(1);
  });

  test('TC2: Code block contains installation/build commands', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeVisible();

    // Find the installation code block
    const installCodeBlock = page.locator('#install-code');
    await expect(installCodeBlock).toBeVisible();

    // Get the text content
    const codeText = await installCodeBlock.textContent();

    // Verify installation commands are present
    expect(codeText).toContain('git clone');
    expect(codeText).toContain('cargo build');
  });

  test('TC3: Code block contains basic usage example commands', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeVisible();

    // Find the usage code block
    const usageCodeBlock = page.locator('#usage-code');
    await expect(usageCodeBlock).toBeVisible();

    // Get the text content
    const codeText = await usageCodeBlock.textContent();

    // Verify usage commands are present
    expect(codeText).toContain('telnet');
    expect(codeText).toContain('set');
    expect(codeText).toContain('get');
  });

  test('TC4: Copy-to-clipboard button copies code content successfully', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeVisible();

    // Find the first copy button
    const copyButton = quickstartSection.locator('.quickstart__copy-btn').first();
    await expect(copyButton).toBeVisible();

    // Get the code content that should be copied
    const codeElement = page.locator('#install-code');
    const expectedCode = await codeElement.textContent();

    // Click the copy button
    await copyButton.click();

    // Verify the button shows "Copied!" feedback
    const copyText = copyButton.locator('.quickstart__copy-text');
    await expect(copyText).toHaveText('Copied!');

    // Verify clipboard content
    const clipboardContent = await page.evaluate(async () => {
      return await navigator.clipboard.readText();
    });
    expect(clipboardContent).toBe(expectedCode);
  });

  test('TC5: Code blocks have syntax highlighting applied (colored tokens)', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeVisible();

    // Check for syntax highlighting classes
    const tokenComment = quickstartSection.locator('.token-comment').first();
    const tokenCommand = quickstartSection.locator('.token-command').first();
    const tokenKeyword = quickstartSection.locator('.token-keyword').first();

    // Verify token classes exist
    await expect(tokenComment).toBeVisible();
    await expect(tokenCommand).toBeVisible();
    await expect(tokenKeyword).toBeVisible();

    // Verify tokens have different colors applied via CSS
    const commentColor = await tokenComment.evaluate((el) => {
      return window.getComputedStyle(el).color;
    });
    const commandColor = await tokenCommand.evaluate((el) => {
      return window.getComputedStyle(el).color;
    });
    const keywordColor = await tokenKeyword.evaluate((el) => {
      return window.getComputedStyle(el).color;
    });

    // Each token type should have a different color (syntax highlighting)
    expect(commentColor).not.toBe(commandColor);
    expect(commandColor).not.toBe(keywordColor);
  });

  test('TC6: Notes about default configuration are displayed near code blocks', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeVisible();

    // Find the configuration notes section
    const configNotes = quickstartSection.locator('[data-testid="config-notes"]');
    await expect(configNotes).toBeVisible();

    // Verify it contains default configuration information
    const notesText = await configNotes.textContent();
    expect(notesText).toContain('Default Configuration');
    expect(notesText).toContain('0.0.0.0:12333');
    expect(notesText).toContain('/tmp/mirdb');
    expect(notesText).toContain('4MB');
    expect(notesText).toContain('100MB');
  });
});
