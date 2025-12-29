import { test, expect } from '@playwright/test';

test.describe('Code Snippet Copy Functionality', () => {
  test.beforeEach(async ({ page, context }) => {
    // Grant clipboard permissions for the test
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);
    await page.goto('/');
  });

  test('copy button is visible on code snippets', async ({ page }) => {
    // Test Case 1: Verify copy buttons are present on code blocks
    // Navigate to the getting started section
    const codeBlock1 = page.locator('[data-testid="code-block-1"]');
    await expect(codeBlock1).toBeVisible();

    const copyBtn1 = page.locator('[data-testid="copy-btn-1"]');
    await expect(copyBtn1).toBeVisible();

    const codeBlock2 = page.locator('[data-testid="code-block-2"]');
    await expect(codeBlock2).toBeVisible();

    const copyBtn2 = page.locator('[data-testid="copy-btn-2"]');
    await expect(copyBtn2).toBeVisible();

    // Also check the configuration code block
    const configCodeBlock = page.locator('[data-testid="code-block-config"]');
    await expect(configCodeBlock).toBeVisible();

    const copyBtnConfig = page.locator('[data-testid="copy-btn-config"]');
    await expect(copyBtnConfig).toBeVisible();
  });

  test('clicking copy button copies code to clipboard', async ({ page }) => {
    // Test Case 1: Click copy button on code snippet - Code is copied to clipboard
    const copyBtn1 = page.locator('[data-testid="copy-btn-1"]');
    await copyBtn1.scrollIntoViewIfNeeded();
    await copyBtn1.click();

    // Read clipboard content
    const clipboardContent = await page.evaluate(async () => {
      return await navigator.clipboard.readText();
    });

    // Verify the clipboard contains the expected code
    expect(clipboardContent).toContain('# Start MirDB server with your configuration file');
    expect(clipboardContent).toContain('mirdb -c /path/to/config.toml');
  });

  test('visual feedback is shown after copy action', async ({ page }) => {
    // Test Case 2: Verify visual feedback (tooltip, icon change) confirms copy action
    const copyBtn1 = page.locator('[data-testid="copy-btn-1"]');
    await copyBtn1.scrollIntoViewIfNeeded();

    // Before clicking, check that button doesn't have 'copied' class
    await expect(copyBtn1).not.toHaveClass(/copied/);

    // Click the copy button
    await copyBtn1.click();

    // After clicking, check that button has 'copied' class (visual feedback)
    await expect(copyBtn1).toHaveClass(/copied/);

    // Check that feedback tooltip is visible
    const feedback1 = page.locator('[data-testid="copy-feedback-1"]');
    await expect(feedback1).toHaveClass(/visible/);
    await expect(feedback1).toHaveText('Copied!');

    // Check icon change - check-icon should be displayed
    const checkIcon = copyBtn1.locator('.check-icon');
    await expect(checkIcon).toHaveCSS('display', 'block');

    const copyIcon = copyBtn1.locator('.copy-icon');
    await expect(copyIcon).toHaveCSS('display', 'none');
  });

  test('pasted content matches original code snippet exactly', async ({ page }) => {
    // Test Case 3: Paste copied content - Pasted content matches original code snippet exactly

    // Get the original code text from the code block
    const codeBlock1 = page.locator('[data-testid="code-block-1"] pre code');
    const originalCode = await codeBlock1.textContent();

    // Click the copy button
    const copyBtn1 = page.locator('[data-testid="copy-btn-1"]');
    await copyBtn1.scrollIntoViewIfNeeded();
    await copyBtn1.click();

    // Read clipboard content
    const clipboardContent = await page.evaluate(async () => {
      return await navigator.clipboard.readText();
    });

    // Verify the clipboard content matches the original code exactly
    expect(clipboardContent).toBe(originalCode);
  });

  test('copy button on second code block copies correct content', async ({ page }) => {
    // Test copying from the second code block (Connect with Memcached client)
    const copyBtn2 = page.locator('[data-testid="copy-btn-2"]');
    await copyBtn2.scrollIntoViewIfNeeded();
    await copyBtn2.click();

    // Read clipboard content
    const clipboardContent = await page.evaluate(async () => {
      return await navigator.clipboard.readText();
    });

    // Verify the clipboard contains the expected telnet commands
    expect(clipboardContent).toContain('telnet localhost 12333');
    expect(clipboardContent).toContain('set mykey 0 0 5');
    expect(clipboardContent).toContain('get mykey');
    expect(clipboardContent).toContain('delete mykey');
  });

  test('copy button on configuration block copies config content', async ({ page }) => {
    // Test copying from the configuration code block
    const copyBtnConfig = page.locator('[data-testid="copy-btn-config"]');
    await copyBtnConfig.scrollIntoViewIfNeeded();
    await copyBtnConfig.click();

    // Read clipboard content
    const clipboardContent = await page.evaluate(async () => {
      return await navigator.clipboard.readText();
    });

    // Verify the clipboard contains the configuration content
    expect(clipboardContent).toContain('addr = "0.0.0.0:12333"');
    expect(clipboardContent).toContain('max_level = 7');
    expect(clipboardContent).toContain('work_dir = "/tmp/mirdb"');
  });

  test('visual feedback resets after timeout', async ({ page }) => {
    // Verify that the visual feedback resets after the timeout period
    const copyBtn1 = page.locator('[data-testid="copy-btn-1"]');
    await copyBtn1.scrollIntoViewIfNeeded();
    await copyBtn1.click();

    // Verify feedback is shown
    await expect(copyBtn1).toHaveClass(/copied/);

    // Wait for the feedback to reset (2 seconds + buffer)
    await page.waitForTimeout(2500);

    // Verify feedback is reset
    await expect(copyBtn1).not.toHaveClass(/copied/);
  });

  test('copy button has accessible label', async ({ page }) => {
    // Verify accessibility - button has proper aria-label
    const copyBtn1 = page.locator('[data-testid="copy-btn-1"]');
    await expect(copyBtn1).toHaveAttribute('aria-label', 'Copy code to clipboard');

    const copyBtn2 = page.locator('[data-testid="copy-btn-2"]');
    await expect(copyBtn2).toHaveAttribute('aria-label', 'Copy code to clipboard');

    const copyBtnConfig = page.locator('[data-testid="copy-btn-config"]');
    await expect(copyBtnConfig).toHaveAttribute('aria-label', 'Copy code to clipboard');
  });
});
