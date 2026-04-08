// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Copy Code Functionality E2E Tests
 * Owner: Scenario 13 - Copy Code Functionality
 *
 * Test cases:
 * - Click copy button on installation code snippet - code copied, success feedback shown
 * - Click copy button on usage example snippet - code copied, success feedback shown
 * - Paste copied code into text area - pasted content matches code snippet exactly
 * - Copy button accessibility - keyboard accessible and have aria-labels
 */

test.describe('Copy Code Functionality', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for clipboard.js to initialize
    await page.waitForFunction(() => window.MirDBClipboard !== undefined);
  });

  test('copy button on installation code snippet shows success feedback', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    // Scroll to installation section
    await page.locator('#installation').scrollIntoViewIfNeeded();

    // Find the first installation copy button (git clone command)
    const copyButton = page.locator('.install-copy-btn').first();
    await expect(copyButton).toBeVisible();

    // Get the expected text from data-copy attribute
    const expectedText = await copyButton.getAttribute('data-copy');

    // Click the copy button
    await copyButton.click();

    // Verify success feedback - button should have 'copied' class
    await expect(copyButton).toHaveClass(/copied/);

    // Verify button text changes to "Copied!"
    const buttonSpan = copyButton.locator('span');
    await expect(buttonSpan).toHaveText('Copied!');

    // Verify clipboard content matches expected text
    const clipboardText = await page.evaluate(() => navigator.clipboard.readText());
    expect(clipboardText).toBe(expectedText);

    // Wait for feedback to reset (2 seconds + buffer)
    await page.waitForTimeout(2500);

    // Verify button resets to original state
    await expect(copyButton).not.toHaveClass(/copied/);
    await expect(buttonSpan).toHaveText('Copy');
  });

  test('copy button on usage example snippet shows success feedback', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    // Scroll to code examples section
    await page.locator('#code-examples').scrollIntoViewIfNeeded();

    // Find a code example copy button (SET command)
    const copyButton = page.locator('.code-copy-btn').first();
    await expect(copyButton).toBeVisible();

    // Get the expected text from data-copy attribute
    const expectedText = await copyButton.getAttribute('data-copy');

    // Click the copy button
    await copyButton.click();

    // Verify success feedback - button should have 'copied' class
    await expect(copyButton).toHaveClass(/copied/);

    // Verify button text changes to "Copied!"
    const buttonSpan = copyButton.locator('span');
    await expect(buttonSpan).toHaveText('Copied!');

    // Verify clipboard content matches expected text
    const clipboardText = await page.evaluate(() => navigator.clipboard.readText());
    expect(clipboardText).toBe(expectedText);
  });

  test('copied code can be pasted and matches original exactly', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    // Scroll to installation section
    await page.locator('#installation').scrollIntoViewIfNeeded();

    // Find a copy button with a specific command
    const copyButton = page.locator('.install-copy-btn[data-copy="cargo build --release"]');
    await expect(copyButton).toBeVisible();

    // Get the expected text
    const expectedText = 'cargo build --release';

    // Click the copy button
    await copyButton.click();

    // Wait for copy to complete
    await expect(copyButton).toHaveClass(/copied/);

    // Create a temporary textarea to paste into
    await page.evaluate(() => {
      const textarea = document.createElement('textarea');
      textarea.id = 'test-paste-area';
      textarea.style.position = 'fixed';
      textarea.style.top = '0';
      textarea.style.left = '0';
      textarea.style.width = '300px';
      textarea.style.height = '100px';
      textarea.style.zIndex = '9999';
      document.body.appendChild(textarea);
    });

    // Focus the textarea and paste
    const textarea = page.locator('#test-paste-area');
    await textarea.focus();

    // Read clipboard and verify content
    const clipboardText = await page.evaluate(() => navigator.clipboard.readText());
    expect(clipboardText).toBe(expectedText);

    // Paste using keyboard
    await page.keyboard.press('Control+v');

    // Verify pasted content matches
    const pastedText = await textarea.inputValue();
    expect(pastedText).toBe(expectedText);

    // Clean up
    await page.evaluate(() => {
      const textarea = document.getElementById('test-paste-area');
      if (textarea) textarea.remove();
    });
  });

  test('copy buttons are keyboard accessible and have aria-labels', async ({ page }) => {
    // Check installation copy buttons
    await page.locator('#installation').scrollIntoViewIfNeeded();
    const installButtons = page.locator('.install-copy-btn');
    const installButtonCount = await installButtons.count();
    expect(installButtonCount).toBeGreaterThan(0);

    for (let i = 0; i < installButtonCount; i++) {
      const button = installButtons.nth(i);

      // Verify aria-label exists
      const ariaLabel = await button.getAttribute('aria-label');
      expect(ariaLabel).toBeTruthy();
      expect(ariaLabel?.toLowerCase()).toContain('copy');

      // Verify button is focusable (has no tabindex=-1)
      const tabindex = await button.getAttribute('tabindex');
      expect(tabindex).not.toBe('-1');

      // Verify button type is set
      const buttonType = await button.getAttribute('type');
      expect(buttonType).toBe('button');
    }

    // Check code example copy buttons
    await page.locator('#code-examples').scrollIntoViewIfNeeded();
    const codeButtons = page.locator('.code-copy-btn');
    const codeButtonCount = await codeButtons.count();
    expect(codeButtonCount).toBeGreaterThan(0);

    for (let i = 0; i < codeButtonCount; i++) {
      const button = codeButtons.nth(i);

      // Verify aria-label exists
      const ariaLabel = await button.getAttribute('aria-label');
      expect(ariaLabel).toBeTruthy();
      expect(ariaLabel?.toLowerCase()).toContain('copy');

      // Verify button type is set
      const buttonType = await button.getAttribute('type');
      expect(buttonType).toBe('button');
    }
  });

  test('copy button can be activated with keyboard', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    // Scroll to installation section
    await page.locator('#installation').scrollIntoViewIfNeeded();

    // Find the first installation copy button
    const copyButton = page.locator('.install-copy-btn').first();
    await expect(copyButton).toBeVisible();

    // Get the expected text from data-copy attribute
    const expectedText = await copyButton.getAttribute('data-copy');

    // Focus the button using keyboard navigation
    await copyButton.focus();

    // Activate with Enter key
    await page.keyboard.press('Enter');

    // Verify success feedback
    await expect(copyButton).toHaveClass(/copied/);

    // Verify clipboard content
    const clipboardText = await page.evaluate(() => navigator.clipboard.readText());
    expect(clipboardText).toBe(expectedText);
  });

  test('copy button can be activated with Space key', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    // Scroll to code examples section
    await page.locator('#code-examples').scrollIntoViewIfNeeded();

    // Find a code example copy button
    const copyButton = page.locator('.code-copy-btn').first();
    await expect(copyButton).toBeVisible();

    // Get the expected text from data-copy attribute
    const expectedText = await copyButton.getAttribute('data-copy');

    // Focus the button
    await copyButton.focus();

    // Activate with Space key
    await page.keyboard.press('Space');

    // Verify success feedback
    await expect(copyButton).toHaveClass(/copied/);

    // Verify clipboard content
    const clipboardText = await page.evaluate(() => navigator.clipboard.readText());
    expect(clipboardText).toBe(expectedText);
  });

  test('all copy buttons have data-copy attribute with content', async ({ page }) => {
    // Check all copy buttons on the page
    const allCopyButtons = page.locator('[data-copy]');
    const count = await allCopyButtons.count();
    expect(count).toBeGreaterThan(0);

    for (let i = 0; i < count; i++) {
      const button = allCopyButtons.nth(i);
      const dataCopy = await button.getAttribute('data-copy');
      expect(dataCopy).toBeTruthy();
      expect(dataCopy?.length).toBeGreaterThan(0);
    }
  });

  test('Python example copy button copies full code snippet', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    // Scroll to code examples section
    await page.locator('#code-examples').scrollIntoViewIfNeeded();

    // Find the Python example copy button
    const pythonCopyButton = page.locator('.code-client-example .code-copy-btn');
    await expect(pythonCopyButton).toBeVisible();

    // Get the expected text from data-copy attribute
    const expectedText = await pythonCopyButton.getAttribute('data-copy');
    expect(expectedText).toContain('pymemcache');
    expect(expectedText).toContain('client.set');
    expect(expectedText).toContain('client.get');

    // Click the copy button
    await pythonCopyButton.click();

    // Verify success feedback
    await expect(pythonCopyButton).toHaveClass(/copied/);

    // Verify clipboard content
    const clipboardText = await page.evaluate(() => navigator.clipboard.readText());
    expect(clipboardText).toBe(expectedText);
  });
});
