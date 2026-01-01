const { test, expect } = require('@playwright/test');

test.describe('Code Block Copy Functionality', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Click copy button on installation code block copies command to clipboard', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    // Navigate to quick start section where installation instructions are
    const quickStartSection = page.locator('#quickstart');
    await quickStartSection.scrollIntoViewIfNeeded();
    await expect(quickStartSection).toBeVisible();

    // Find the first code block (installation command)
    const codeBlocks = quickStartSection.locator('.code-block');
    const installCodeBlock = codeBlocks.first();
    await expect(installCodeBlock).toBeVisible();

    // Find the copy button within the code block
    const copyButton = installCodeBlock.locator('[data-testid="copy-button"]');
    await expect(copyButton).toBeVisible();

    // Click the copy button
    await copyButton.click();

    // Verify the clipboard contains the installation command
    const clipboardContent = await page.evaluate(() => navigator.clipboard.readText());
    expect(clipboardContent).toContain('cargo build --release');
  });

  test('TC2: Click copy button on configuration example copies TOML to clipboard', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    // Navigate to configuration section
    const configSection = page.locator('#configuration');
    await configSection.scrollIntoViewIfNeeded();
    await expect(configSection).toBeVisible();

    // Find the config code block
    const configCodeBlock = page.locator('[data-testid="config-toml-block"]');
    await expect(configCodeBlock).toBeVisible();

    // Find the copy button within the code block
    const copyButton = configCodeBlock.locator('[data-testid="copy-button"]');
    await expect(copyButton).toBeVisible();

    // Click the copy button
    await copyButton.click();

    // Verify the clipboard contains the configuration content
    const clipboardContent = await page.evaluate(() => navigator.clipboard.readText());
    expect(clipboardContent).toContain('addr = "0.0.0.0:12333"');
    expect(clipboardContent).toContain('max_level = 7');
    expect(clipboardContent).toContain('work_dir = "/tmp/mirdb"');
  });

  test('TC3: Verify copy feedback indication appears after copy', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    // Navigate to quick start section
    const quickStartSection = page.locator('#quickstart');
    await quickStartSection.scrollIntoViewIfNeeded();
    await expect(quickStartSection).toBeVisible();

    // Find the first code block
    const codeBlocks = quickStartSection.locator('.code-block');
    const installCodeBlock = codeBlocks.first();
    await expect(installCodeBlock).toBeVisible();

    // Find the copy button
    const copyButton = installCodeBlock.locator('[data-testid="copy-button"]');
    await expect(copyButton).toBeVisible();

    // Check the initial state (should show copy icon or "Copy" text)
    const initialAriaLabel = await copyButton.getAttribute('aria-label');
    expect(initialAriaLabel).toContain('Copy');

    // Click the copy button
    await copyButton.click();

    // Verify visual feedback - button should show copied state
    // The aria-label should change to indicate success
    await expect(copyButton).toHaveAttribute('aria-label', /Copied/);

    // Or check for a checkmark icon or visual indicator
    const successIndicator = installCodeBlock.locator('.copy-success, [data-copied="true"]');
    const buttonHasSuccessState = await copyButton.evaluate(el =>
      el.classList.contains('copied') || el.dataset.copied === 'true'
    );

    // At least one feedback mechanism should be present
    expect(buttonHasSuccessState || await successIndicator.count() > 0).toBeTruthy();
  });

  test('TC4: Test copy on code block with telnet commands preserves multi-line formatting', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    // Navigate to quick start section
    const quickStartSection = page.locator('#quickstart');
    await quickStartSection.scrollIntoViewIfNeeded();
    await expect(quickStartSection).toBeVisible();

    // Find the Basic Operations heading and its code block (contains telnet commands)
    const basicOpsHeading = quickStartSection.locator('h3', { hasText: 'Basic Operations' });
    await expect(basicOpsHeading).toBeVisible();

    // Get all code blocks and find the one with telnet content
    const codeBlocks = quickStartSection.locator('.code-block');
    let telnetCodeBlock = null;

    const count = await codeBlocks.count();
    for (let i = 0; i < count; i++) {
      const codeText = await codeBlocks.nth(i).textContent();
      if (codeText.includes('telnet localhost')) {
        telnetCodeBlock = codeBlocks.nth(i);
        break;
      }
    }

    expect(telnetCodeBlock).not.toBeNull();
    await expect(telnetCodeBlock).toBeVisible();

    // Find the copy button within the code block
    const copyButton = telnetCodeBlock.locator('[data-testid="copy-button"]');
    await expect(copyButton).toBeVisible();

    // Click the copy button
    await copyButton.click();

    // Verify the clipboard contains multi-line content with proper formatting
    const clipboardContent = await page.evaluate(() => navigator.clipboard.readText());

    // Check that multi-line content is preserved
    expect(clipboardContent).toContain('telnet localhost 12333');
    expect(clipboardContent).toContain('set mykey 0 0 5');
    expect(clipboardContent).toContain('hello');
    expect(clipboardContent).toContain('STORED');
    expect(clipboardContent).toContain('get mykey');
    expect(clipboardContent).toContain('delete mykey');
    expect(clipboardContent).toContain('DELETED');

    // Verify line breaks are preserved (content should span multiple lines)
    const lines = clipboardContent.split('\n');
    expect(lines.length).toBeGreaterThan(5);
  });

  test('All code blocks have copy buttons', async ({ page }) => {
    // Navigate to page
    await page.goto('/');

    // Find all code blocks on the page
    const codeBlocks = page.locator('.code-block');
    const count = await codeBlocks.count();

    // There should be multiple code blocks
    expect(count).toBeGreaterThan(0);

    // Each code block should have a copy button
    for (let i = 0; i < count; i++) {
      const codeBlock = codeBlocks.nth(i);
      await codeBlock.scrollIntoViewIfNeeded();

      const copyButton = codeBlock.locator('[data-testid="copy-button"]');
      await expect(copyButton).toBeVisible();

      // Verify button is accessible
      await expect(copyButton).toHaveAttribute('aria-label', /Copy/);
    }
  });

  test('Copy button has proper accessibility attributes', async ({ page }) => {
    // Navigate to quick start section
    const quickStartSection = page.locator('#quickstart');
    await quickStartSection.scrollIntoViewIfNeeded();

    // Find a code block's copy button
    const codeBlock = quickStartSection.locator('.code-block').first();
    const copyButton = codeBlock.locator('[data-testid="copy-button"]');

    await expect(copyButton).toBeVisible();

    // Check accessibility attributes
    await expect(copyButton).toHaveAttribute('aria-label');
    await expect(copyButton).toHaveAttribute('type', 'button');

    // Button should be focusable
    await copyButton.focus();
    await expect(copyButton).toBeFocused();
  });
});
