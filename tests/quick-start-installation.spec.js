// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Quick Start Installation Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Navigate to quick start section
    const quickStartSection = page.locator('#quick-start');
    await quickStartSection.scrollIntoViewIfNeeded();
    await expect(quickStartSection).toBeVisible();
  });

  test('TC1: Displays clone command - git clone https://github.com/yetone/mirdb', async ({ page }) => {
    // Find the installation code block
    const installCode = page.locator('#install-code');
    await expect(installCode).toBeVisible();

    // Verify the clone command is present
    const codeText = await installCode.textContent();
    expect(codeText).toContain('git clone https://github.com/yetone/mirdb');
  });

  test('TC2: Displays build command - cd mirdb && cargo build --release', async ({ page }) => {
    // Find the installation code block
    const installCode = page.locator('#install-code');
    await expect(installCode).toBeVisible();

    // Verify the build command is present
    const codeText = await installCode.textContent();
    expect(codeText).toContain('cd mirdb && cargo build --release');
  });

  test('TC3: Displays run command - ./target/release/mirdb-server -c etc/mirdb.toml', async ({ page }) => {
    // Find the installation code block
    const installCode = page.locator('#install-code');
    await expect(installCode).toBeVisible();

    // Verify the run command is present
    const codeText = await installCode.textContent();
    expect(codeText).toContain('./target/release/mirdb-server -c etc/mirdb.toml');
  });

  test('TC4: Copy button copies command text to clipboard successfully', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    // Find the copy button for installation commands
    const copyButton = page.locator('.copy-btn[data-copy="install"]');
    await expect(copyButton).toBeVisible();
    await expect(copyButton).toHaveText('Copy');

    // Click the copy button
    await copyButton.click();

    // Verify the clipboard contains the installation commands
    const clipboardContent = await page.evaluate(() => navigator.clipboard.readText());

    // Verify all three commands are copied
    expect(clipboardContent).toContain('git clone https://github.com/yetone/mirdb');
    expect(clipboardContent).toContain('cd mirdb && cargo build --release');
    expect(clipboardContent).toContain('./target/release/mirdb-server -c etc/mirdb.toml');
  });

  test('TC5: Copy button shows visual confirmation on success', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    // Find the copy button for installation commands
    const copyButton = page.locator('.copy-btn[data-copy="install"]');
    await expect(copyButton).toBeVisible();

    // Verify initial state
    await expect(copyButton).toHaveText('Copy');

    // Click the copy button
    await copyButton.click();

    // Verify the button text changes to "Copied!" as visual confirmation
    await expect(copyButton).toHaveText('Copied!');

    // Wait for the text to revert back to "Copy" (timeout set in the implementation is 2000ms)
    await expect(copyButton).toHaveText('Copy', { timeout: 3000 });
  });

  test('Quick start section has proper heading and intro text', async ({ page }) => {
    // Verify section title
    const title = page.locator('#quickstart-title');
    await expect(title).toBeVisible();
    await expect(title).toHaveText('Quick Start');

    // Verify intro text
    const intro = page.locator('.quickstart-intro');
    await expect(intro).toBeVisible();
    await expect(intro).toContainText('Get MirDB up and running');
  });

  test('Installation code block has proper title', async ({ page }) => {
    // Find the code block header
    const codeHeader = page.locator('.code-block').first().locator('.code-title');
    await expect(codeHeader).toBeVisible();
    await expect(codeHeader).toHaveText('Installation');
  });

  test('Copy button is accessible with proper aria-label', async ({ page }) => {
    // Verify the copy button has an accessible label
    const copyButton = page.locator('.copy-btn[data-copy="install"]');
    await expect(copyButton).toHaveAttribute('aria-label', 'Copy installation commands');
  });
});
