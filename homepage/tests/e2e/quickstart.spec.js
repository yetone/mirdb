/**
 * Quick Start Section E2E Tests
 * Owner: Scenario 4 - Quick Start Section
 *
 * Tests for copy functionality and interactive features
 */

const { test, expect } = require('@playwright/test');

test.describe('Quick Start Section E2E Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('Section Navigation', () => {
    test('Quick Start section is visible on page', async ({ page }) => {
      const quickstartSection = page.locator('#quickstart');
      await expect(quickstartSection).toBeVisible();
    });

    test('Clicking Get Started navigates to Quick Start section', async ({ page }) => {
      const ctaButton = page.locator('#get-started-btn');
      await ctaButton.click();

      // Verify we've scrolled to the quickstart section
      const quickstartSection = page.locator('#quickstart');
      await expect(quickstartSection).toBeInViewport();
    });
  });

  test.describe('Test Case 5: Copy Functionality', () => {
    test('Copy button is visible on code blocks', async ({ page }) => {
      const copyButtons = page.locator('.code-block .copy-btn');
      const count = await copyButtons.count();
      expect(count).toBeGreaterThanOrEqual(1);

      // First copy button should be visible
      await expect(copyButtons.first()).toBeVisible();
    });

    test('Click copy button copies code content to clipboard', async ({ page, context }) => {
      // Grant clipboard permissions
      await context.grantPermissions(['clipboard-read', 'clipboard-write']);

      // Find the first copy button in the quickstart section
      const quickstartSection = page.locator('#quickstart');
      const firstCodeBlock = quickstartSection.locator('.code-block').first();
      const copyButton = firstCodeBlock.locator('.copy-btn');

      // Click the copy button
      await copyButton.click();

      // Read from clipboard and verify content was copied
      const clipboardText = await page.evaluate(() => navigator.clipboard.readText());
      expect(clipboardText).toBeTruthy();
      expect(clipboardText.length).toBeGreaterThan(0);
    });

    test('Copy button shows feedback after click', async ({ page, context }) => {
      // Grant clipboard permissions
      await context.grantPermissions(['clipboard-read', 'clipboard-write']);

      const quickstartSection = page.locator('#quickstart');
      const firstCodeBlock = quickstartSection.locator('.code-block').first();
      const copyButton = firstCodeBlock.locator('.copy-btn');

      // Click the copy button
      await copyButton.click();

      // Check that button shows "Copied!" feedback
      const buttonText = copyButton.locator('.copy-text');
      await expect(buttonText).toHaveText('Copied!');

      // Check button has 'copied' class
      await expect(copyButton).toHaveClass(/copied/);
    });

    test('Copy button text resets after feedback timeout', async ({ page, context }) => {
      // Grant clipboard permissions
      await context.grantPermissions(['clipboard-read', 'clipboard-write']);

      const quickstartSection = page.locator('#quickstart');
      const firstCodeBlock = quickstartSection.locator('.code-block').first();
      const copyButton = firstCodeBlock.locator('.copy-btn');

      // Click the copy button
      await copyButton.click();

      // Wait for the reset (2 seconds + buffer)
      await page.waitForTimeout(2500);

      // Check that button text has reset
      const buttonText = copyButton.locator('.copy-text');
      await expect(buttonText).toHaveText('Copy');
    });

    test('Copying cargo install command copies correct content', async ({ page, context }) => {
      // Grant clipboard permissions
      await context.grantPermissions(['clipboard-read', 'clipboard-write']);

      // Find the code block containing cargo install
      const cargoCodeBlock = page.locator('.code-block:has(code:text("cargo install mirdb"))');
      const copyButton = cargoCodeBlock.locator('.copy-btn');

      // Click the copy button
      await copyButton.click();

      // Verify clipboard contains the cargo install command
      const clipboardText = await page.evaluate(() => navigator.clipboard.readText());
      expect(clipboardText).toContain('cargo install mirdb');
    });

    test('Copying server command copies full command with flags', async ({ page, context }) => {
      // Grant clipboard permissions
      await context.grantPermissions(['clipboard-read', 'clipboard-write']);

      // Find the code block containing mirdb-server
      const serverCodeBlock = page.locator('.code-block:has(code:text("mirdb-server"))');
      const copyButton = serverCodeBlock.locator('.copy-btn');

      // Click the copy button
      await copyButton.click();

      // Verify clipboard contains the server command with flags
      const clipboardText = await page.evaluate(() => navigator.clipboard.readText());
      expect(clipboardText).toContain('mirdb-server');
      expect(clipboardText).toContain('--addr');
      expect(clipboardText).toContain('--dir');
    });
  });

  test.describe('Code Block Accessibility', () => {
    test('Copy buttons have accessible aria-label', async ({ page }) => {
      const copyButtons = page.locator('.code-block .copy-btn');
      const firstButton = copyButtons.first();

      await expect(firstButton).toHaveAttribute('aria-label', 'Copy to clipboard');
    });

    test('Copy buttons are keyboard accessible', async ({ page }) => {
      // Tab to the first copy button
      const quickstartSection = page.locator('#quickstart');
      const firstCodeBlock = quickstartSection.locator('.code-block').first();
      const copyButton = firstCodeBlock.locator('.copy-btn');

      // Focus the button
      await copyButton.focus();

      // Check it's focused
      await expect(copyButton).toBeFocused();
    });
  });

  test.describe('Quick Start Content Display', () => {
    test('All step headings are visible', async ({ page }) => {
      await expect(page.locator('h3:has-text("1. Install MirDB")')).toBeVisible();
      await expect(page.locator('h3:has-text("2. Start the Server")')).toBeVisible();
      await expect(page.locator('h3:has-text("3. Connect and Use")')).toBeVisible();
      await expect(page.locator('h3:has-text("4. Store and Retrieve Data")')).toBeVisible();
    });

    test('Code blocks display correct commands', async ({ page }) => {
      // Check installation command
      await expect(page.locator('code:has-text("cargo install mirdb")')).toBeVisible();

      // Check server startup command
      await expect(page.locator('code:has-text("mirdb-server")')).toBeVisible();

      // Check set command
      await expect(page.locator('code:has-text("set mykey 0 0 5")')).toBeVisible();

      // Check get command
      await expect(page.locator('code:has-text("get mykey")')).toBeVisible();
    });
  });
});
