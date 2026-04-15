/**
 * Quick Start Section E2E Tests
 * Owner: Scenario 3 - Quick Start Section
 *
 * Tests for code block visibility, syntax highlighting,
 * copy functionality, and code content verification.
 */

import { test, expect } from '@playwright/test';
import { SELECTORS, EXPECTED_TEXT } from '../fixtures/test-data';

test.describe('Quick Start Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Quick Start section exists with code block element', async ({ page }) => {
    // Navigate to Quick Start section
    const quickstartSection = page.locator(SELECTORS.QUICK_START_SECTION);
    await expect(quickstartSection).toBeVisible();

    // Check code block exists
    const codeBlock = page.locator(SELECTORS.CODE_BLOCK);
    await expect(codeBlock).toBeVisible();
  });

  test('Code block contains styled/highlighted syntax elements', async ({ page }) => {
    const codeBlock = page.locator(SELECTORS.CODE_BLOCK);
    await expect(codeBlock).toBeVisible();

    // Check for syntax highlighting elements
    const syntaxKeyword = codeBlock.locator('.syntax-keyword');
    const syntaxString = codeBlock.locator('.syntax-string');
    const syntaxComment = codeBlock.locator('.syntax-comment');

    // Verify syntax highlighting elements exist
    await expect(syntaxKeyword.first()).toBeVisible();
    await expect(syntaxString.first()).toBeVisible();
    await expect(syntaxComment.first()).toBeVisible();

    // Verify the code block has a dark background (code styling)
    const codeContent = page.locator(SELECTORS.CODE_CONTENT);
    await expect(codeContent).toBeVisible();
  });

  test('Code contains memcached client connection pattern (localhost:12333)', async ({ page }) => {
    const codeBlock = page.locator(SELECTORS.CODE_BLOCK);
    const codeContent = codeBlock.locator('code');

    // Verify the code contains the expected connection details
    await expect(codeContent).toContainText(EXPECTED_TEXT.CONNECTION_HOST);
    await expect(codeContent).toContainText(EXPECTED_TEXT.CONNECTION_PORT);

    // Verify it shows memcached client usage
    await expect(codeContent).toContainText('client');
    await expect(codeContent).toContainText('set');
    await expect(codeContent).toContainText('get');
  });

  test('Code content is copied to clipboard successfully', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    const copyButton = page.locator(SELECTORS.COPY_BUTTON);
    await expect(copyButton).toBeVisible();

    // Click the copy button
    await copyButton.click();

    // Verify visual feedback (button shows "Copied!")
    const copyText = copyButton.locator('.code-block__copy-text');
    await expect(copyText).toHaveText('Copied!');

    // Verify the copied class is applied
    await expect(copyButton).toHaveClass(/copied/);

    // Read from clipboard and verify content
    const clipboardContent = await page.evaluate(() => navigator.clipboard.readText());
    expect(clipboardContent).toContain('localhost');
    expect(clipboardContent).toContain('12333');
    expect(clipboardContent).toContain('pymemcache');

    // Wait for feedback to reset
    await page.waitForTimeout(2500);
    await expect(copyText).toHaveText('Copy');
    await expect(copyButton).not.toHaveClass(/copied/);
  });

  test('Quick Start section has proper heading hierarchy', async ({ page }) => {
    const quickstartSection = page.locator(SELECTORS.QUICK_START_SECTION);
    const heading = quickstartSection.locator('h2');

    await expect(heading).toBeVisible();
    await expect(heading).toHaveText('Quick Start');
  });

  test('Copy button is keyboard accessible', async ({ page, context }) => {
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    const copyButton = page.locator(SELECTORS.COPY_BUTTON);

    // Focus the button using keyboard
    await copyButton.focus();
    await expect(copyButton).toBeFocused();

    // Activate with Enter key
    await page.keyboard.press('Enter');

    // Verify copy was triggered
    const copyText = copyButton.locator('.code-block__copy-text');
    await expect(copyText).toHaveText('Copied!');
  });

  test('Code block has proper ARIA labels', async ({ page }) => {
    const copyButton = page.locator(SELECTORS.COPY_BUTTON);

    // Verify copy button has accessible label
    await expect(copyButton).toHaveAttribute('aria-label', 'Copy code to clipboard');
  });
});
