// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Quick Start Section E2E Tests
 * Owner: Scenario 2 - Quick Start Section
 *
 * End-to-end tests for quick start:
 * - Section is present
 * - Installation command visible
 * - Server start command visible
 * - Client example visible
 * - Copy buttons work
 * - Syntax highlighting applied
 * - Language labels present
 */

test.describe('Quick Start Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Quick Start section exists with installation commands', async ({ page }) => {
    // Test Case 1: Check Quick Start section exists
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeVisible();

    // Check for the section heading
    const heading = page.locator('#quickstart-title');
    await expect(heading).toHaveText('Quick Start');

    // Check for installation command presence
    const installCode = page.locator('#quickstart .code-block code').first();
    await expect(installCode).toBeVisible();
  });

  test('cargo install command is displayed', async ({ page }) => {
    // Test Case 2: Verify cargo install command
    const codeBlocks = page.locator('#quickstart .code-block code');

    // Find the code block containing cargo install
    const installBlock = codeBlocks.filter({ hasText: 'cargo install mirdb' });
    await expect(installBlock).toBeVisible();
    await expect(installBlock).toContainText('cargo install mirdb');
  });

  test('server start command is displayed with data directory option', async ({ page }) => {
    // Test Case 3: Verify server start command
    const codeBlocks = page.locator('#quickstart .code-block code');

    // Find the code block containing server start command
    const serverBlock = codeBlocks.filter({ hasText: 'mirdb-server --data-dir' });
    await expect(serverBlock).toBeVisible();
    await expect(serverBlock).toContainText('mirdb-server --data-dir ./data');
  });

  test('copy button copies installation command to clipboard', async ({ page, context }) => {
    // Test Case 4: Click copy button on installation command
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    // Find the first code block (installation) and its copy button
    const firstCodeBlock = page.locator('#quickstart .code-block').first();
    const copyButton = firstCodeBlock.locator('.code-block__copy');

    await expect(copyButton).toBeVisible();
    await copyButton.click();

    // Wait for the copy feedback
    await expect(copyButton).toHaveClass(/code-block__copy--copied/);

    // Verify clipboard content
    const clipboardText = await page.evaluate(() => navigator.clipboard.readText());
    expect(clipboardText).toBe('cargo install mirdb');
  });

  test('client connection example shows telnet on port 11211', async ({ page }) => {
    // Test Case 5: Verify client connection example
    const codeBlocks = page.locator('#quickstart .code-block code');

    // Find the code block containing telnet command
    const telnetBlock = codeBlocks.filter({ hasText: 'telnet localhost 11211' });
    await expect(telnetBlock).toBeVisible();
    await expect(telnetBlock).toContainText('telnet localhost 11211');
  });

  test('set/get operation examples are demonstrated', async ({ page }) => {
    // Test Case 6: Verify set/get operation examples
    const codeBlocks = page.locator('#quickstart .code-block code');

    // Find the code block containing Memcached operations
    const memcachedBlock = codeBlocks.filter({ hasText: 'set mykey' });
    await expect(memcachedBlock).toBeVisible();

    // Verify it contains both set and get commands
    const blockText = await memcachedBlock.textContent();
    expect(blockText).toContain('set mykey');
    expect(blockText).toContain('get mykey');
    expect(blockText).toContain('STORED');
    expect(blockText).toContain('VALUE');
    expect(blockText).toContain('END');
  });

  test('code blocks have syntax highlighting applied', async ({ page }) => {
    // Test Case 7: Check syntax highlighting
    // Wait for Prism.js to load and apply highlighting
    await page.waitForTimeout(1000);

    // Check that at least one code block has syntax-highlighted tokens
    const codeBlocks = page.locator('#quickstart .code-block pre code');
    const firstBlock = codeBlocks.first();

    // Prism.js adds language-specific classes
    const hasHighlighting = await firstBlock.evaluate((el) => {
      // Check if the code element has the language class or contains tokens
      return el.classList.contains('language-bash') ||
             el.querySelector('.token') !== null ||
             el.innerHTML !== el.textContent;
    });

    expect(hasHighlighting).toBeTruthy();
  });

  test('each code block has a language label', async ({ page }) => {
    // Test Case 8: Verify language labels on code blocks
    const codeBlocks = page.locator('#quickstart .code-block');
    const count = await codeBlocks.count();

    expect(count).toBeGreaterThan(0);

    for (let i = 0; i < count; i++) {
      const block = codeBlocks.nth(i);
      const languageLabel = block.locator('.code-block__language');

      await expect(languageLabel).toBeVisible();

      const labelText = await languageLabel.textContent();
      expect(labelText).toBeTruthy();
      expect(labelText.trim().length).toBeGreaterThan(0);
    }
  });

  test('all copy buttons are accessible via keyboard', async ({ page }) => {
    // Accessibility: copy buttons should be keyboard accessible
    const copyButtons = page.locator('#quickstart .code-block__copy');
    const count = await copyButtons.count();

    for (let i = 0; i < count; i++) {
      const button = copyButtons.nth(i);

      // Check that button is focusable
      await button.focus();
      await expect(button).toBeFocused();

      // Check that button has accessible label
      const ariaLabel = await button.getAttribute('aria-label');
      expect(ariaLabel).toBeTruthy();
    }
  });

  test('code blocks are horizontally scrollable on overflow', async ({ page }) => {
    // Check that code blocks handle overflow properly
    const codeBlocks = page.locator('#quickstart .code-block pre');
    const firstBlock = codeBlocks.first();

    // Check overflow-x is set to auto or scroll
    const overflowX = await firstBlock.evaluate((el) => {
      return window.getComputedStyle(el).overflowX;
    });

    expect(['auto', 'scroll']).toContain(overflowX);
  });

  test('Quick Start section is navigable from hero CTA', async ({ page }) => {
    // Check that clicking Get Started scrolls to Quick Start
    const ctaButton = page.locator('.hero__cta');
    await expect(ctaButton).toBeVisible();

    await ctaButton.click();

    // Wait for smooth scroll
    await page.waitForTimeout(500);

    // Check that Quick Start section is now in view
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeInViewport();
  });
});
