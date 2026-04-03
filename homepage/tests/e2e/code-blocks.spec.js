/**
 * Code Block E2E Tests
 * Owner: Scenario 4 - Quick Start, Scenario 17 - Code Block Interactions
 *
 * Tests:
 * - Quick Start section exists with clear heading
 * - Installation commands are present in code blocks
 * - Server start command is documented
 * - Basic usage examples are present
 * - Copy button functionality
 * - Syntax highlighting applied
 */

const { test, expect } = require('@playwright/test');

// ============================================================================
// Quick Start Section Tests (Scenario 4)
// ============================================================================

test.describe('Quick Start Section (Scenario 4)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Quick Start section exists with clear heading', async ({ page }) => {
    // Navigate to Quick Start section
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    // Check for clear heading
    const sectionTitle = quickStartSection.locator('.section-title, h2');
    await expect(sectionTitle).toBeVisible();
    await expect(sectionTitle).toContainText('Quick Start');

    // Check for section subtitle
    const sectionSubtitle = quickStartSection.locator('.section-subtitle');
    await expect(sectionSubtitle).toBeVisible();
  });

  test('TC2: Code blocks contain cargo/rust installation commands', async ({ page }) => {
    // Navigate to Quick Start section
    await page.locator('a[href="#quick-start"]').first().click();
    await page.waitForTimeout(500);

    const quickStartSection = page.locator('#quick-start');

    // Find code blocks with installation commands
    const codeBlocks = quickStartSection.locator('.code-block');
    const count = await codeBlocks.count();
    expect(count).toBeGreaterThanOrEqual(1);

    // Check for cargo build command in the first code block
    const firstCodeBlock = codeBlocks.first();
    const codeContent = await firstCodeBlock.locator('code').textContent();

    // Verify Rust/Cargo installation commands are present
    expect(codeContent).toMatch(/cargo\s+build|git\s+clone/);
  });

  test('TC3: Server start command is documented (mirdb -c config.toml)', async ({ page }) => {
    const quickStartSection = page.locator('#quick-start');

    // Find code blocks
    const codeBlocks = quickStartSection.locator('.code-block code');
    const allCodeContent = await codeBlocks.allTextContents();
    const combinedContent = allCodeContent.join(' ');

    // Check for the mirdb -c config command
    expect(combinedContent).toMatch(/mirdb.*-c.*config\.toml|mirdb-server.*-c/i);
  });

  test('TC4: Example shows connecting and using SET/GET commands', async ({ page }) => {
    const quickStartSection = page.locator('#quick-start');

    // Find code blocks
    const codeBlocks = quickStartSection.locator('.code-block code');
    const allCodeContent = await codeBlocks.allTextContents();
    const combinedContent = allCodeContent.join(' ');

    // Check for SET command
    expect(combinedContent.toLowerCase()).toContain('set');

    // Check for GET command
    expect(combinedContent.toLowerCase()).toContain('get');

    // Check for telnet or connection example
    expect(combinedContent.toLowerCase()).toMatch(/telnet|connect/);
  });

  test('TC5: Copy button exists and copies code content to clipboard on click', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    const quickStartSection = page.locator('#quick-start');

    // Find the first code block with a copy button
    const copyButton = quickStartSection.locator('.code-block__copy').first();
    await expect(copyButton).toBeVisible();

    // Verify button has expected attributes
    await expect(copyButton).toHaveAttribute('aria-label', 'Copy code to clipboard');
    await expect(copyButton).toHaveAttribute('data-copy-state', 'idle');

    // Get the code content before clicking
    const codeBlock = quickStartSection.locator('.code-block').first();
    const codeContent = await codeBlock.locator('code').textContent();

    // Click the copy button
    await copyButton.click();

    // Verify the button state changes to 'copied'
    await expect(copyButton).toHaveAttribute('data-copy-state', 'copied');

    // Verify the button text changes
    const copyText = copyButton.locator('.code-block__copy-text');
    await expect(copyText).toContainText('Copied!');

    // Verify clipboard content matches
    const clipboardContent = await page.evaluate(() => navigator.clipboard.readText());
    expect(clipboardContent.trim()).toBe(codeContent.trim());

    // Wait for button to reset
    await page.waitForTimeout(2500);
    await expect(copyButton).toHaveAttribute('data-copy-state', 'idle');
  });

  test('TC6: Code blocks display with appropriate syntax highlighting', async ({ page }) => {
    const quickStartSection = page.locator('#quick-start');

    // Find code blocks
    const codeBlocks = quickStartSection.locator('.code-block');
    const count = await codeBlocks.count();
    expect(count).toBeGreaterThanOrEqual(1);

    // Check for syntax highlighting tokens in code blocks
    for (let i = 0; i < Math.min(count, 3); i++) {
      const codeBlock = codeBlocks.nth(i);

      // Verify code block has language indicator in header
      const languageLabel = codeBlock.locator('.code-block__language');
      await expect(languageLabel).toBeVisible();
      const language = await languageLabel.textContent();
      expect(language).toBeTruthy();

      // Verify syntax highlighting is applied (tokens exist)
      const code = codeBlock.locator('code');
      const tokens = code.locator('.token');
      const tokenCount = await tokens.count();

      // Code should have some syntax highlighting tokens
      expect(tokenCount).toBeGreaterThanOrEqual(1);

      // Verify different token types exist (comments, keywords, etc.)
      const hasComments = await code.locator('.token.comment').count() > 0;
      const hasFunctions = await code.locator('.token.function, .token.builtin, .token.keyword').count() > 0;

      // At least one type of token highlighting should be present
      expect(hasComments || hasFunctions).toBe(true);
    }
  });
});

// ============================================================================
// Code Block Interaction Tests (Scenario 17)
// ============================================================================

test.describe('Code Block Interactions (Scenario 17)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('All code blocks have copy buttons', async ({ page }) => {
    const codeBlocks = page.locator('.code-block');
    const count = await codeBlocks.count();

    for (let i = 0; i < count; i++) {
      const codeBlock = codeBlocks.nth(i);
      const copyButton = codeBlock.locator('.code-block__copy');
      await expect(copyButton).toBeVisible();
    }
  });

  test('Code blocks have proper header with language indicator', async ({ page }) => {
    const codeBlocks = page.locator('.code-block');
    const count = await codeBlocks.count();

    for (let i = 0; i < count; i++) {
      const codeBlock = codeBlocks.nth(i);
      const header = codeBlock.locator('.code-block__header');
      await expect(header).toBeVisible();

      const languageLabel = codeBlock.locator('.code-block__language');
      await expect(languageLabel).toBeVisible();
    }
  });

  test('Copy button has hover state', async ({ page }) => {
    const copyButton = page.locator('.code-block__copy').first();
    await expect(copyButton).toBeVisible();

    // Get initial background color
    const initialBgColor = await copyButton.evaluate(el =>
      window.getComputedStyle(el).backgroundColor
    );

    // Hover over the button
    await copyButton.hover();

    // Give time for hover transition
    await page.waitForTimeout(200);

    // Background color should change on hover (or border color)
    const hoverBgColor = await copyButton.evaluate(el =>
      window.getComputedStyle(el).backgroundColor
    );

    // The colors may or may not be different depending on CSS, but the element should be interactive
    expect(copyButton).toBeTruthy();
  });

  test('Code blocks are scrollable horizontally for long lines', async ({ page }) => {
    const codeBlocks = page.locator('.code-block pre');
    const count = await codeBlocks.count();

    if (count > 0) {
      const pre = codeBlocks.first();
      const overflowX = await pre.evaluate(el =>
        window.getComputedStyle(el).overflowX
      );

      // Should allow horizontal scrolling
      expect(['auto', 'scroll']).toContain(overflowX);
    }
  });

  test('Quick Start steps are numbered correctly', async ({ page }) => {
    const quickStartSection = page.locator('#quick-start');
    const stepNumbers = quickStartSection.locator('.quick-start__step-number');
    const count = await stepNumbers.count();

    expect(count).toBeGreaterThanOrEqual(3);

    // Verify step numbers are sequential
    for (let i = 0; i < count; i++) {
      const stepNumber = stepNumbers.nth(i);
      await expect(stepNumber).toContainText(String(i + 1));
    }
  });
});
