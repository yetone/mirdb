import { test, expect } from '@playwright/test';

test.describe('Quick Start Section with Code Examples', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case 1: Navigate to quick-start section - displays numbered installation steps
  test('TC1: Quick start section displays numbered installation steps', async ({ page }) => {
    // Click the Get Started button to navigate to quick-start section
    await page.click('[data-testid="get-started-button"]');

    // Wait for the quick-start section to be visible
    const quickStartSection = page.locator('[data-testid="quick-start-section"]');
    await expect(quickStartSection).toBeVisible();

    // Verify numbered installation steps are present
    const stepNumbers = page.locator('[data-testid="step-number"]');
    await expect(stepNumbers).toHaveCount(4);

    // Verify steps are numbered 1, 2, 3, 4
    const steps = await stepNumbers.allTextContents();
    expect(steps).toEqual(['1', '2', '3', '4']);

    // Verify step containers exist
    await expect(page.locator('[data-testid="installation-step-1"]')).toBeVisible();
    await expect(page.locator('[data-testid="installation-step-2"]')).toBeVisible();
    await expect(page.locator('[data-testid="installation-step-3"]')).toBeVisible();
    await expect(page.locator('[data-testid="installation-step-4"]')).toBeVisible();
  });

  // Test Case 2: Shell commands have syntax highlighting applied
  test('TC2: Shell code block has syntax highlighting applied', async ({ page }) => {
    // Navigate to quick-start section
    await page.click('[data-testid="get-started-button"]');

    // Find shell code blocks (they use language-bash class)
    const shellCodeBlocks = page.locator('[data-testid="code-block-shell"], [data-testid="code-block-bash"]');
    const shellCodeBlockCount = await shellCodeBlocks.count();
    expect(shellCodeBlockCount).toBeGreaterThan(0);

    // Check for syntax highlighting tokens in shell code
    const firstShellBlock = shellCodeBlocks.first();
    await expect(firstShellBlock).toBeVisible();

    // Verify Prism classes are applied
    const preElement = firstShellBlock.locator('pre.language-bash');
    await expect(preElement).toBeVisible();

    const codeElement = firstShellBlock.locator('code.language-bash');
    await expect(codeElement).toBeVisible();

    // Verify syntax highlighting tokens exist (comments have .token.comment class)
    const tokens = firstShellBlock.locator('.token');
    const tokenCount = await tokens.count();
    expect(tokenCount).toBeGreaterThan(0);
  });

  // Test Case 3: TOML code block has syntax highlighting for keys and values
  test('TC3: TOML code block has syntax highlighting for keys and values', async ({ page }) => {
    // Navigate to quick-start section
    await page.click('[data-testid="get-started-button"]');

    // Find TOML code block
    const tomlCodeBlock = page.locator('[data-testid="code-block-toml"]');
    await expect(tomlCodeBlock).toBeVisible();

    // Verify Prism classes for TOML
    const preElement = tomlCodeBlock.locator('pre.language-toml');
    await expect(preElement).toBeVisible();

    const codeElement = tomlCodeBlock.locator('code.language-toml');
    await expect(codeElement).toBeVisible();

    // Verify syntax highlighting tokens exist
    const tokens = tomlCodeBlock.locator('.token');
    const tokenCount = await tokens.count();
    expect(tokenCount).toBeGreaterThan(0);

    // Check for specific TOML content highlighting (keys, strings, numbers)
    const codeContent = await codeElement.textContent();
    expect(codeContent).toContain('[server]');
    expect(codeContent).toContain('host');
    expect(codeContent).toContain('port');
  });

  // Test Case 4: Click copy button copies code to clipboard
  test('TC4: Copy button copies code content to clipboard', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    // Navigate to quick-start section
    await page.click('[data-testid="get-started-button"]');

    // Find the first code block's copy button
    const copyButtons = page.locator('[data-testid="copy-button"]');
    const firstCopyButton = copyButtons.first();

    // Hover over the code block to make the copy button visible
    const firstCodeBlock = page.locator('.code-block-container').first();
    await firstCodeBlock.hover();

    await expect(firstCopyButton).toBeVisible();

    // Click the copy button
    await firstCopyButton.click();

    // Verify button text changes to "Copied!"
    await expect(firstCopyButton).toHaveText('Copied!');

    // Verify clipboard content
    const clipboardContent = await page.evaluate(() => navigator.clipboard.readText());
    expect(clipboardContent.length).toBeGreaterThan(0);
    expect(clipboardContent).toContain('cargo install mirdb-server');
  });

  // Test Case 5: Rust code examples display with proper syntax highlighting
  test('TC5: Rust code examples display with proper syntax highlighting', async ({ page }) => {
    // Navigate to quick-start section
    await page.click('[data-testid="get-started-button"]');

    // Find Rust code block
    const rustCodeBlock = page.locator('[data-testid="code-block-rust"]');
    await expect(rustCodeBlock).toBeVisible();

    // Verify Prism classes for Rust
    const preElement = rustCodeBlock.locator('pre.language-rust');
    await expect(preElement).toBeVisible();

    const codeElement = rustCodeBlock.locator('code.language-rust');
    await expect(codeElement).toBeVisible();

    // Verify syntax highlighting tokens exist
    const tokens = rustCodeBlock.locator('.token');
    const tokenCount = await tokens.count();
    expect(tokenCount).toBeGreaterThan(0);

    // Verify Rust-specific content is present
    const codeContent = await codeElement.textContent();
    expect(codeContent).toContain('use std::net::TcpStream');
    expect(codeContent).toContain('fn main()');

    // Check for Rust-specific token types (keywords like fn, use, let)
    const keywords = rustCodeBlock.locator('.token.keyword');
    const keywordCount = await keywords.count();
    expect(keywordCount).toBeGreaterThan(0);
  });
});
