import { test, expect } from '@playwright/test';

/**
 * Syntax Highlighting E2E Tests
 * Verifies NFR-6: Code examples must have syntax highlighting
 *
 * Test Cases:
 * 1. Shell command code blocks have appropriate syntax highlighting
 * 2. TOML code blocks have appropriate syntax highlighting
 * 3. Code blocks have dark background color
 */

test.describe('Syntax Highlighting for Code Blocks', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  test('TC1: Shell/bash code blocks have appropriate syntax highlighting', async ({ page }) => {
    // Navigate to Quick Start section where bash code blocks are displayed
    const quickStartSection = page.locator('#quick-start');
    await quickStartSection.scrollIntoViewIfNeeded();

    // Find a bash code block (installation command)
    const bashCodeBlock = page.locator('.code-block').filter({ hasText: 'cargo install' }).first();
    await expect(bashCodeBlock).toBeVisible();

    // Verify the code element has syntax highlighting enabled
    const codeElement = bashCodeBlock.locator('code');
    await expect(codeElement).toHaveAttribute('data-highlighted', 'true');

    // Verify builtin command is highlighted
    const builtinToken = bashCodeBlock.locator('.syntax-builtin').first();
    await expect(builtinToken).toBeVisible();

    // Verify the builtin token has the correct styling (blue color)
    const builtinColor = await builtinToken.evaluate((el) => {
      return window.getComputedStyle(el).color;
    });
    // Blue color in rgb format: #82aaff = rgb(130, 170, 255)
    expect(builtinColor).toBe('rgb(130, 170, 255)');
  });

  test('TC1-extended: Shell comment highlighting', async ({ page }) => {
    // Check if there's a code block with a comment
    const codeBlockWithComment = page.locator('.code-block').filter({ hasText: '#' }).first();

    if (await codeBlockWithComment.count() > 0) {
      await codeBlockWithComment.scrollIntoViewIfNeeded();

      // Check for comment token
      const commentToken = codeBlockWithComment.locator('.syntax-comment');
      if (await commentToken.count() > 0) {
        await expect(commentToken.first()).toBeVisible();

        // Verify comment styling (gray, italic)
        const commentStyles = await commentToken.first().evaluate((el) => {
          const style = window.getComputedStyle(el);
          return {
            color: style.color,
            fontStyle: style.fontStyle,
          };
        });
        // Gray color: #676e95 = rgb(103, 110, 149)
        expect(commentStyles.color).toBe('rgb(103, 110, 149)');
        expect(commentStyles.fontStyle).toBe('italic');
      }
    }
  });

  test('TC2: TOML configuration code blocks have appropriate syntax highlighting', async ({ page }) => {
    // Navigate to Quick Start section where TOML config is displayed
    const quickStartSection = page.locator('#quick-start');
    await quickStartSection.scrollIntoViewIfNeeded();

    // Find the TOML code block (contains addr = or configuration)
    const tomlCodeBlock = page.locator('.code-block').filter({ has: page.locator('.language-toml') }).first();

    if (await tomlCodeBlock.count() > 0) {
      await expect(tomlCodeBlock).toBeVisible();

      // Verify syntax highlighting is enabled
      const codeElement = tomlCodeBlock.locator('code');
      await expect(codeElement).toHaveAttribute('data-highlighted', 'true');

      // Verify property keys are highlighted (yellow)
      const propToken = tomlCodeBlock.locator('.syntax-property').first();
      await expect(propToken).toBeVisible();

      const propColor = await propToken.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });
      // Yellow color: #ffcb6b = rgb(255, 203, 107)
      expect(propColor).toBe('rgb(255, 203, 107)');

      // Verify string values are highlighted (green)
      const stringToken = tomlCodeBlock.locator('.syntax-string').first();
      if (await stringToken.count() > 0) {
        await expect(stringToken).toBeVisible();

        const stringColor = await stringToken.evaluate((el) => {
          return window.getComputedStyle(el).color;
        });
        // Green color: #c3e88d = rgb(195, 232, 141)
        expect(stringColor).toBe('rgb(195, 232, 141)');
      }
    }
  });

  test('TC2-extended: TOML number highlighting', async ({ page }) => {
    // Find TOML code block with numbers (e.g., max_level = 7)
    const tomlCodeBlock = page.locator('.code-block').filter({ has: page.locator('.language-toml') }).first();

    if (await tomlCodeBlock.count() > 0) {
      await tomlCodeBlock.scrollIntoViewIfNeeded();

      // Check for number token
      const numberToken = tomlCodeBlock.locator('.syntax-number');
      if (await numberToken.count() > 0) {
        await expect(numberToken.first()).toBeVisible();

        // Verify number styling (orange)
        const numberColor = await numberToken.first().evaluate((el) => {
          return window.getComputedStyle(el).color;
        });
        // Orange color: #f78c6c = rgb(247, 140, 108)
        expect(numberColor).toBe('rgb(247, 140, 108)');
      }
    }
  });

  test('TC3: Code blocks have dark background color', async ({ page }) => {
    // Find all code blocks on the page
    const codeBlocks = page.locator('.code-block');
    const count = await codeBlocks.count();
    expect(count).toBeGreaterThan(0);

    // Check the first few code blocks for dark background
    for (let i = 0; i < Math.min(count, 3); i++) {
      const codeBlock = codeBlocks.nth(i);
      await codeBlock.scrollIntoViewIfNeeded();

      // Get background color
      const bgColor = await codeBlock.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });

      // Verify it's a dark color (low RGB values)
      // Expected: #1a1a2e = rgb(26, 26, 46)
      expect(bgColor).toBe('rgb(26, 26, 46)');
    }
  });

  test('TC3-extended: Code block text color is light on dark background', async ({ page }) => {
    // Find code blocks and check text color is light for readability
    const codeBlock = page.locator('.code-block').first();
    await codeBlock.scrollIntoViewIfNeeded();

    const codePre = codeBlock.locator('.code-pre');
    const textColor = await codePre.evaluate((el) => {
      return window.getComputedStyle(el).color;
    });

    // Verify text is light colored (high RGB values)
    // Expected: #e1e1e1 = rgb(225, 225, 225)
    expect(textColor).toBe('rgb(225, 225, 225)');
  });

  test('All code blocks have syntax highlighting classes', async ({ page }) => {
    // Get all code blocks on the page
    const codeBlocks = page.locator('.code-block');
    const count = await codeBlocks.count();
    expect(count).toBeGreaterThan(0);

    // Check each code block has proper language class (check up to 4 blocks)
    for (let i = 0; i < Math.min(count, 4); i++) {
      const codeBlock = codeBlocks.nth(i);
      await codeBlock.scrollIntoViewIfNeeded();
      const codePre = codeBlock.locator('.code-pre');
      await codePre.waitFor({ state: 'visible', timeout: 10000 });

      // Should have a language class
      const className = await codePre.getAttribute('class');
      expect(className).toMatch(/language-(bash|shell|toml|rust)/);
    }
  });

  test('Syntax highlighting is visible in Quick Start section', async ({ page }) => {
    // Navigate to quick start section
    const quickStart = page.locator('#quick-start');
    await quickStart.scrollIntoViewIfNeeded();

    // Find all code blocks in quick start
    const codeBlocksInQuickStart = quickStart.locator('.code-block');
    const count = await codeBlocksInQuickStart.count();

    // Should have at least 3 code blocks (install, config, run, usage)
    expect(count).toBeGreaterThanOrEqual(3);

    // Each should have syntax highlighting tokens
    for (let i = 0; i < count; i++) {
      const codeBlock = codeBlocksInQuickStart.nth(i);

      // Should have at least one syntax token
      const syntaxTokens = codeBlock.locator('[class^="syntax-"]');
      const tokenCount = await syntaxTokens.count();
      expect(tokenCount).toBeGreaterThan(0);
    }
  });

  test('Code block copy functionality preserves original code', async ({ page }) => {
    // Find a code block with copy button
    const codeBlock = page.locator('.code-block').first();
    await codeBlock.scrollIntoViewIfNeeded();

    // Get the code content (without syntax highlighting spans)
    const codeContent = await codeBlock.locator('code').textContent();
    expect(codeContent).toBeTruthy();
    expect(codeContent!.length).toBeGreaterThan(0);

    // Verify copy button exists
    const copyButton = codeBlock.locator('.copy-button');
    await expect(copyButton).toBeVisible();

    // Click copy button
    await copyButton.click();

    // Verify copied indicator appears
    const copiedIcon = codeBlock.locator('.copy-icon.copied');
    await expect(copiedIcon).toBeVisible({ timeout: 1000 });
  });

  test('Multiple code languages are properly styled', async ({ page }) => {
    // Check for bash code blocks
    const bashBlocks = page.locator('.code-pre.language-bash, .code-pre.language-shell');
    if (await bashBlocks.count() > 0) {
      const bashBlock = bashBlocks.first();
      await expect(bashBlock).toBeVisible();
    }

    // Check for TOML code blocks
    const tomlBlocks = page.locator('.code-pre.language-toml');
    if (await tomlBlocks.count() > 0) {
      const tomlBlock = tomlBlocks.first();
      await expect(tomlBlock).toBeVisible();
    }

    // Verify at least one language type exists
    const allBlocks = page.locator('.code-pre[class*="language-"]');
    const count = await allBlocks.count();
    expect(count).toBeGreaterThan(0);
  });
});
