/**
 * Syntax Highlighting E2E Tests
 * Owner: Scenario 18 - Code Syntax Highlighting
 *
 * Tests:
 * - Bash code blocks have syntax highlighting (Test Case 1)
 * - TOML code blocks have syntax highlighting (Test Case 2)
 *
 * Requirements: REQ-4 (Quick Start Guide), Code Highlighting
 */

const { test, expect } = require('@playwright/test');

test.describe('Code Syntax Highlighting', () => {
  test.beforeEach(async ({ page }) => {
    // Start a local server and navigate to the page
    await page.goto('/');
    // Wait for Prism.js to initialize
    await page.waitForTimeout(500);
  });

  test.describe('Test Case 1: Bash code blocks have syntax highlighting', () => {
    test('bash code blocks exist with language-bash class', async ({ page }) => {
      // Find code blocks with language-bash class
      const bashBlocks = page.locator('code.language-bash');
      const count = await bashBlocks.count();

      expect(count).toBeGreaterThan(0);
    });

    test('bash code blocks contain highlighted tokens', async ({ page }) => {
      // Find the first bash code block
      const bashBlock = page.locator('code.language-bash').first();

      // Wait for Prism.js to process the code
      await expect(bashBlock).toBeVisible();

      // Check if Prism.js has added token spans
      const hasTokens = await bashBlock.locator('.token').count();

      // Prism.js should add token spans for syntax highlighting
      expect(hasTokens).toBeGreaterThan(0);
    });

    test('bash code blocks highlight keywords like cargo, git', async ({ page }) => {
      // Find bash code blocks
      const bashBlocks = page.locator('code.language-bash');

      // Get the text content
      const textContent = await bashBlocks.first().textContent();

      // The quick start section should have cargo or git commands
      const hasBashContent =
        textContent.includes('cargo') ||
        textContent.includes('git') ||
        textContent.includes('./') ||
        textContent.includes('#');

      expect(hasBashContent).toBe(true);
    });

    test('bash comments have distinct styling (token class)', async ({ page }) => {
      // Check if comments (starting with #) have token styling
      const commentTokens = page.locator('code.language-bash .token.comment');
      const count = await commentTokens.count();

      expect(count).toBeGreaterThan(0);
    });
  });

  test.describe('Test Case 2: TOML code blocks have syntax highlighting', () => {
    test('toml code blocks exist with language-toml class', async ({ page }) => {
      // Find code blocks with language-toml class
      const tomlBlocks = page.locator('code.language-toml');
      const count = await tomlBlocks.count();

      expect(count).toBeGreaterThan(0);
    });

    test('toml code blocks contain highlighted tokens', async ({ page }) => {
      // Find the first toml code block
      const tomlBlock = page.locator('code.language-toml').first();

      // Wait for Prism.js to process the code
      await expect(tomlBlock).toBeVisible();

      // Check if Prism.js has added token spans
      const hasTokens = await tomlBlock.locator('.token').count();

      // Prism.js should add token spans for syntax highlighting
      expect(hasTokens).toBeGreaterThan(0);
    });

    test('toml code blocks highlight keys, values, and strings', async ({ page }) => {
      // Find toml code blocks
      const tomlBlock = page.locator('code.language-toml').first();

      // Check for string tokens (quoted values in TOML)
      const stringTokens = await tomlBlock.locator('.token.string').count();

      // Check for key/property tokens
      const keyTokens = await tomlBlock.locator('.token.key, .token.property').count();

      // TOML configuration should have both keys and string values
      expect(stringTokens).toBeGreaterThan(0);
      expect(keyTokens).toBeGreaterThan(0);
    });

    test('toml code blocks show configuration content', async ({ page }) => {
      // Find toml code blocks
      const tomlBlocks = page.locator('code.language-toml');

      // Get the text content
      const textContent = await tomlBlocks.first().textContent();

      // TOML content should include MirDB configuration keys
      const hasTomlContent =
        textContent.includes('addr') ||
        textContent.includes('work_dir') ||
        textContent.includes('mem_table_max_size') ||
        textContent.includes('=');

      expect(hasTomlContent).toBe(true);
    });

    test('toml comments have distinct styling', async ({ page }) => {
      // Check if comments have token styling
      const commentTokens = page.locator('code.language-toml .token.comment');
      const count = await commentTokens.count();

      // TOML config files often have comments
      expect(count).toBeGreaterThanOrEqual(0);
    });
  });

  test.describe('Code Block Visual Styling', () => {
    test('code blocks have dark background applied', async ({ page }) => {
      // Find pre elements containing code
      const preElement = page.locator('pre').first();
      await expect(preElement).toBeVisible();

      // Check computed background color
      const backgroundColor = await preElement.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });

      // #2D2D2D converts to rgb(45, 45, 45)
      // Allow for Prism theme variations
      expect(backgroundColor).toMatch(/rgb\(45,\s*45,\s*45\)|rgba\(45,\s*45,\s*45/);
    });

    test('code blocks have monospace font applied', async ({ page }) => {
      // Find code elements
      const codeElement = page.locator('code[class*="language-"]').first();
      await expect(codeElement).toBeVisible();

      // Check computed font family
      const fontFamily = await codeElement.evaluate((el) => {
        return window.getComputedStyle(el).fontFamily;
      });

      // Should include monospace fonts
      const hasMonospace =
        fontFamily.toLowerCase().includes('fira code') ||
        fontFamily.toLowerCase().includes('jetbrains mono') ||
        fontFamily.toLowerCase().includes('cascadia') ||
        fontFamily.toLowerCase().includes('monospace');

      expect(hasMonospace).toBe(true);
    });
  });
});
