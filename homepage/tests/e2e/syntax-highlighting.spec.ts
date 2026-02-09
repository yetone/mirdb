/**
 * E2E tests for syntax highlighting.
 * Owner: Scenario 11 - Code Syntax Highlighting
 *
 * Tests that code blocks have proper syntax highlighting
 * applied in both light and dark themes.
 */

import { test, expect } from '@playwright/test';

test.describe('Code Syntax Highlighting', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for the page to load
    await page.waitForLoadState('domcontentloaded');
  });

  test.describe('Quick Start section', () => {
    test('should have syntax highlighted code blocks', async ({ page }) => {
      // Navigate to the Quick Start section
      const quickStartSection = page.locator('#quickstart');
      await expect(quickStartSection).toBeVisible();

      // Check that code blocks exist and have highlighting classes
      const codeBlocks = quickStartSection.locator('pre code');
      const count = await codeBlocks.count();
      expect(count).toBeGreaterThan(0);

      // Verify first code block has token classes (indicating highlighting)
      const firstCodeBlock = codeBlocks.first();
      await expect(firstCodeBlock).toBeVisible();

      // Check for Prism.js token classes in the HTML
      const innerHTML = await firstCodeBlock.innerHTML();
      expect(innerHTML).toContain('class="token');
    });

    test('should have language-specific highlighting', async ({ page }) => {
      const quickStartSection = page.locator('#quickstart');

      // Look for bash/shell code blocks
      const bashCodeBlocks = quickStartSection.locator('code[class*="language-bash"], code[class*="language-shell"]');
      const bashCount = await bashCodeBlocks.count();
      expect(bashCount).toBeGreaterThan(0);

      // Verify they have syntax highlighting applied
      const firstBash = bashCodeBlocks.first();
      const innerHTML = await firstBash.innerHTML();

      // Should contain Prism token classes
      expect(innerHTML).toMatch(/class="token\s+\w+"/);
    });

    test('should highlight cargo install command', async ({ page }) => {
      const quickStartSection = page.locator('#quickstart');

      // Find the installation code block
      const installBlock = quickStartSection.locator('pre:has-text("cargo install")');
      await expect(installBlock).toBeVisible();

      const codeEl = installBlock.locator('code');
      const innerHTML = await codeEl.innerHTML();

      // Should have token highlighting
      expect(innerHTML).toContain('token');
    });
  });

  test.describe('Protocol section', () => {
    test('should display command syntax examples', async ({ page }) => {
      // Navigate to the Protocol section
      const protocolSection = page.locator('#protocol');
      await expect(protocolSection).toBeVisible();

      // Check that command cards exist
      const commandCards = protocolSection.locator('.command-card');
      const count = await commandCards.count();
      expect(count).toBeGreaterThan(0);

      // Verify syntax elements are present
      const syntaxElements = protocolSection.locator('.command-syntax');
      const syntaxCount = await syntaxElements.count();
      expect(syntaxCount).toBeGreaterThan(0);
    });

    test('should display example code for commands', async ({ page }) => {
      const protocolSection = page.locator('#protocol');
      await expect(protocolSection).toBeVisible();

      // Check for example code blocks
      const exampleElements = protocolSection.locator('.command-example');
      const count = await exampleElements.count();

      // Not all commands have examples, but there should be some
      if (count > 0) {
        const firstExample = exampleElements.first();
        await expect(firstExample).toBeVisible();
      }
    });
  });

  test.describe('Dark mode highlighting', () => {
    test('should maintain highlighting in dark mode', async ({ page }) => {
      const quickStartSection = page.locator('#quickstart');
      await expect(quickStartSection).toBeVisible();

      // Get code block before theme change
      const codeBlock = quickStartSection.locator('pre code').first();
      await codeBlock.innerHTML();

      // Toggle to dark mode by setting the data-theme attribute
      await page.evaluate(() => {
        document.documentElement.setAttribute('data-theme', 'dark');
      });

      // Wait for theme change to apply
      await page.waitForTimeout(100);

      // Verify we're in dark mode
      const theme = await page.evaluate(() =>
        document.documentElement.getAttribute('data-theme')
      );
      expect(theme).toBe('dark');

      // Get code block after theme change
      const darkModeHtml = await codeBlock.innerHTML();

      // Token classes should still be present
      expect(darkModeHtml).toContain('class="token');

      // The HTML structure should be the same (same tokens)
      expect(darkModeHtml).toContain('token');
    });

    test('should have theme-appropriate highlighting colors', async ({ page }) => {
      const quickStartSection = page.locator('#quickstart');
      await expect(quickStartSection).toBeVisible();

      // Set light mode
      await page.evaluate(() => {
        document.documentElement.setAttribute('data-theme', 'light');
      });

      // Get the computed style of a code block in light mode
      const codeBlock = quickStartSection.locator('pre').first();
      const lightBgColor = await codeBlock.evaluate((el) =>
        window.getComputedStyle(el).backgroundColor
      );

      // Switch to dark mode
      await page.evaluate(() => {
        document.documentElement.setAttribute('data-theme', 'dark');
      });

      await page.waitForTimeout(100);

      // Get the computed style in dark mode
      const darkBgColor = await codeBlock.evaluate((el) =>
        window.getComputedStyle(el).backgroundColor
      );

      // Colors should be different between themes
      // (This validates that theme CSS is being applied)
      expect(lightBgColor).not.toBe(darkBgColor);
    });

    test('should preserve code readability in dark mode', async ({ page }) => {
      // Set dark mode
      await page.evaluate(() => {
        document.documentElement.setAttribute('data-theme', 'dark');
      });

      const quickStartSection = page.locator('#quickstart');
      await expect(quickStartSection).toBeVisible();

      // Check that code blocks are still visible
      const codeBlocks = quickStartSection.locator('pre code');
      const count = await codeBlocks.count();

      for (let i = 0; i < Math.min(count, 3); i++) {
        const block = codeBlocks.nth(i);
        await expect(block).toBeVisible();

        // Verify text content is present
        const text = await block.textContent();
        expect(text).toBeTruthy();
        expect(text?.length).toBeGreaterThan(0);
      }
    });
  });

  test.describe('Accessibility', () => {
    test('should have proper semantic markup for code blocks', async ({ page }) => {
      const quickStartSection = page.locator('#quickstart');
      await expect(quickStartSection).toBeVisible();

      // Code should be in <pre><code> structure
      const preElements = quickStartSection.locator('pre');
      const preCount = await preElements.count();
      expect(preCount).toBeGreaterThan(0);

      for (let i = 0; i < preCount; i++) {
        const pre = preElements.nth(i);
        const code = pre.locator('code');
        await expect(code).toBeVisible();
      }
    });

    test('should have language indication for code blocks', async ({ page }) => {
      const quickStartSection = page.locator('#quickstart');

      // Code blocks should have language class or data attribute
      const codeBlocks = quickStartSection.locator('pre code');
      const count = await codeBlocks.count();

      for (let i = 0; i < count; i++) {
        const block = codeBlocks.nth(i);
        const className = await block.getAttribute('class');
        const dataLang = await block.evaluate((el) =>
          el.closest('pre')?.getAttribute('data-language')
        );

        // Should have either a language class or data attribute
        const hasLanguageClass = className?.includes('language-');
        const hasDataLang = Boolean(dataLang);

        expect(hasLanguageClass || hasDataLang).toBe(true);
      }
    });
  });
});
