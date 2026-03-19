/**
 * E2E tests for Quick Start section.
 * Owner: Scenario 5 - Quick Start Section with Code Examples
 */

import { test, expect } from '@playwright/test';

test.describe('Quick Start Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Scroll to the Quick Start section
    await page.locator('#quick-start').scrollIntoViewIfNeeded();
  });

  // Test Case 4: Click copy button on code block
  test('TC4: Copy button copies command to clipboard and shows success state', async ({
    page,
    context,
  }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    // Find the first code block with a copy button
    const codeBlock = page.locator('[data-testid="code-block"]').first();
    await expect(codeBlock).toBeVisible();

    // Find the copy button
    const copyButton = codeBlock.locator('[data-testid="copy-button"]');
    await expect(copyButton).toBeVisible();

    // Verify initial state shows "Copy"
    await expect(copyButton).toContainText('Copy');

    // Click the copy button
    await copyButton.click();

    // Verify the button shows "Copied!" state
    await expect(copyButton).toContainText('Copied!');

    // Verify aria-label is updated
    await expect(copyButton).toHaveAttribute('aria-label', 'Copied!');

    // Verify clipboard contains the command (git clone command)
    const clipboardText = await page.evaluate(async () => {
      return await navigator.clipboard.readText();
    });
    expect(clipboardText).toContain('git clone https://github.com/yetone/mirdb.git');
  });

  test('Quick Start section displays prerequisites', async ({ page }) => {
    const prerequisitesSection = page.locator('[data-testid="prerequisites-section"]');
    await expect(prerequisitesSection).toBeVisible();

    // Check for Rust toolchain
    await expect(prerequisitesSection).toContainText('Rust toolchain');
  });

  test('Quick Start section displays installation steps', async ({ page }) => {
    const installationSection = page.locator('[data-testid="installation-section"]');
    await expect(installationSection).toBeVisible();

    // Verify step descriptions
    await expect(page.getByText('1. Clone the repository')).toBeVisible();
    await expect(page.getByText('2. Navigate to the project directory')).toBeVisible();
    await expect(page.getByText('3. Build and run MirDB')).toBeVisible();
  });

  test('Quick Start section displays memcached connection example', async ({ page }) => {
    const usageSection = page.locator('[data-testid="usage-section"]');
    await expect(usageSection).toBeVisible();

    // Verify heading
    await expect(page.getByText('Connect Using Memcached Protocol')).toBeVisible();
  });

  test('All code blocks have copy buttons', async ({ page }) => {
    const codeBlocks = page.locator('#quick-start [data-testid="code-block"]');
    const count = await codeBlocks.count();

    // Should have at least 4 code blocks (3 installation steps + 1 usage example)
    expect(count).toBeGreaterThanOrEqual(4);

    // Each code block should have a copy button
    for (let i = 0; i < count; i++) {
      const copyButton = codeBlocks.nth(i).locator('[data-testid="copy-button"]');
      await expect(copyButton).toBeVisible();
    }
  });

  test('Code blocks display language indicators', async ({ page }) => {
    const languageLabels = page.locator('#quick-start [data-testid="code-language"]');
    const count = await languageLabels.count();

    expect(count).toBeGreaterThanOrEqual(4);

    // All should show "bash" language
    for (let i = 0; i < count; i++) {
      await expect(languageLabels.nth(i)).toContainText('bash');
    }
  });

  test('Copy button resets to initial state after delay', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    const copyButton = page
      .locator('[data-testid="code-block"]')
      .first()
      .locator('[data-testid="copy-button"]');

    // Click copy
    await copyButton.click();
    await expect(copyButton).toContainText('Copied!');

    // Wait for reset (default is 2 seconds + buffer)
    await page.waitForTimeout(2500);

    // Should reset to "Copy"
    await expect(copyButton).toContainText('Copy');
  });

  // Accessibility tests
  test('Quick Start section has proper heading structure', async ({ page }) => {
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toHaveAttribute('aria-labelledby', 'quick-start-heading');

    const heading = page.locator('#quick-start-heading');
    await expect(heading).toHaveText('Quick Start');
  });

  test('Prerequisites list has proper accessibility attributes', async ({ page }) => {
    const prerequisitesList = page.locator('[aria-label="Prerequisites list"]');
    await expect(prerequisitesList).toBeVisible();
  });

  test('Copy buttons have accessible labels', async ({ page }) => {
    const copyButtons = page.locator('#quick-start [data-testid="copy-button"]');
    const count = await copyButtons.count();

    for (let i = 0; i < count; i++) {
      const button = copyButtons.nth(i);
      const ariaLabel = await button.getAttribute('aria-label');
      expect(ariaLabel).toBe('Copy code to clipboard');
    }
  });
});

// Scenario 16: Code Syntax Highlighting E2E Tests
test.describe('Code Syntax Highlighting', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.locator('#quick-start').scrollIntoViewIfNeeded();
  });

  // Test Case 3: Syntax highlighting works in both light and dark themes
  test('TC3: Syntax highlighting colors are visible in light mode', async ({ page }) => {
    // Ensure light mode
    await page.emulateMedia({ colorScheme: 'light' });
    await page.evaluate(() => localStorage.removeItem('mirdb-theme'));
    await page.reload();
    await page.locator('#quick-start').scrollIntoViewIfNeeded();

    // Find the first code block
    const codeBlock = page.locator('[data-testid="code-block"]').first();
    await expect(codeBlock).toBeVisible();

    // Check code content exists
    const codeContent = codeBlock.locator('[data-testid="code-content"]');
    await expect(codeContent).toBeVisible();

    // Verify syntax highlighting elements exist (command highlighting)
    const highlightedElement = codeBlock.locator('.text-green-400').first();
    await expect(highlightedElement).toBeVisible();

    // Verify the highlighted element has visible color (not transparent)
    const color = await highlightedElement.evaluate((el) =>
      getComputedStyle(el).color
    );
    expect(color).not.toBe('rgba(0, 0, 0, 0)');
    expect(color).not.toBe('transparent');
  });

  test('TC3: Syntax highlighting colors are visible in dark mode', async ({ page }) => {
    // Enable dark mode via theme toggle
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    const themeToggle = page.getByTestId('theme-toggle');

    // Check if we need to toggle to dark mode
    const isDarkMode = await page.evaluate(() =>
      document.documentElement.classList.contains('dark')
    );

    if (!isDarkMode) {
      await themeToggle.click();
      await page.waitForTimeout(300);
    }

    // Verify dark mode is active
    await expect(page.locator('html')).toHaveClass(/dark/);

    // Scroll to Quick Start
    await page.locator('#quick-start').scrollIntoViewIfNeeded();

    // Find the first code block
    const codeBlock = page.locator('[data-testid="code-block"]').first();
    await expect(codeBlock).toBeVisible();

    // Check code content exists
    const codeContent = codeBlock.locator('[data-testid="code-content"]');
    await expect(codeContent).toBeVisible();

    // Verify syntax highlighting elements exist
    const highlightedCommand = codeBlock.locator('.text-green-400').first();
    await expect(highlightedCommand).toBeVisible();

    // Verify the highlighted element has visible green color
    const color = await highlightedCommand.evaluate((el) =>
      getComputedStyle(el).color
    );
    // Green should have high G value
    const rgbMatch = color.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
    expect(rgbMatch).not.toBeNull();
    if (rgbMatch) {
      const g = parseInt(rgbMatch[2]);
      expect(g).toBeGreaterThan(100); // Green component should be significant
    }
  });

  test('Code blocks display correct syntax highlighting for git command', async ({ page }) => {
    const codeBlock = page.locator('[data-testid="code-block"]').first();
    await expect(codeBlock).toBeVisible();

    // Verify 'git' command is highlighted (green)
    const gitCommand = codeBlock.locator('.text-green-400');
    await expect(gitCommand.first()).toBeVisible();

    // Verify URL is highlighted (blue)
    const urlHighlight = codeBlock.locator('.text-blue-400');
    await expect(urlHighlight.first()).toBeVisible();
  });

  test('Code blocks display correct syntax highlighting for cargo command', async ({ page }) => {
    // Find the code block with cargo command
    const codeBlocks = page.locator('[data-testid="code-block"]');
    const count = await codeBlocks.count();

    let cargoBlockFound = false;
    for (let i = 0; i < count; i++) {
      const block = codeBlocks.nth(i);
      const text = await block.textContent();
      if (text?.includes('cargo')) {
        cargoBlockFound = true;

        // Verify 'cargo' command is highlighted (green)
        const cargoCommand = block.locator('.text-green-400');
        await expect(cargoCommand.first()).toBeVisible();

        // Verify '--release' flag is highlighted (yellow)
        const releaseFlag = block.locator('.text-yellow-400');
        await expect(releaseFlag.first()).toBeVisible();
        break;
      }
    }

    expect(cargoBlockFound).toBe(true);
  });

  test('Language indicator is displayed above code block', async ({ page }) => {
    const codeBlock = page.locator('[data-testid="code-block"]').first();
    await expect(codeBlock).toBeVisible();

    // Verify language indicator exists and shows 'bash'
    const languageIndicator = codeBlock.locator('[data-testid="code-language"]');
    await expect(languageIndicator).toBeVisible();
    await expect(languageIndicator).toHaveText(/bash/i);
  });

  test('Syntax highlighting is consistent across multiple code blocks', async ({ page }) => {
    const codeBlocks = page.locator('#quick-start [data-testid="code-block"]');
    const count = await codeBlocks.count();

    // Should have multiple code blocks
    expect(count).toBeGreaterThanOrEqual(3);

    // Each code block should have syntax highlighting
    for (let i = 0; i < Math.min(count, 3); i++) {
      const block = codeBlocks.nth(i);
      await expect(block).toBeVisible();

      // Each should have a language indicator
      const languageIndicator = block.locator('[data-testid="code-language"]');
      await expect(languageIndicator).toBeVisible();

      // Each should have some highlighted content
      const codeContent = block.locator('[data-testid="code-content"]');
      await expect(codeContent).toBeVisible();
    }
  });

  test('Code block maintains styling when switching themes', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');
    await page.locator('#quick-start').scrollIntoViewIfNeeded();

    const codeBlock = page.locator('[data-testid="code-block"]').first();
    const highlightedElement = codeBlock.locator('.text-green-400').first();

    // Get color in current theme
    const initialColor = await highlightedElement.evaluate((el) =>
      getComputedStyle(el).color
    );

    // Toggle theme
    const themeToggle = page.getByTestId('theme-toggle');
    await themeToggle.click();
    await page.waitForTimeout(300);

    // Get color after theme toggle
    const newColor = await highlightedElement.evaluate((el) =>
      getComputedStyle(el).color
    );

    // Syntax highlighting color should remain consistent (tailwind classes don't change)
    expect(newColor).toBe(initialColor);
  });
});
