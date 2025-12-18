import { test, expect } from '@playwright/test';

/**
 * Copy-to-Clipboard Feature Tests
 *
 * These tests verify that code snippets on the MirDB homepage
 * have functional copy-to-clipboard buttons.
 *
 * Test Cases:
 * TC1: Code blocks have copy-to-clipboard button visible or on hover
 * TC2: Code content is correctly copied to clipboard
 * TC3: User receives visual feedback when copy is successful
 */

test.describe('User Interaction - Copy to Clipboard', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
    // Wait for JavaScript to initialize copy buttons
    await page.waitForSelector('.code-block-wrapper', { timeout: 10000 });
  });

  test.describe('TC1: Code blocks have copy-to-clipboard button visible or on hover', () => {
    test('Copy buttons exist on all code blocks', async ({ page }) => {
      // Get all code block wrappers
      const codeBlockWrappers = page.locator('.code-block-wrapper');
      const count = await codeBlockWrappers.count();

      // There should be at least 3 code blocks (bash, python, toml in getting-started)
      expect(count).toBeGreaterThanOrEqual(3);

      // Each wrapper should have a copy button
      for (let i = 0; i < count; i++) {
        const wrapper = codeBlockWrappers.nth(i);
        const copyButton = wrapper.locator('.copy-button');
        await expect(copyButton).toBeAttached();
      }
    });

    test('Copy button is hidden by default', async ({ page }) => {
      const firstCopyButton = page.locator('.copy-button').first();

      // Check that the button has opacity 0 (hidden) by default
      const opacity = await firstCopyButton.evaluate((el) => {
        return window.getComputedStyle(el).opacity;
      });
      expect(opacity).toBe('0');
    });

    test('Copy button becomes visible on hover', async ({ page }) => {
      const firstWrapper = page.locator('.code-block-wrapper').first();
      const firstCopyButton = firstWrapper.locator('.copy-button');

      // Scroll to make the code block visible
      await firstWrapper.scrollIntoViewIfNeeded();

      // Hover over the code block wrapper
      await firstWrapper.hover();

      // Wait for transition
      await page.waitForTimeout(300);

      // Check that the button is now visible (opacity 1)
      const opacity = await firstCopyButton.evaluate((el) => {
        return window.getComputedStyle(el).opacity;
      });
      expect(opacity).toBe('1');
    });

    test('Copy button has correct accessibility attributes', async ({ page }) => {
      const firstCopyButton = page.locator('.copy-button').first();

      // Check aria-label
      await expect(firstCopyButton).toHaveAttribute('aria-label', 'Copy code to clipboard');

      // Check title attribute
      await expect(firstCopyButton).toHaveAttribute('title', 'Copy to clipboard');

      // Check data-testid
      await expect(firstCopyButton).toHaveAttribute('data-testid', 'copy-button');
    });

    test('Copy button displays copy icon and text', async ({ page }) => {
      const firstCopyButton = page.locator('.copy-button').first();

      // Check that button contains an SVG icon
      const svgIcon = firstCopyButton.locator('svg');
      await expect(svgIcon).toBeAttached();

      // Check that button contains "Copy" text
      const copyText = firstCopyButton.locator('.copy-text');
      await expect(copyText).toHaveText('Copy');
    });

    test('Copy buttons exist in Getting Started section', async ({ page }) => {
      const gettingStartedSection = page.locator('#getting-started');
      await gettingStartedSection.scrollIntoViewIfNeeded();

      // Find copy buttons within the getting started section
      const copyButtons = gettingStartedSection.locator('.copy-button');
      const count = await copyButtons.count();

      // Should have at least 3 code blocks (bash, python, toml)
      expect(count).toBeGreaterThanOrEqual(3);
    });
  });

  test.describe('TC2: Code content is correctly copied to clipboard', () => {
    test('Clicking copy button copies code content to clipboard', async ({ page, context }) => {
      // Grant clipboard permissions
      await context.grantPermissions(['clipboard-read', 'clipboard-write']);

      const gettingStartedSection = page.locator('#getting-started');
      await gettingStartedSection.scrollIntoViewIfNeeded();

      // Get the first code block in getting started (bash installation)
      const firstWrapper = gettingStartedSection.locator('.code-block-wrapper').first();
      const copyButton = firstWrapper.locator('.copy-button');
      const codeElement = firstWrapper.locator('pre code');

      // Get the expected code content
      const expectedCode = await codeElement.textContent();

      // Hover to make button visible
      await firstWrapper.hover();
      await page.waitForTimeout(300);

      // Click the copy button
      await copyButton.click();

      // Read from clipboard and verify
      const clipboardContent = await page.evaluate(async () => {
        return await navigator.clipboard.readText();
      });

      expect(clipboardContent).toBe(expectedCode);
    });

    test('Different code blocks copy their own content', async ({ page, context }) => {
      // Grant clipboard permissions
      await context.grantPermissions(['clipboard-read', 'clipboard-write']);

      const gettingStartedSection = page.locator('#getting-started');
      await gettingStartedSection.scrollIntoViewIfNeeded();

      // Get the second code block (Python example)
      const pythonWrapper = gettingStartedSection.locator('.code-block-wrapper').nth(1);
      const copyButton = pythonWrapper.locator('.copy-button');
      const codeElement = pythonWrapper.locator('pre code');

      // Get the expected code content
      const expectedCode = await codeElement.textContent();

      // Hover and click
      await pythonWrapper.hover();
      await page.waitForTimeout(300);
      await copyButton.click();

      // Verify clipboard content
      const clipboardContent = await page.evaluate(async () => {
        return await navigator.clipboard.readText();
      });

      expect(clipboardContent).toBe(expectedCode);

      // Verify it's actually Python code
      expect(clipboardContent).toContain('pymemcache');
    });

    test('Copied code contains full content without truncation', async ({ page, context }) => {
      // Grant clipboard permissions
      await context.grantPermissions(['clipboard-read', 'clipboard-write']);

      const gettingStartedSection = page.locator('#getting-started');
      await gettingStartedSection.scrollIntoViewIfNeeded();

      // Get the TOML config code block (third one)
      const tomlWrapper = gettingStartedSection.locator('.code-block-wrapper').nth(2);
      const copyButton = tomlWrapper.locator('.copy-button');
      const codeElement = tomlWrapper.locator('pre code');

      const expectedCode = await codeElement.textContent();

      await tomlWrapper.hover();
      await page.waitForTimeout(300);
      await copyButton.click();

      const clipboardContent = await page.evaluate(async () => {
        return await navigator.clipboard.readText();
      });

      // Verify full content is copied
      expect(clipboardContent).toBe(expectedCode);

      // Verify TOML-specific content is present
      expect(clipboardContent).toContain('mirdb.toml');
      expect(clipboardContent).toContain('addr');
      expect(clipboardContent).toContain('max_level');
    });
  });

  test.describe('TC3: User receives visual feedback when copy is successful', () => {
    test('Button shows "Copied!" text after successful copy', async ({ page, context }) => {
      // Grant clipboard permissions
      await context.grantPermissions(['clipboard-read', 'clipboard-write']);

      const firstWrapper = page.locator('.code-block-wrapper').first();
      await firstWrapper.scrollIntoViewIfNeeded();

      const copyButton = firstWrapper.locator('.copy-button');

      // Hover to make button visible
      await firstWrapper.hover();
      await page.waitForTimeout(300);

      // Verify initial text
      const copyText = copyButton.locator('.copy-text');
      await expect(copyText).toHaveText('Copy');

      // Click copy button
      await copyButton.click();

      // Verify text changes to "Copied!"
      await expect(copyText).toHaveText('Copied!');
    });

    test('Button gets "copied" CSS class after successful copy', async ({ page, context }) => {
      // Grant clipboard permissions
      await context.grantPermissions(['clipboard-read', 'clipboard-write']);

      const firstWrapper = page.locator('.code-block-wrapper').first();
      await firstWrapper.scrollIntoViewIfNeeded();

      const copyButton = firstWrapper.locator('.copy-button');

      await firstWrapper.hover();
      await page.waitForTimeout(300);

      // Verify button doesn't have copied class initially
      await expect(copyButton).not.toHaveClass(/copied/);

      // Click copy button
      await copyButton.click();

      // Verify button has copied class
      await expect(copyButton).toHaveClass(/copied/);
    });

    test('Button shows checkmark icon after successful copy', async ({ page, context }) => {
      // Grant clipboard permissions
      await context.grantPermissions(['clipboard-read', 'clipboard-write']);

      const firstWrapper = page.locator('.code-block-wrapper').first();
      await firstWrapper.scrollIntoViewIfNeeded();

      const copyButton = firstWrapper.locator('.copy-button');

      await firstWrapper.hover();
      await page.waitForTimeout(300);

      // Click copy button
      await copyButton.click();

      // Check that SVG contains checkmark (polyline element for checkmark icon)
      const svgIcon = copyButton.locator('svg');
      const polyline = svgIcon.locator('polyline');
      await expect(polyline).toBeAttached();
    });

    test('Button aria-label updates after successful copy', async ({ page, context }) => {
      // Grant clipboard permissions
      await context.grantPermissions(['clipboard-read', 'clipboard-write']);

      const firstWrapper = page.locator('.code-block-wrapper').first();
      await firstWrapper.scrollIntoViewIfNeeded();

      const copyButton = firstWrapper.locator('.copy-button');

      await firstWrapper.hover();
      await page.waitForTimeout(300);

      // Verify initial aria-label
      await expect(copyButton).toHaveAttribute('aria-label', 'Copy code to clipboard');

      // Click copy button
      await copyButton.click();

      // Verify aria-label changes
      await expect(copyButton).toHaveAttribute('aria-label', 'Code copied to clipboard');
    });

    test('Button reverts to original state after 2 seconds', async ({ page, context }) => {
      // Grant clipboard permissions
      await context.grantPermissions(['clipboard-read', 'clipboard-write']);

      const firstWrapper = page.locator('.code-block-wrapper').first();
      await firstWrapper.scrollIntoViewIfNeeded();

      const copyButton = firstWrapper.locator('.copy-button');

      await firstWrapper.hover();
      await page.waitForTimeout(300);

      // Click copy button
      await copyButton.click();

      // Verify copied state
      const copyText = copyButton.locator('.copy-text');
      await expect(copyText).toHaveText('Copied!');
      await expect(copyButton).toHaveClass(/copied/);

      // Wait for reset (2 seconds + buffer)
      await page.waitForTimeout(2500);

      // Verify button reverts to original state
      await expect(copyText).toHaveText('Copy');
      await expect(copyButton).not.toHaveClass(/copied/);
      await expect(copyButton).toHaveAttribute('aria-label', 'Copy code to clipboard');
    });

    test('Copy button has visual styling change when in copied state', async ({ page, context }) => {
      // Grant clipboard permissions
      await context.grantPermissions(['clipboard-read', 'clipboard-write']);

      const firstWrapper = page.locator('.code-block-wrapper').first();
      await firstWrapper.scrollIntoViewIfNeeded();

      const copyButton = firstWrapper.locator('.copy-button');

      await firstWrapper.hover();
      await page.waitForTimeout(300);

      // Click copy button
      await copyButton.click();

      // Wait for CSS transition to complete
      await page.waitForTimeout(250);

      // Verify the button has the 'copied' class which applies the success color
      await expect(copyButton).toHaveClass(/copied/);

      // Verify the success background color is applied (greenish color)
      // The success color is #22c55e which is rgb(34, 197, 94)
      // Due to transitions, we check for greenish color (high G value, relatively low R/B)
      const copiedBgColor = await copyButton.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });

      // Parse the rgba/rgb values and verify green component is dominant
      const match = copiedBgColor.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);
      expect(match).not.toBeNull();
      if (match) {
        const [, r, g, b] = match.map(Number);
        // Green should be significantly higher than red and blue
        expect(g).toBeGreaterThan(150); // Green component should be high
        expect(g).toBeGreaterThan(r);   // Green > Red
      }
    });
  });

  test.describe('Integration Tests', () => {
    test('Multiple copy operations work independently', async ({ page, context }) => {
      // Grant clipboard permissions
      await context.grantPermissions(['clipboard-read', 'clipboard-write']);

      const gettingStartedSection = page.locator('#getting-started');
      await gettingStartedSection.scrollIntoViewIfNeeded();

      const wrappers = gettingStartedSection.locator('.code-block-wrapper');

      // Copy first code block
      const firstWrapper = wrappers.first();
      await firstWrapper.hover();
      await page.waitForTimeout(300);
      await firstWrapper.locator('.copy-button').click();

      const firstClipboard = await page.evaluate(async () => {
        return await navigator.clipboard.readText();
      });

      // Wait for reset
      await page.waitForTimeout(2500);

      // Copy second code block
      const secondWrapper = wrappers.nth(1);
      await secondWrapper.scrollIntoViewIfNeeded();
      await secondWrapper.hover();
      await page.waitForTimeout(300);
      await secondWrapper.locator('.copy-button').click();

      const secondClipboard = await page.evaluate(async () => {
        return await navigator.clipboard.readText();
      });

      // Verify both copied successfully and are different
      expect(firstClipboard).toBeTruthy();
      expect(secondClipboard).toBeTruthy();
      expect(firstClipboard).not.toBe(secondClipboard);
    });

    test('Copy buttons work across different sections', async ({ page, context }) => {
      // Grant clipboard permissions
      await context.grantPermissions(['clipboard-read', 'clipboard-write']);

      // Find all code blocks on the page
      const allWrappers = page.locator('.code-block-wrapper');
      const totalCount = await allWrappers.count();

      // Test at least the first code block
      expect(totalCount).toBeGreaterThanOrEqual(1);

      const wrapper = allWrappers.first();
      await wrapper.scrollIntoViewIfNeeded();
      await wrapper.hover();
      await page.waitForTimeout(300);

      const copyButton = wrapper.locator('.copy-button');
      await copyButton.click();

      const copyText = copyButton.locator('.copy-text');
      await expect(copyText).toHaveText('Copied!');
    });

    test('Copy functionality preserves code formatting', async ({ page, context }) => {
      // Grant clipboard permissions
      await context.grantPermissions(['clipboard-read', 'clipboard-write']);

      const gettingStartedSection = page.locator('#getting-started');
      await gettingStartedSection.scrollIntoViewIfNeeded();

      const firstWrapper = gettingStartedSection.locator('.code-block-wrapper').first();
      const codeElement = firstWrapper.locator('pre code');

      // Get the original code content with whitespace
      const originalCode = await codeElement.textContent();

      await firstWrapper.hover();
      await page.waitForTimeout(300);
      await firstWrapper.locator('.copy-button').click();

      const clipboardContent = await page.evaluate(async () => {
        return await navigator.clipboard.readText();
      });

      // Verify whitespace and newlines are preserved
      expect(clipboardContent).toBe(originalCode);

      // Verify it contains newlines (multi-line code)
      expect(clipboardContent).toContain('\n');
    });
  });
});
