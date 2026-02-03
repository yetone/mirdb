/**
 * E2E tests for Usage Examples section.
 * Owner: Scenario 3 - Usage Examples Section
 *
 * Tests:
 * - Copy button functionality with visual feedback
 * - Tab switching in browser
 * - Terminal code block rendering
 */

import { test, expect } from '@playwright/test';

test.describe('Usage Examples Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/mirdb/');
    // Wait for the examples section to be visible
    await page.waitForSelector('#examples');
  });

  test.describe('Section Structure', () => {
    test('should display the Usage Examples heading', async ({ page }) => {
      const heading = page.locator('#examples-heading');
      await expect(heading).toBeVisible();
      await expect(heading).toContainText('Usage Examples');
    });

    test('should have navigation to examples section', async ({ page }) => {
      const section = page.locator('#examples');
      await expect(section).toBeVisible();
    });
  });

  test.describe('Tab Interface', () => {
    test('should display three tabs: Basic, Advanced, Benchmark', async ({ page }) => {
      const basicTab = page.locator('[data-tab="basic"]');
      const advancedTab = page.locator('[data-tab="advanced"]');
      const benchmarkTab = page.locator('[data-tab="benchmark"]');

      await expect(basicTab).toBeVisible();
      await expect(advancedTab).toBeVisible();
      await expect(benchmarkTab).toBeVisible();

      await expect(basicTab).toContainText('Basic');
      await expect(advancedTab).toContainText('Advanced');
      await expect(benchmarkTab).toContainText('Benchmark');
    });

    test('should have Basic tab selected by default', async ({ page }) => {
      const basicTab = page.locator('[data-tab="basic"]');
      await expect(basicTab).toHaveAttribute('aria-selected', 'true');

      const basicPanel = page.locator('[data-panel="basic"]');
      await expect(basicPanel).toBeVisible();
    });

    test('should switch to Advanced tab on click', async ({ page }) => {
      const advancedTab = page.locator('[data-tab="advanced"]');
      await advancedTab.click();

      await expect(advancedTab).toHaveAttribute('aria-selected', 'true');

      const advancedPanel = page.locator('[data-panel="advanced"]');
      await expect(advancedPanel).toBeVisible();

      const basicPanel = page.locator('[data-panel="basic"]');
      await expect(basicPanel).not.toBeVisible();
    });

    test('should switch to Benchmark tab on click', async ({ page }) => {
      const benchmarkTab = page.locator('[data-tab="benchmark"]');
      await benchmarkTab.click();

      await expect(benchmarkTab).toHaveAttribute('aria-selected', 'true');

      const benchmarkPanel = page.locator('[data-panel="benchmark"]');
      await expect(benchmarkPanel).toBeVisible();
    });

    test('tab switching should not reload page', async ({ page }) => {
      const url = page.url();

      const advancedTab = page.locator('[data-tab="advanced"]');
      await advancedTab.click();

      // URL should remain the same (no page reload)
      expect(page.url()).toBe(url);
    });
  });

  test.describe('Code Blocks', () => {
    test('should display terminal-style code blocks', async ({ page }) => {
      const terminalWindow = page.locator('.terminal-window').first();
      await expect(terminalWindow).toBeVisible();
    });

    test('should have terminal header with window controls', async ({ page }) => {
      // Scope to the examples section to avoid conflicts with other sections
      const examplesSection = page.locator('#examples');
      const terminalHeader = examplesSection.locator('.terminal-header').first();
      await expect(terminalHeader).toBeVisible();

      const terminalWindows = examplesSection.locator('.terminal-window');
      const dots = examplesSection.locator('.terminal-dot');
      await expect(dots).toHaveCount(await terminalWindows.count() * 3);
    });

    test('should display SET command examples', async ({ page }) => {
      const codeBlocks = page.locator('.terminal-body pre');
      const firstCodeBlock = codeBlocks.first();
      const content = await firstCodeBlock.textContent();
      expect(content?.toLowerCase()).toContain('set');
    });

    test('should display GET command examples', async ({ page }) => {
      const codeBlocks = page.locator('.terminal-body pre');
      const hasGet = await codeBlocks.evaluateAll((blocks) =>
        blocks.some(block => block.textContent?.toLowerCase().includes('get '))
      );
      expect(hasGet).toBe(true);
    });

    test('should have syntax highlighting with color classes', async ({ page }) => {
      const commandSpans = page.locator('.token-command');
      await expect(commandSpans.first()).toBeVisible();

      const responseSpans = page.locator('.token-response');
      await expect(responseSpans.first()).toBeVisible();
    });
  });

  test.describe('Copy Button Functionality', () => {
    test('should have copy button on code blocks', async ({ page }) => {
      const copyButton = page.locator('.copy-button').first();
      await expect(copyButton).toBeVisible();
    });

    test('should have accessible copy button label', async ({ page }) => {
      const copyButton = page.locator('.copy-button').first();
      await expect(copyButton).toHaveAttribute('aria-label', 'Copy code to clipboard');
    });

    test('should show check icon after clicking copy', async ({ page }) => {
      // Grant clipboard permissions
      await page.context().grantPermissions(['clipboard-read', 'clipboard-write']);

      const copyButton = page.locator('.copy-button').first();
      const copyIcon = copyButton.locator('.copy-icon');
      const checkIcon = copyButton.locator('.check-icon');

      // Initially copy icon visible, check icon hidden
      await expect(copyIcon).toBeVisible();
      await expect(checkIcon).not.toBeVisible();

      // Click copy button
      await copyButton.click();

      // Check icon should become visible (visual feedback)
      await expect(checkIcon).toBeVisible();
      await expect(copyIcon).not.toBeVisible();

      // After 2 seconds, should reset back
      await page.waitForTimeout(2500);
      await expect(copyIcon).toBeVisible();
      await expect(checkIcon).not.toBeVisible();
    });

    test('should copy code to clipboard', async ({ page }) => {
      // Grant clipboard permissions
      await page.context().grantPermissions(['clipboard-read', 'clipboard-write']);

      const copyButton = page.locator('.copy-button').first();
      await copyButton.click();

      // Read clipboard and verify content was copied
      const clipboardContent = await page.evaluate(() => navigator.clipboard.readText());
      expect(clipboardContent).toBeTruthy();
      expect(clipboardContent.length).toBeGreaterThan(0);
    });

    test('should update aria-label after copy', async ({ page }) => {
      await page.context().grantPermissions(['clipboard-read', 'clipboard-write']);

      const copyButton = page.locator('.copy-button').first();
      await copyButton.click();

      // Aria-label should update to "Copied!"
      await expect(copyButton).toHaveAttribute('aria-label', 'Copied!');
    });
  });

  test.describe('Keyboard Navigation', () => {
    test('should navigate tabs with arrow keys', async ({ page }) => {
      const basicTab = page.locator('[data-tab="basic"]');
      await basicTab.focus();

      // Press ArrowRight to move to Advanced
      await page.keyboard.press('ArrowRight');

      const advancedTab = page.locator('[data-tab="advanced"]');
      await expect(advancedTab).toBeFocused();
      await expect(advancedTab).toHaveAttribute('aria-selected', 'true');
    });

    test('should wrap around from last to first tab', async ({ page }) => {
      const benchmarkTab = page.locator('[data-tab="benchmark"]');
      await benchmarkTab.click();
      await benchmarkTab.focus();

      // Press ArrowRight to wrap to Basic
      await page.keyboard.press('ArrowRight');

      const basicTab = page.locator('[data-tab="basic"]');
      await expect(basicTab).toBeFocused();
    });

    test('should navigate with Home and End keys', async ({ page }) => {
      const advancedTab = page.locator('[data-tab="advanced"]');
      await advancedTab.click();
      await advancedTab.focus();

      // Press Home to go to first tab
      await page.keyboard.press('Home');

      const basicTab = page.locator('[data-tab="basic"]');
      await expect(basicTab).toBeFocused();

      // Press End to go to last tab
      await page.keyboard.press('End');

      const benchmarkTab = page.locator('[data-tab="benchmark"]');
      await expect(benchmarkTab).toBeFocused();
    });
  });

  test.describe('Accessibility', () => {
    test('should have proper ARIA roles', async ({ page }) => {
      // Scope to the examples section to avoid conflicts with Installation section
      const examplesSection = page.locator('#examples');
      const tablist = examplesSection.locator('[role="tablist"]');
      await expect(tablist).toBeVisible();

      const tabs = examplesSection.locator('[role="tab"]');
      await expect(tabs).toHaveCount(3);

      const tabpanels = examplesSection.locator('[role="tabpanel"]');
      await expect(tabpanels).toHaveCount(3);
    });

    test('should have aria-controls linking tabs to panels', async ({ page }) => {
      const basicTab = page.locator('[data-tab="basic"]');
      const controlsId = await basicTab.getAttribute('aria-controls');
      expect(controlsId).toBe('panel-basic');

      const panel = page.locator(`#${controlsId}`);
      await expect(panel).toBeVisible();
    });

    test('code blocks should be focusable for screen readers', async ({ page }) => {
      const codeBlock = page.locator('.terminal-body pre').first();
      await expect(codeBlock).toHaveAttribute('tabindex', '0');
    });
  });
});
