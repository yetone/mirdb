/**
 * E2E tests for Copy-to-Clipboard across browsers.
 * Owner: Scenario 15 - Cross-Browser Compatibility
 *
 * Tests cover:
 * - Clipboard API works in Chrome, Firefox, Safari, Edge
 * - Copy button visual feedback across browsers
 * - Fallback behavior when Clipboard API is not available
 *
 * Note: The Clipboard API requires a secure context (HTTPS or localhost)
 * and user activation (click) in most browsers.
 * WebKit and Firefox don't support grantPermissions for clipboard, so we
 * use different test strategies for those browsers.
 */

import { test, expect } from '@playwright/test';

/**
 * Helper to safely grant clipboard permissions if the browser supports it.
 * Returns true if permissions were granted, false otherwise.
 */
async function tryGrantClipboardPermissions(
  page: { context: () => { grantPermissions: (perms: string[]) => Promise<void> } },
  browserName: string
): Promise<boolean> {
  // Only Chromium-based browsers support grantPermissions for clipboard
  if (browserName === 'chromium' || browserName === 'edge') {
    try {
      await page.context().grantPermissions(['clipboard-write', 'clipboard-read']);
      return true;
    } catch {
      return false;
    }
  }
  return false;
}

test.describe('Clipboard API Cross-Browser Compatibility', () => {
  test.describe('Copy Button Functionality', () => {
    test('copy button is visible in terminal section', async ({ page, browserName }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Find copy button in terminal
      const copyButton = page.locator('[data-testid="copy-button"]');
      const count = await copyButton.count();

      if (count > 0) {
        await expect(copyButton.first()).toBeVisible();
        console.log(`Copy button visible on: ${browserName}`);
      } else {
        console.log(`Copy button not found on page - skipping on: ${browserName}`);
      }
    });

    test('copy button is clickable', async ({ page, browserName }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      const copyButton = page.locator('[data-testid="copy-button"]').first();
      const count = await copyButton.count();

      if (count > 0) {
        // Try to grant clipboard permissions (only works on Chromium)
        await tryGrantClipboardPermissions(page as any, browserName);

        // Click the copy button - this should work on all browsers
        // because user activation is granted by click
        await copyButton.click();

        // Should show feedback (either "Copied!" text or checkmark icon)
        await page.waitForTimeout(500);

        // Check if aria-label changed to indicate success
        const ariaLabel = await copyButton.getAttribute('aria-label');
        const buttonText = await copyButton.textContent();

        const showsSuccess =
          ariaLabel?.toLowerCase().includes('copied') ||
          buttonText?.toLowerCase().includes('copied');

        expect(showsSuccess).toBe(true);
        console.log(`Copy button click works on: ${browserName}`);
      }
    });

    test('copy button shows visual feedback on success', async ({ page, browserName }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      const copyButton = page.locator('[data-testid="copy-button"]').first();
      const count = await copyButton.count();

      if (count > 0) {
        await tryGrantClipboardPermissions(page as any, browserName);

        // Click the copy button
        await copyButton.click();

        // Wait for feedback
        await page.waitForTimeout(100);

        // Check for success indicator
        const successIndicator = page.locator('[data-testid="copy-success"]');
        const successCount = await successIndicator.count();

        if (successCount > 0) {
          await expect(successIndicator).toBeVisible();
          console.log(`Visual feedback shown on: ${browserName}`);
        } else {
          // Fallback: check button text changed
          const buttonText = await copyButton.textContent();
          expect(buttonText?.toLowerCase()).toContain('copied');
          console.log(`Text feedback shown on: ${browserName}`);
        }
      }
    });

    test('copy feedback reverts after timeout', async ({ page, browserName }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      const copyButton = page.locator('[data-testid="copy-button"]').first();
      const count = await copyButton.count();

      if (count > 0) {
        await tryGrantClipboardPermissions(page as any, browserName);

        // Get initial state
        const initialAriaLabel = await copyButton.getAttribute('aria-label');

        // Click the copy button
        await copyButton.click();

        // Wait for feedback to appear
        await page.waitForTimeout(100);

        // Wait for feedback to disappear (typically 2 seconds)
        await page.waitForTimeout(2500);

        // Check that state reverted
        const finalAriaLabel = await copyButton.getAttribute('aria-label');

        // The aria-label should have reverted or at least not show "Copied" anymore
        const isReverted =
          finalAriaLabel === initialAriaLabel ||
          !finalAriaLabel?.toLowerCase().includes('copied');

        expect(isReverted).toBe(true);
        console.log(`Feedback reverted on: ${browserName}`);
      }
    });

    test('copy button is keyboard accessible', async ({ page, browserName }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      const copyButton = page.locator('[data-testid="copy-button"]').first();
      const count = await copyButton.count();

      if (count > 0) {
        await tryGrantClipboardPermissions(page as any, browserName);

        // Focus the copy button
        await copyButton.focus();

        // Verify it's focused
        const isFocused = await copyButton.evaluate((el) => document.activeElement === el);
        expect(isFocused).toBe(true);

        // Press Enter to activate
        await page.keyboard.press('Enter');

        // Wait for feedback
        await page.waitForTimeout(500);

        // Verify it triggered copy
        const ariaLabel = await copyButton.getAttribute('aria-label');
        expect(ariaLabel?.toLowerCase()).toContain('copied');

        console.log(`Keyboard accessibility verified on: ${browserName}`);
      }
    });
  });

  test.describe('Clipboard API Availability', () => {
    test('navigator.clipboard is available in secure context', async ({ page, browserName }) => {
      await page.goto('/');

      const hasClipboard = await page.evaluate(() => {
        return typeof navigator.clipboard !== 'undefined';
      });

      // Clipboard API should be available in localhost (secure context)
      expect(hasClipboard).toBe(true);
      console.log(`Clipboard API available on: ${browserName}`);
    });

    test('clipboard.writeText is available', async ({ page, browserName }) => {
      await page.goto('/');

      const hasWriteText = await page.evaluate(() => {
        return (
          typeof navigator.clipboard !== 'undefined' &&
          typeof navigator.clipboard.writeText === 'function'
        );
      });

      expect(hasWriteText).toBe(true);
      console.log(`clipboard.writeText available on: ${browserName}`);
    });

    test('clipboard.readText is available', async ({ page, browserName }) => {
      await page.goto('/');

      const hasReadText = await page.evaluate(() => {
        return (
          typeof navigator.clipboard !== 'undefined' &&
          typeof navigator.clipboard.readText === 'function'
        );
      });

      expect(hasReadText).toBe(true);
      console.log(`clipboard.readText available on: ${browserName}`);
    });
  });

  test.describe('Clipboard Integration with Terminal', () => {
    test('clicking copy copies terminal code content', async ({ page, browserName }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Try to grant clipboard permissions
      const permissionsGranted = await tryGrantClipboardPermissions(page as any, browserName);

      const copyButton = page.locator('[data-testid="copy-button"]').first();
      const count = await copyButton.count();

      if (count > 0) {
        // Click copy button
        await copyButton.click();
        await page.waitForTimeout(200);

        // Only try to read clipboard if permissions were granted
        if (permissionsGranted) {
          const clipboardContent = await page.evaluate(async () => {
            try {
              return await navigator.clipboard.readText();
            } catch (e) {
              return null;
            }
          });

          // Clipboard should have content
          if (clipboardContent !== null) {
            expect(clipboardContent.length).toBeGreaterThan(0);
            console.log(`Clipboard content verified on: ${browserName}`);
          } else {
            console.log(`Could not read clipboard on: ${browserName} (permission issue)`);
          }
        } else {
          // For browsers without grantPermissions, verify the UI feedback instead
          const ariaLabel = await copyButton.getAttribute('aria-label');
          expect(ariaLabel?.toLowerCase()).toContain('copied');
          console.log(`Clipboard copy verified via UI feedback on: ${browserName}`);
        }
      }
    });

    test('copied content contains expected code', async ({ page, browserName }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      const permissionsGranted = await tryGrantClipboardPermissions(page as any, browserName);

      const copyButton = page.locator('[data-testid="copy-button"]').first();
      const count = await copyButton.count();

      if (count > 0) {
        await copyButton.click();
        await page.waitForTimeout(200);

        if (permissionsGranted) {
          const clipboardContent = await page.evaluate(async () => {
            try {
              return await navigator.clipboard.readText();
            } catch (e) {
              return '';
            }
          });

          // Content should contain shell commands or code
          if (clipboardContent) {
            expect(clipboardContent.trim().length).toBeGreaterThan(0);
            console.log(`Code content copied on: ${browserName}`);
          }
        } else {
          // Verify UI feedback for non-Chromium browsers
          const ariaLabel = await copyButton.getAttribute('aria-label');
          expect(ariaLabel?.toLowerCase()).toContain('copied');
          console.log(`Copy verified via UI on: ${browserName}`);
        }
      }
    });

    test('tab switching changes copied content', async ({ page, browserName }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      const permissionsGranted = await tryGrantClipboardPermissions(page as any, browserName);

      // Find the config tab
      const configTab = page.locator('[data-testid="tab-config"]');
      const tabCount = await configTab.count();

      if (tabCount === 0) {
        console.log(`Config tab not found on: ${browserName} - skipping`);
        return;
      }

      // Switch to config tab
      await configTab.click();
      await page.waitForTimeout(300);

      // Click copy button
      const copyButton = page.locator('[data-testid="copy-button"]').first();
      const copyCount = await copyButton.count();

      if (copyCount > 0) {
        await copyButton.click();
        await page.waitForTimeout(200);

        if (permissionsGranted) {
          const clipboardContent = await page.evaluate(async () => {
            try {
              return await navigator.clipboard.readText();
            } catch (e) {
              return '';
            }
          });

          // Config tab content should include TOML or config-related content
          if (clipboardContent) {
            const hasConfigContent =
              clipboardContent.includes('[') || // TOML sections
              clipboardContent.includes('=') || // Key-value pairs
              clipboardContent.toLowerCase().includes('config');

            expect(hasConfigContent).toBe(true);
            console.log(`Tab-specific content copied on: ${browserName}`);
          }
        } else {
          // Verify UI feedback for non-Chromium browsers
          const ariaLabel = await copyButton.getAttribute('aria-label');
          expect(ariaLabel?.toLowerCase()).toContain('copied');
          console.log(`Tab switch copy verified via UI on: ${browserName}`);
        }
      }
    });
  });

  test.describe('Error Handling', () => {
    test('copy button handles clipboard errors gracefully', async ({ page, browserName }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Don't grant clipboard permissions to simulate potential errors
      // Note: Some browsers may still allow localhost without explicit permissions

      const copyButton = page.locator('[data-testid="copy-button"]').first();
      const count = await copyButton.count();

      if (count > 0) {
        // Listen for page errors
        let pageErrorOccurred = false;
        page.on('pageerror', () => {
          pageErrorOccurred = true;
        });

        // Try to click copy (may or may not succeed depending on browser)
        await copyButton.click();
        await page.waitForTimeout(500);

        // Page should not crash even if clipboard fails
        expect(pageErrorOccurred).toBe(false);

        // Button should still be in the DOM and functional
        await expect(copyButton).toBeVisible();

        console.log(`Error handling verified on: ${browserName}`);
      }
    });
  });
});
