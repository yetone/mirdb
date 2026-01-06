// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for Quick Start Section with Copy-to-Clipboard
 * Scenario: Verify the quick-start section displays installation commands
 * with functional copy-to-clipboard functionality (REQ-3)
 */

test.describe('Quick Start Section', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
    });

    /**
     * Test Case 1: Quick-start section exists on the page
     */
    test('TC1: Quick-start section exists on the page', async ({ page }) => {
        // Navigate to quick-start section
        const quickStartSection = page.locator('[data-testid="quick-start-section"]');

        // Verify section exists
        await expect(quickStartSection).toBeVisible();

        // Verify section has expected heading
        const heading = quickStartSection.locator('h2');
        await expect(heading).toHaveText('Quick Start');

        // Verify section contains installation and usage examples
        const installSection = quickStartSection.locator('[data-testid="install-code-block"]');
        const usageSection = quickStartSection.locator('[data-testid="usage-code-block"]');
        await expect(installSection).toBeVisible();
        await expect(usageSection).toBeVisible();
    });

    /**
     * Test Case 2: Code snippet is displayed in a formatted code block
     */
    test('TC2: Code snippet is displayed in a formatted code block', async ({ page }) => {
        // Check for code block element
        const codeBlock = page.locator('[data-testid="install-code-block"]');
        await expect(codeBlock).toBeVisible();

        // Verify code block contains proper structure
        const preElement = codeBlock.locator('pre');
        const codeElement = codeBlock.locator('code');
        await expect(preElement).toBeVisible();
        await expect(codeElement).toBeVisible();

        // Verify code block has a label/header
        const codeLabel = codeBlock.locator('.code-label');
        await expect(codeLabel).toBeVisible();
        await expect(codeLabel).toHaveText('Terminal');

        // Verify installation command content
        const installCode = page.locator('[data-testid="install-code"]');
        await expect(installCode).toContainText('cargo install mirdb-server');
    });

    /**
     * Test Case 3: Copy-to-clipboard button is present near the code snippet
     */
    test('TC3: Copy-to-clipboard button is present near the code snippet', async ({ page }) => {
        // Check for copy button near installation code
        const installCopyBtn = page.locator('[data-testid="copy-install-btn"]');
        await expect(installCopyBtn).toBeVisible();

        // Verify button has expected text
        const copyText = installCopyBtn.locator('.copy-text');
        await expect(copyText).toHaveText('Copy');

        // Verify button has copy icon
        const copyIcon = installCopyBtn.locator('.copy-icon');
        await expect(copyIcon).toBeVisible();

        // Verify button is accessible (has aria-label)
        await expect(installCopyBtn).toHaveAttribute('aria-label', 'Copy installation command');

        // Check for copy button near usage code
        const usageCopyBtn = page.locator('[data-testid="copy-usage-btn"]');
        await expect(usageCopyBtn).toBeVisible();
    });

    /**
     * Test Case 4: Code content is copied to clipboard with visual feedback
     */
    test('TC4: Code content is copied to clipboard with visual feedback', async ({ page, context }) => {
        // Grant clipboard permissions
        await context.grantPermissions(['clipboard-read', 'clipboard-write']);

        // Click the copy button
        const copyBtn = page.locator('[data-testid="copy-install-btn"]');
        await copyBtn.click();

        // Verify visual feedback - button should show "Copied"
        const copyText = copyBtn.locator('.copy-text');
        await expect(copyText).toHaveText('Copied');

        // Verify button has "copied" class
        await expect(copyBtn).toHaveClass(/copied/);

        // Verify icon changed to checkmark
        const copyIcon = copyBtn.locator('.copy-icon');
        await expect(copyIcon).toHaveText('✓');

        // Verify clipboard content
        const clipboardContent = await page.evaluate(async () => {
            return await navigator.clipboard.readText();
        });
        expect(clipboardContent).toBe('cargo install mirdb-server');

        // Wait for feedback to reset
        await expect(copyText).toHaveText('Copy', { timeout: 3000 });
        await expect(copyIcon).toHaveText('📋', { timeout: 3000 });
    });

    /**
     * Test Case 5: Basic usage code example is provided
     */
    test('TC5: Basic usage code example is provided', async ({ page }) => {
        // Check for usage example code block
        const usageCodeBlock = page.locator('[data-testid="usage-code-block"]');
        await expect(usageCodeBlock).toBeVisible();

        // Verify usage code contains expected content
        const usageCode = page.locator('[data-testid="usage-code"]');
        await expect(usageCode).toBeVisible();

        // Check for specific usage commands
        await expect(usageCode).toContainText('mirdb-server');
        await expect(usageCode).toContainText('telnet');
        await expect(usageCode).toContainText('set mykey');
        await expect(usageCode).toContainText('get mykey');
        await expect(usageCode).toContainText('STORED');
    });

    /**
     * Test Case 6: Link to full documentation exists and is functional
     */
    test('TC6: Link to full documentation exists and is functional', async ({ page }) => {
        // Check for documentation link
        const docsLink = page.locator('[data-testid="docs-link"]');
        await expect(docsLink).toBeVisible();

        // Verify link text
        await expect(docsLink).toContainText('View Full Documentation');

        // Verify link has correct href
        await expect(docsLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb#readme');

        // Verify link opens in new tab (security best practice)
        await expect(docsLink).toHaveAttribute('target', '_blank');
        await expect(docsLink).toHaveAttribute('rel', /noopener/);
    });
});

test.describe('Copy Button Additional Scenarios', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
    });

    /**
     * Additional test: Multiple copy buttons work independently
     */
    test('Multiple copy buttons work independently', async ({ page, context }) => {
        await context.grantPermissions(['clipboard-read', 'clipboard-write']);

        // Click usage copy button
        const usageCopyBtn = page.locator('[data-testid="copy-usage-btn"]');
        await usageCopyBtn.click();

        // Verify usage button shows feedback
        await expect(usageCopyBtn.locator('.copy-text')).toHaveText('Copied');

        // Install button should still show "Copy"
        const installCopyBtn = page.locator('[data-testid="copy-install-btn"]');
        await expect(installCopyBtn.locator('.copy-text')).toHaveText('Copy');

        // Verify clipboard has usage content
        const clipboardContent = await page.evaluate(async () => {
            return await navigator.clipboard.readText();
        });
        expect(clipboardContent).toContain('mirdb-server --config');
    });

    /**
     * Additional test: Copy button is keyboard accessible
     */
    test('Copy button is keyboard accessible', async ({ page }) => {
        // Tab to the copy button
        const copyBtn = page.locator('[data-testid="copy-install-btn"]');

        // Focus on the button
        await copyBtn.focus();

        // Verify button is focused
        await expect(copyBtn).toBeFocused();

        // Verify focus is visible (focus-visible styles apply)
        const isVisible = await copyBtn.evaluate(el => {
            const style = window.getComputedStyle(el);
            return style.outlineWidth !== '0px' || style.outline !== 'none';
        });
        expect(isVisible).toBeTruthy();
    });
});
