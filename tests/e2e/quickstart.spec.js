/**
 * Quick Start Section E2E Tests
 * Owner: Scenario 3 - Quick Start Section with Code Examples
 *
 * Tests for:
 * - Set/get command examples
 * - Syntax highlighting
 * - Copy button functionality
 * - Dark-themed code blocks
 */

const { test, expect } = require('@playwright/test');

test.describe('Quick Start Section', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
    });

    test('TC1: Quick Start section is visible with correct heading', async ({ page }) => {
        // Locate the Quick Start section
        const quickstartSection = page.locator('#quickstart');
        await expect(quickstartSection).toBeVisible();

        // Check for heading containing "Quick Start"
        const heading = quickstartSection.locator('h2');
        await expect(heading).toBeVisible();
        await expect(heading).toContainText('Quick Start');
    });

    test('TC2: Set command example is displayed', async ({ page }) => {
        const quickstartSection = page.locator('#quickstart');

        // Check for code block containing 'set' command
        const setCodeBlock = quickstartSection.locator('#code-set');
        await expect(setCodeBlock).toBeVisible();

        // Verify it contains the 'set' command
        const codeContent = await setCodeBlock.textContent();
        expect(codeContent).toContain('set');
        expect(codeContent).toContain('mykey');
    });

    test('TC3: Get command example is displayed', async ({ page }) => {
        const quickstartSection = page.locator('#quickstart');

        // Check for code block containing 'get' command
        const getCodeBlock = quickstartSection.locator('#code-get');
        await expect(getCodeBlock).toBeVisible();

        // Verify it contains the 'get' command
        const codeContent = await getCodeBlock.textContent();
        expect(codeContent).toContain('get');
        expect(codeContent).toContain('mykey');
    });

    test('TC4: Code blocks have syntax highlighting applied', async ({ page }) => {
        const quickstartSection = page.locator('#quickstart');

        // Check for syntax highlighting tokens (CSS classes)
        const tokenCommand = quickstartSection.locator('.token-command').first();
        await expect(tokenCommand).toBeVisible();

        // Verify command token has different styling (cyan color)
        const commandColor = await tokenCommand.evaluate(el => {
            return window.getComputedStyle(el).color;
        });
        // #22d3ee in rgb is approximately rgb(34, 211, 238)
        expect(commandColor).toMatch(/rgb\(34,\s*211,\s*238\)/);

        // Check for other token types
        const tokenKey = quickstartSection.locator('.token-key').first();
        await expect(tokenKey).toBeVisible();

        const tokenNumber = quickstartSection.locator('.token-number').first();
        await expect(tokenNumber).toBeVisible();

        const tokenValue = quickstartSection.locator('.token-value').first();
        await expect(tokenValue).toBeVisible();

        const tokenResponse = quickstartSection.locator('.token-response').first();
        await expect(tokenResponse).toBeVisible();
    });

    test('TC5: Copy buttons are present near code blocks', async ({ page }) => {
        const quickstartSection = page.locator('#quickstart');

        // Check for copy buttons
        const copyButtons = quickstartSection.locator('.copy-btn');

        // Should have 3 copy buttons (for start, set, and get examples)
        await expect(copyButtons).toHaveCount(3);

        // Each copy button should be visible
        for (let i = 0; i < 3; i++) {
            await expect(copyButtons.nth(i)).toBeVisible();
        }

        // Check for clipboard icon in the buttons
        const copyIcons = quickstartSection.locator('.copy-btn .copy-icon');
        await expect(copyIcons).toHaveCount(3);

        // Check for accessible labels
        const firstButton = copyButtons.first();
        await expect(firstButton).toHaveAttribute('aria-label', 'Copy code to clipboard');
    });

    test('TC6: Copy button copies code to clipboard', async ({ page, context }) => {
        // Grant clipboard permissions
        await context.grantPermissions(['clipboard-read', 'clipboard-write']);

        const quickstartSection = page.locator('#quickstart');

        // Click the copy button for the 'set' example
        const setCopyButton = quickstartSection.locator('.copy-btn[data-copy-target="code-set"]');
        await setCopyButton.click();

        // Verify the button shows "Copied!" feedback
        const copyText = setCopyButton.locator('.copy-text');
        await expect(copyText).toHaveText('Copied!');

        // Verify the button has the 'copied' class
        await expect(setCopyButton).toHaveClass(/copied/);

        // Wait for the feedback to reset
        await page.waitForTimeout(2500);
        await expect(copyText).toHaveText('Copy');
        await expect(setCopyButton).not.toHaveClass(/copied/);

        // Verify clipboard content (check that it contains expected text)
        const clipboardText = await page.evaluate(async () => {
            return await navigator.clipboard.readText();
        });
        expect(clipboardText).toContain('set');
        expect(clipboardText).toContain('mykey');
    });

    test('TC7: Code blocks have dark background styling', async ({ page }) => {
        const quickstartSection = page.locator('#quickstart');

        // Get the first code block
        const codeBlock = quickstartSection.locator('.code-block').first();
        await expect(codeBlock).toBeVisible();

        // Check background color (should be dark: #1e293b)
        const bgColor = await codeBlock.evaluate(el => {
            return window.getComputedStyle(el).backgroundColor;
        });

        // #1e293b in RGB is rgb(30, 41, 59)
        expect(bgColor).toMatch(/rgb\(30,\s*41,\s*59\)/);

        // Check text color (should be light: #e2e8f0)
        const textColor = await codeBlock.evaluate(el => {
            return window.getComputedStyle(el).color;
        });

        // #e2e8f0 in RGB is rgb(226, 232, 240)
        expect(textColor).toMatch(/rgb\(226,\s*232,\s*240\)/);
    });
});
