// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Copy to Clipboard Functionality (Scenario 16)', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
    });

    // Test Case 1: Click copy button on installation command - visual feedback shown
    test('TC1: Copy button shows visual feedback when clicked (tooltip or icon change)', async ({ page, context }) => {
        // Grant clipboard permissions
        await context.grantPermissions(['clipboard-read', 'clipboard-write']);

        // Navigate to quick start section
        const getStartedBtn = page.locator('#get-started-btn');
        await getStartedBtn.click();
        await page.waitForTimeout(500);

        // Locate the quick start section
        const quickStartSection = page.locator('#quick-start');
        await expect(quickStartSection).toBeVisible();

        // Find the copy button in the installation code block
        const installationBlock = quickStartSection.locator('.code-block').first();
        const copyBtn = installationBlock.locator('.copy-btn');
        await expect(copyBtn).toBeVisible();

        // Verify button shows "Copy" text initially
        await expect(copyBtn).toHaveText('Copy');

        // Click the copy button
        await copyBtn.click();

        // Verify visual feedback - button text changes to "Copied!"
        await expect(copyBtn).toHaveText('Copied!');

        // Verify the clipboard now contains the installation command
        const clipboardText = await page.evaluate(async () => {
            return await navigator.clipboard.readText();
        });
        expect(clipboardText).toBe('cargo install mirdb-server');

        // Wait for button to revert back to "Copy" (after 2 seconds)
        await page.waitForTimeout(2500);
        await expect(copyBtn).toHaveText('Copy');
    });

    // Test Case 2: Paste copied content - content matches original exactly
    test('TC2: Copied content matches original code exactly (no extra characters)', async ({ page, context }) => {
        // Grant clipboard permissions
        await context.grantPermissions(['clipboard-read', 'clipboard-write']);

        // Navigate to quick start section
        const getStartedBtn = page.locator('#get-started-btn');
        await getStartedBtn.click();
        await page.waitForTimeout(500);

        // Locate the quick start section
        const quickStartSection = page.locator('#quick-start');
        await expect(quickStartSection).toBeVisible();

        // Find the copy button in the installation code block
        const installationBlock = quickStartSection.locator('.code-block').first();
        const copyBtn = installationBlock.locator('.copy-btn');
        await expect(copyBtn).toBeVisible();

        // Get the original text from the code block
        const codeElement = installationBlock.locator('code');
        const originalText = await codeElement.textContent();

        // Also get the data-clipboard-text attribute to verify it matches
        const clipboardData = await copyBtn.getAttribute('data-clipboard-text');

        // Click the copy button
        await copyBtn.click();

        // Wait for copy operation to complete
        await expect(copyBtn).toHaveText('Copied!');

        // Verify clipboard content matches original exactly
        const clipboardText = await page.evaluate(async () => {
            return await navigator.clipboard.readText();
        });

        // Content should match the data-clipboard-text attribute exactly
        expect(clipboardText).toBe(clipboardData);

        // Verify no extra whitespace or characters
        expect(clipboardText).toBe('cargo install mirdb-server');
        expect(clipboardText.trim()).toBe(clipboardText); // No leading/trailing whitespace
        expect(clipboardText).not.toContain('\n'); // No newlines
        expect(clipboardText).not.toContain('\r'); // No carriage returns

        // Verify it matches the visible code content
        expect(originalText).toContain(clipboardText);
    });

    // Test Case 3: Copy button keyboard accessibility - Enter/Space activation
    test('TC3: Copy button can be activated with Enter/Space when focused', async ({ page, context }) => {
        // Grant clipboard permissions
        await context.grantPermissions(['clipboard-read', 'clipboard-write']);

        // Navigate to quick start section
        const getStartedBtn = page.locator('#get-started-btn');
        await getStartedBtn.click();
        await page.waitForTimeout(500);

        // Locate the quick start section
        const quickStartSection = page.locator('#quick-start');
        await expect(quickStartSection).toBeVisible();

        // Find the copy button in the installation code block
        const installationBlock = quickStartSection.locator('.code-block').first();
        const copyBtn = installationBlock.locator('.copy-btn');
        await expect(copyBtn).toBeVisible();

        // Clear clipboard first
        await page.evaluate(async () => {
            await navigator.clipboard.writeText('');
        });

        // Test 1: Verify the copy button is focusable
        await copyBtn.focus();

        // Verify the button has focus
        await expect(copyBtn).toBeFocused();

        // Test 2: Activate with Enter key
        await page.keyboard.press('Enter');

        // Verify visual feedback
        await expect(copyBtn).toHaveText('Copied!');

        // Verify clipboard content
        let clipboardText = await page.evaluate(async () => {
            return await navigator.clipboard.readText();
        });
        expect(clipboardText).toBe('cargo install mirdb-server');

        // Wait for button to reset
        await page.waitForTimeout(2500);
        await expect(copyBtn).toHaveText('Copy');

        // Clear clipboard for next test
        await page.evaluate(async () => {
            await navigator.clipboard.writeText('');
        });

        // Test 3: Activate with Space key
        await copyBtn.focus();
        await expect(copyBtn).toBeFocused();

        await page.keyboard.press('Space');

        // Verify visual feedback
        await expect(copyBtn).toHaveText('Copied!');

        // Verify clipboard content
        clipboardText = await page.evaluate(async () => {
            return await navigator.clipboard.readText();
        });
        expect(clipboardText).toBe('cargo install mirdb-server');
    });

    // Additional test: Verify copy button is focusable and maintains focus
    test('Copy button is focusable for keyboard navigation', async ({ page }) => {
        // Navigate to quick start section
        const getStartedBtn = page.locator('#get-started-btn');
        await getStartedBtn.click();
        await page.waitForTimeout(500);

        // Find the copy button
        const quickStartSection = page.locator('#quick-start');
        const installationBlock = quickStartSection.locator('.code-block').first();
        const copyBtn = installationBlock.locator('.copy-btn');

        // Verify button is visible
        await expect(copyBtn).toBeVisible();

        // Focus the button using Tab key navigation
        await page.keyboard.press('Tab');
        await page.keyboard.press('Tab');
        await page.keyboard.press('Tab');

        // Focus it directly as well to confirm focusability
        await copyBtn.focus();
        await expect(copyBtn).toBeFocused();

        // Verify button can receive and maintain focus
        const isFocused = await copyBtn.evaluate((btn) => document.activeElement === btn);
        expect(isFocused).toBe(true);
    });

    // Test: Verify copy button is a proper button element
    test('Copy button is an accessible button element', async ({ page }) => {
        // Navigate to quick start section
        const getStartedBtn = page.locator('#get-started-btn');
        await getStartedBtn.click();
        await page.waitForTimeout(500);

        // Find the copy button
        const quickStartSection = page.locator('#quick-start');
        const installationBlock = quickStartSection.locator('.code-block').first();
        const copyBtn = installationBlock.locator('.copy-btn');

        // Verify it's a button element (inherently accessible)
        const tagName = await copyBtn.evaluate((el) => el.tagName.toLowerCase());
        expect(tagName).toBe('button');

        // Verify button has accessible name (text content)
        const accessibleName = await copyBtn.textContent();
        expect(accessibleName).toBeTruthy();
        expect(accessibleName?.length).toBeGreaterThan(0);
    });
});
