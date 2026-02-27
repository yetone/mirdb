// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E tests for Quick Start Code Examples
 * Owner: Scenario 3 - Quick Start Code Examples
 */

test.describe('Quick Start Section', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
    });

    test('Test Case 1: Quick Start section contains code blocks with language classes', async ({ page }) => {
        // Navigate to Quick Start section
        const quickStartSection = page.locator('#quick-start');
        await expect(quickStartSection).toBeVisible();

        // Query for code blocks
        const codeBlocks = quickStartSection.locator('.code-block');
        const count = await codeBlocks.count();
        expect(count).toBeGreaterThanOrEqual(1);

        // Check for language-python or language-go class in code elements
        const pythonCode = quickStartSection.locator('code.language-python');
        const goCode = quickStartSection.locator('code.language-go');

        const hasPython = await pythonCode.count() > 0;
        const hasGo = await goCode.count() > 0;

        expect(hasPython || hasGo).toBeTruthy();
    });

    test('Test Case 2: Python code example contains required content', async ({ page }) => {
        const pythonCode = page.locator('#quick-start code.language-python');
        await expect(pythonCode).toBeVisible();

        const codeText = await pythonCode.textContent();

        // Check for required pymemcache keywords
        expect(codeText).toContain('pymemcache');
        expect(codeText).toContain('localhost');
        expect(codeText).toContain('12333');
        expect(codeText).toContain('client.set');
        expect(codeText).toContain('client.get');
    });

    test('Test Case 3: Go code example contains required content', async ({ page }) => {
        const goCode = page.locator('#quick-start code.language-go');
        await expect(goCode).toBeVisible();

        const codeText = await goCode.textContent();

        // Check for required gomemcache keywords
        expect(codeText).toContain('memcache.New');
        expect(codeText).toContain('localhost:12333');
        expect(codeText).toContain('mc.Set');
    });

    test('Test Case 4: Code blocks have Prism.js syntax highlighting', async ({ page }) => {
        // Wait for Prism to highlight the code
        await page.waitForFunction(() => {
            const tokens = document.querySelectorAll('#quick-start .token');
            return tokens.length > 0;
        }, { timeout: 5000 });

        // Check for Prism.js token classes
        const tokenClasses = ['.token.keyword', '.token.string', '.token.function', '.token.comment'];
        let foundTokens = 0;

        for (const tokenClass of tokenClasses) {
            const tokens = page.locator(`#quick-start ${tokenClass}`);
            const count = await tokens.count();
            if (count > 0) foundTokens++;
        }

        // At least some token classes should be present
        expect(foundTokens).toBeGreaterThan(0);
    });

    test('Test Case 8: Each code block has a copy button', async ({ page }) => {
        const codeBlocks = page.locator('#quick-start .code-block');
        const count = await codeBlocks.count();

        expect(count).toBeGreaterThanOrEqual(1);

        // Verify each code block has a copy button
        for (let i = 0; i < count; i++) {
            const codeBlock = codeBlocks.nth(i);
            const copyButton = codeBlock.locator('.copy-btn');
            await expect(copyButton).toBeVisible();
            await expect(copyButton).toHaveAttribute('type', 'button');
        }
    });
});

test.describe('Copy to Clipboard Functionality', () => {
    test.beforeEach(async ({ page, context }) => {
        // Grant clipboard permissions
        await context.grantPermissions(['clipboard-read', 'clipboard-write']);
        await page.goto('/');
    });

    test('Test Case 6: Clicking copy button copies code to clipboard', async ({ page }) => {
        // Find the first copy button and its associated code
        const firstCodeBlock = page.locator('#quick-start .code-block').first();
        const copyButton = firstCodeBlock.locator('.copy-btn');
        const codeElement = firstCodeBlock.locator('code');

        // Get the expected code content
        const expectedCode = await codeElement.textContent();

        // Click the copy button
        await copyButton.click();

        // Read from clipboard and verify
        const clipboardContent = await page.evaluate(() => navigator.clipboard.readText());
        expect(clipboardContent).toBe(expectedCode);
    });

    test('Test Case 7: Toast notification appears after copying', async ({ page }) => {
        // Click the copy button
        const copyButton = page.locator('#quick-start .copy-btn').first();
        await copyButton.click();

        // Verify toast notification appears
        const toast = page.locator('.toast');
        await expect(toast).toBeVisible();
        await expect(toast).toHaveText('Copied!');

        // Verify toast has success class
        await expect(toast).toHaveClass(/toast-success/);

        // Verify toast disappears after timeout
        await expect(toast).toBeHidden({ timeout: 5000 });
    });

    test('Copy button text changes to "Copied!" after clicking', async ({ page }) => {
        const copyButton = page.locator('#quick-start .copy-btn').first();
        const buttonText = copyButton.locator('span');

        // Verify initial text
        await expect(buttonText).toHaveText('Copy');

        // Click and verify text changes
        await copyButton.click();
        await expect(buttonText).toHaveText('Copied!');

        // Verify text reverts after timeout
        await expect(buttonText).toHaveText('Copy', { timeout: 5000 });
    });
});

test.describe('Accessibility', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
    });

    test('Copy buttons have accessible labels', async ({ page }) => {
        const copyButtons = page.locator('#quick-start .copy-btn');
        const count = await copyButtons.count();

        for (let i = 0; i < count; i++) {
            const button = copyButtons.nth(i);
            const ariaLabel = await button.getAttribute('aria-label');
            expect(ariaLabel).toBe('Copy code to clipboard');
        }
    });

    test('Toast container has aria-live attribute', async ({ page }) => {
        const toastContainer = page.locator('#toast-container');
        await expect(toastContainer).toHaveAttribute('aria-live', 'polite');
        await expect(toastContainer).toHaveAttribute('aria-atomic', 'true');
    });
});
