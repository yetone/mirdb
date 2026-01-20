/**
 * Supported Commands Section E2E Tests
 * Playwright tests for verifying command examples are displayed in code block format (REQ-7)
 */

const { test, expect } = require('@playwright/test');
const path = require('path');

test.describe('Supported Commands Section - Code Block Format', () => {
    test.beforeEach(async ({ page }) => {
        const filePath = path.resolve(__dirname, '../index.html');
        await page.goto(`file://${filePath}`);
    });

    test('TC4: Command examples are displayed in code block format', async ({ page }) => {
        // Set desktop viewport
        await page.setViewportSize({ width: 1280, height: 720 });

        // Get the commands section
        const commandsSection = page.locator('.commands');
        await expect(commandsSection).toBeVisible();

        // Verify the section has a heading
        const heading = commandsSection.locator('h2');
        await expect(heading).toBeVisible();
        await expect(heading).toContainText(/command/i);

        // Get all command items
        const commandItems = page.locator('.commands .command-item');
        const count = await commandItems.count();
        expect(count).toBeGreaterThan(0);

        // Verify each command item contains a code element (code block format)
        for (let i = 0; i < count; i++) {
            const item = commandItems.nth(i);
            const codeBlock = item.locator('code');
            await expect(codeBlock).toBeVisible();

            // Verify the code block has appropriate styling (monospace font, distinct background)
            const codeStyles = await codeBlock.evaluate((el) => {
                const computedStyle = window.getComputedStyle(el);
                return {
                    fontFamily: computedStyle.fontFamily,
                    backgroundColor: computedStyle.backgroundColor,
                    padding: computedStyle.padding,
                    borderRadius: computedStyle.borderRadius
                };
            });

            // Code blocks should have monospace font
            const hasMonospace = codeStyles.fontFamily.toLowerCase().includes('mono') ||
                                 codeStyles.fontFamily.toLowerCase().includes('consolas') ||
                                 codeStyles.fontFamily.toLowerCase().includes('courier');
            expect(hasMonospace).toBe(true);

            // Code blocks should have a distinct background color (not transparent or white)
            expect(codeStyles.backgroundColor).not.toBe('rgba(0, 0, 0, 0)');
        }
    });

    test('should display GET command in code block format', async ({ page }) => {
        await page.setViewportSize({ width: 1280, height: 720 });

        const commandsSection = page.locator('.commands');
        await expect(commandsSection).toBeVisible();

        // Find a code element containing GET
        const getCodeBlocks = commandsSection.locator('code');
        const codes = await getCodeBlocks.allTextContents();
        const hasGet = codes.some(text => text.toLowerCase().includes('get'));
        expect(hasGet).toBe(true);
    });

    test('should display SET command in code block format', async ({ page }) => {
        await page.setViewportSize({ width: 1280, height: 720 });

        const commandsSection = page.locator('.commands');
        await expect(commandsSection).toBeVisible();

        // Find a code element containing SET
        const setCodeBlocks = commandsSection.locator('code');
        const codes = await setCodeBlocks.allTextContents();
        const hasSet = codes.some(text => text.toLowerCase().includes('set'));
        expect(hasSet).toBe(true);
    });
});
