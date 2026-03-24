/**
 * Quick Start Section E2E Tests
 * Owner: Scenario 4 - Quick Start Section
 *
 * Tests:
 * - Installation code blocks presence and syntax highlighting
 * - Usage examples presence
 * - Port 12333 reference
 * - Copy-to-clipboard functionality
 * - Mobile responsiveness
 */

import { test, expect } from '@playwright/test';

test.describe('Quick Start Section', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
    });

    test('installation code block is present with syntax highlighting', async ({ page }) => {
        // Navigate to quickstart section
        const quickstartSection = page.locator('#quickstart');
        await expect(quickstartSection).toBeVisible();

        // Check for installation code block
        const installationColumn = quickstartSection.locator('.quickstart-column').first();
        await expect(installationColumn).toBeVisible();

        // Check for code block with installation commands
        const codeBlock = installationColumn.locator('.code-block');
        await expect(codeBlock).toBeVisible();

        // Check for git clone command
        const codeContent = await codeBlock.locator('code').textContent();
        expect(codeContent).toContain('git clone');
        expect(codeContent).toContain('cargo run');

        // Check for syntax highlighting - token classes should exist
        const tokenClasses = await codeBlock.locator('code span[class^="token-"]').count();
        expect(tokenClasses).toBeGreaterThan(0);
    });

    test('usage example code block shows memcached protocol commands', async ({ page }) => {
        const quickstartSection = page.locator('#quickstart');

        // Get the usage column (second column)
        const usageColumn = quickstartSection.locator('.quickstart-column').nth(1);
        await expect(usageColumn).toBeVisible();

        // Check for code block
        const codeBlock = usageColumn.locator('.code-block');
        await expect(codeBlock).toBeVisible();

        // Check for SET and GET commands
        const codeContent = await codeBlock.locator('code').textContent();
        expect(codeContent).toContain('SET');
        expect(codeContent).toContain('GET');
        expect(codeContent).toContain('STORED');
    });

    test('port 12333 is referenced in connection examples', async ({ page }) => {
        const quickstartSection = page.locator('#quickstart');

        // Check text content for port reference
        const sectionText = await quickstartSection.textContent();
        expect(sectionText).toContain('12333');

        // Also verify it's in the code example
        const codeBlocks = quickstartSection.locator('.code-block code');
        let portFound = false;

        for (let i = 0; i < await codeBlocks.count(); i++) {
            const content = await codeBlocks.nth(i).textContent();
            if (content && content.includes('12333')) {
                portFound = true;
                break;
            }
        }

        expect(portFound).toBe(true);
    });

    test('copy button appears on hover and copies code', async ({ page, context }) => {
        // Grant clipboard permissions
        await context.grantPermissions(['clipboard-read', 'clipboard-write']);

        const quickstartSection = page.locator('#quickstart');
        const codeBlock = quickstartSection.locator('.code-block').first();

        // Initially, copy button may be hidden (opacity: 0)
        const copyBtn = codeBlock.locator('.copy-btn');
        await expect(copyBtn).toBeAttached();

        // Hover over code block
        await codeBlock.hover();

        // Copy button should now be visible
        await expect(copyBtn).toBeVisible();

        // Click the copy button
        await copyBtn.click();

        // Button should show "Copied!" feedback
        await expect(copyBtn).toHaveText('Copied!');

        // Verify clipboard content
        const clipboardContent = await page.evaluate(() => navigator.clipboard.readText());
        expect(clipboardContent).toContain('git clone');
    });

    test('code blocks have syntax highlighting classes', async ({ page }) => {
        const quickstartSection = page.locator('#quickstart');
        const codeBlocks = quickstartSection.locator('.code-block');

        // Check that code blocks exist
        const count = await codeBlocks.count();
        expect(count).toBeGreaterThan(0);

        // Check for various syntax highlighting classes
        const highlightClasses = [
            '.token-comment',
            '.token-command',
            '.token-keyword',
            '.token-number'
        ];

        for (const className of highlightClasses) {
            const elements = quickstartSection.locator(className);
            const elementCount = await elements.count();
            expect(elementCount).toBeGreaterThan(0);
        }
    });

    test('code blocks are horizontally scrollable on mobile', async ({ page }) => {
        // Set mobile viewport
        await page.setViewportSize({ width: 375, height: 667 });
        await page.goto('/');

        const quickstartSection = page.locator('#quickstart');
        await expect(quickstartSection).toBeVisible();

        // Check that code blocks have overflow-x: auto
        const codeBlock = quickstartSection.locator('.code-block pre').first();

        const overflowX = await codeBlock.evaluate((el) => {
            return window.getComputedStyle(el).overflowX;
        });

        expect(overflowX).toBe('auto');

        // Verify code is still readable (not cut off)
        await expect(codeBlock).toBeVisible();

        // Check that the code content is present
        const codeContent = await codeBlock.textContent();
        expect(codeContent).toBeTruthy();
        expect(codeContent!.length).toBeGreaterThan(10);
    });

    test('quickstart grid stacks on mobile', async ({ page }) => {
        // Set mobile viewport
        await page.setViewportSize({ width: 375, height: 667 });
        await page.goto('/');

        const quickstartGrid = page.locator('.quickstart-grid');
        await expect(quickstartGrid).toBeVisible();

        // Check grid layout changes to single column
        const gridTemplateColumns = await quickstartGrid.evaluate((el) => {
            return window.getComputedStyle(el).gridTemplateColumns;
        });

        // On mobile, should be single column (just one value, not "1fr 1fr")
        expect(gridTemplateColumns).not.toContain(' ');
    });

    test('section title is visible', async ({ page }) => {
        const sectionTitle = page.locator('#quickstart .section-title');
        await expect(sectionTitle).toBeVisible();
        await expect(sectionTitle).toHaveText('Quick Start');
    });

    test('copy button is accessible via keyboard', async ({ page, context }) => {
        await context.grantPermissions(['clipboard-read', 'clipboard-write']);

        const quickstartSection = page.locator('#quickstart');
        const copyBtn = quickstartSection.locator('.copy-btn').first();

        // Focus the copy button
        await copyBtn.focus();
        await expect(copyBtn).toBeFocused();

        // Press Enter to activate
        await copyBtn.press('Enter');

        // Should show copied feedback
        await expect(copyBtn).toHaveText('Copied!');
    });
});
