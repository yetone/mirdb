/**
 * Quick Start Section Tests
 * Owner: Scenario 3 - Quick Start Section
 *
 * Tests:
 * - Code block presence
 * - cargo install command
 * - telnet usage example
 * - SET/GET command examples
 * - Syntax highlighting
 */

const { test, expect } = require('@playwright/test');
const { selectors, gotoHomepage } = require('./test-utils');

test.describe('Quick Start Section', () => {
    test.beforeEach(async ({ page }) => {
        await gotoHomepage(page);
    });

    test('TC1: Quick start section has code blocks with installation commands', async ({ page }) => {
        // Navigate to quick start section
        const quickstartSection = page.locator(selectors.quickstart);
        await expect(quickstartSection).toBeVisible();

        // Check for code blocks
        const codeBlocks = quickstartSection.locator('.code-block');
        const count = await codeBlocks.count();
        expect(count).toBeGreaterThanOrEqual(1);

        // Verify code blocks have content
        const firstCodeBlock = codeBlocks.first();
        await expect(firstCodeBlock).toBeVisible();
        const content = await firstCodeBlock.textContent();
        expect(content.length).toBeGreaterThan(0);
    });

    test('TC2: Code block contains cargo install mirdb command', async ({ page }) => {
        const quickstartSection = page.locator(selectors.quickstart);
        await expect(quickstartSection).toBeVisible();

        // Check for cargo install command
        const pageContent = await quickstartSection.textContent();
        expect(pageContent).toContain('cargo install mirdb');
    });

    test('TC3: Code block contains telnet localhost 12333 command', async ({ page }) => {
        const quickstartSection = page.locator(selectors.quickstart);
        await expect(quickstartSection).toBeVisible();

        // Check for telnet usage command
        const pageContent = await quickstartSection.textContent();
        expect(pageContent).toContain('telnet localhost 12333');
    });

    test('TC4: Code block contains SET command example', async ({ page }) => {
        const quickstartSection = page.locator(selectors.quickstart);
        await expect(quickstartSection).toBeVisible();

        // Check for SET command
        const pageContent = await quickstartSection.textContent();
        expect(pageContent).toContain('SET');
    });

    test('TC5: Code block contains GET command example', async ({ page }) => {
        const quickstartSection = page.locator(selectors.quickstart);
        await expect(quickstartSection).toBeVisible();

        // Check for GET command
        const pageContent = await quickstartSection.textContent();
        expect(pageContent).toContain('GET');
    });

    test('TC6: Code blocks have syntax highlighting classes applied', async ({ page }) => {
        const quickstartSection = page.locator(selectors.quickstart);
        await expect(quickstartSection).toBeVisible();

        // Check for syntax-highlighted class on code blocks
        const highlightedBlocks = quickstartSection.locator('.code-block.syntax-highlighted');
        const count = await highlightedBlocks.count();
        expect(count).toBeGreaterThanOrEqual(1);

        // Verify syntax highlighting span classes exist
        const firstBlock = highlightedBlocks.first();

        // Check for at least one syntax highlighting element
        const syntaxElements = firstBlock.locator('[class^="code-"]');
        const syntaxCount = await syntaxElements.count();
        expect(syntaxCount).toBeGreaterThan(0);
    });
});
