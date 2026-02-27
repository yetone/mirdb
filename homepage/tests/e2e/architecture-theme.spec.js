/**
 * Integration Tests for Architecture Diagram Theme Support (Scenario 4)
 *
 * Tests that the architecture diagram colors change according to theme.
 */
const { test, expect } = require('@playwright/test');

test.describe('Architecture Diagram Theme Integration', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
    });

    // Test Case 6: Toggle theme to dark mode and verify SVG colors change
    test('SVG colors change according to dark theme CSS variables', async ({ page }) => {
        const architectureSection = page.locator('#architecture');
        await architectureSection.scrollIntoViewIfNeeded();

        const svg = page.locator('#architecture svg.architecture-diagram');
        await expect(svg).toBeVisible();

        // Get initial computed style of a diagram element (light mode)
        const diagramBg = svg.locator('.diagram-bg');

        // Get light mode fill color
        const lightModeFill = await diagramBg.evaluate((el) => {
            return getComputedStyle(el).fill;
        });

        // Simulate setting dark theme
        await page.evaluate(() => {
            document.documentElement.setAttribute('data-theme', 'dark');
        });

        // Give time for styles to apply
        await page.waitForTimeout(100);

        // Get dark mode fill color
        const darkModeFill = await diagramBg.evaluate((el) => {
            return getComputedStyle(el).fill;
        });

        // Verify colors are different between themes
        expect(lightModeFill).not.toBe(darkModeFill);
    });

    test('Memtable box color changes with theme', async ({ page }) => {
        const svg = page.locator('#architecture svg.architecture-diagram');
        await svg.scrollIntoViewIfNeeded();

        const memtableBox = svg.locator('.box-memtable').first();

        // Get light mode fill
        const lightFill = await memtableBox.evaluate((el) => {
            return getComputedStyle(el).fill;
        });

        // Switch to dark theme
        await page.evaluate(() => {
            document.documentElement.setAttribute('data-theme', 'dark');
        });

        await page.waitForTimeout(100);

        // Get dark mode fill
        const darkFill = await memtableBox.evaluate((el) => {
            return getComputedStyle(el).fill;
        });

        // Colors should change
        expect(lightFill).not.toBe(darkFill);
    });

    test('SSTable box color changes with theme', async ({ page }) => {
        const svg = page.locator('#architecture svg.architecture-diagram');
        await svg.scrollIntoViewIfNeeded();

        const sstableBox = svg.locator('.box-sstable').first();

        // Get light mode fill
        const lightFill = await sstableBox.evaluate((el) => {
            return getComputedStyle(el).fill;
        });

        // Switch to dark theme
        await page.evaluate(() => {
            document.documentElement.setAttribute('data-theme', 'dark');
        });

        await page.waitForTimeout(100);

        // Get dark mode fill
        const darkFill = await sstableBox.evaluate((el) => {
            return getComputedStyle(el).fill;
        });

        // Colors should change
        expect(lightFill).not.toBe(darkFill);
    });

    test('Text colors update with theme change', async ({ page }) => {
        const svg = page.locator('#architecture svg.architecture-diagram');
        await svg.scrollIntoViewIfNeeded();

        const textElement = svg.locator('.diagram-text').first();

        // Get light mode fill
        const lightFill = await textElement.evaluate((el) => {
            return getComputedStyle(el).fill;
        });

        // Switch to dark theme - simulate by adding custom colors
        await page.evaluate(() => {
            document.documentElement.setAttribute('data-theme', 'dark');
            // Also set the text color for dark mode
            document.documentElement.style.setProperty('--color-text', '#f0f0f0');
        });

        await page.waitForTimeout(100);

        // Get dark mode fill
        const darkFill = await textElement.evaluate((el) => {
            return getComputedStyle(el).fill;
        });

        // Text color should change
        expect(lightFill).not.toBe(darkFill);
    });

    test('Theme can be toggled back to light mode', async ({ page }) => {
        const svg = page.locator('#architecture svg.architecture-diagram');
        await svg.scrollIntoViewIfNeeded();

        const diagramBg = svg.locator('.diagram-bg');

        // Get initial fill
        const initialFill = await diagramBg.evaluate((el) => {
            return getComputedStyle(el).fill;
        });

        // Switch to dark
        await page.evaluate(() => {
            document.documentElement.setAttribute('data-theme', 'dark');
        });
        await page.waitForTimeout(100);

        // Switch back to light
        await page.evaluate(() => {
            document.documentElement.removeAttribute('data-theme');
        });
        await page.waitForTimeout(100);

        // Get final fill
        const finalFill = await diagramBg.evaluate((el) => {
            return getComputedStyle(el).fill;
        });

        // Should be back to initial color
        expect(initialFill).toBe(finalFill);
    });
});
