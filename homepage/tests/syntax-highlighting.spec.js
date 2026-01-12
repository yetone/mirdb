// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Syntax Highlighting Tests (NFR-5)
 *
 * Verifies that code examples on the homepage have proper syntax highlighting
 * for readability, with monospace fonts and visual differentiation.
 */
test.describe('Syntax Highlighting - NFR-5', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
    });

    // Test Case 1: Check installation command block styling
    test('TC1: Installation command has monospace font, distinguished from body text', async ({ page }) => {
        // Navigate to quick-start section
        const quickStartSection = page.locator('#quick-start');
        await quickStartSection.scrollIntoViewIfNeeded();
        await expect(quickStartSection).toBeVisible();

        // Get the installation code block (first code-block in quick-start)
        const installationBlock = quickStartSection.locator('.code-block').first();
        await expect(installationBlock).toBeVisible();

        // Get the code element
        const codeElement = installationBlock.locator('code');
        await expect(codeElement).toBeVisible();

        // Verify code element has monospace font
        const codeStyles = await codeElement.evaluate((el) => {
            const computed = window.getComputedStyle(el);
            return {
                fontFamily: computed.fontFamily,
                color: computed.color,
                backgroundColor: window.getComputedStyle(el.closest('.code-block')).backgroundColor
            };
        });

        // Verify monospace font family (should contain 'Fira Code', 'Monaco', 'Consolas', or 'monospace')
        const isMonospace = codeStyles.fontFamily.toLowerCase().includes('monospace') ||
                           codeStyles.fontFamily.toLowerCase().includes('fira') ||
                           codeStyles.fontFamily.toLowerCase().includes('monaco') ||
                           codeStyles.fontFamily.toLowerCase().includes('consolas');
        expect(isMonospace).toBe(true);

        // Get body text styles for comparison
        const bodyStyles = await page.evaluate(() => {
            const body = document.querySelector('body');
            return {
                fontFamily: window.getComputedStyle(body).fontFamily,
                color: window.getComputedStyle(body).color
            };
        });

        // Verify code font is different from body font (distinguished)
        expect(codeStyles.fontFamily).not.toBe(bodyStyles.fontFamily);

        // Verify code block has a dark background (distinguishes from body)
        // Parse the background color and check it's dark (RGB values should be low)
        const bgColorMatch = codeStyles.backgroundColor.match(/rgb[a]?\((\d+),\s*(\d+),\s*(\d+)/);
        if (bgColorMatch) {
            const [_, r, g, b] = bgColorMatch.map(Number);
            // Dark background means RGB values are generally below 100
            expect(r).toBeLessThan(100);
            expect(g).toBeLessThan(100);
            expect(b).toBeLessThan(100);
        }

        // Verify the code color contrasts with background (light text on dark background)
        const codeColorMatch = codeStyles.color.match(/rgb[a]?\((\d+),\s*(\d+),\s*(\d+)/);
        if (codeColorMatch) {
            const [_, r, g, b] = codeColorMatch.map(Number);
            // Light text should have at least one high RGB value (cyan #00d4ff = 0, 212, 255)
            const maxRgb = Math.max(r, g, b);
            expect(maxRgb).toBeGreaterThan(150);
        }
    });

    // Test Case 2: Check configuration example highlighting
    test('TC2: Configuration example has TOML/config syntax with appropriate highlighting', async ({ page }) => {
        // Navigate to configuration section
        const configSection = page.locator('#configuration');
        await configSection.scrollIntoViewIfNeeded();
        await expect(configSection).toBeVisible();

        // Get the configuration code block
        const configBlock = configSection.locator('.code-block');
        await expect(configBlock).toBeVisible();

        // Get the code element
        const codeElement = configBlock.locator('code');
        await expect(codeElement).toBeVisible();

        // Verify configuration content contains key-value pairs
        const codeText = await codeElement.textContent();

        // Check for TOML-style key = "value" patterns
        expect(codeText).toMatch(/addr\s*=\s*"/);
        expect(codeText).toMatch(/max_level\s*=\s*\d+/);
        expect(codeText).toMatch(/work_dir\s*=\s*"/);
        expect(codeText).toMatch(/sst_max_size\s*=\s*"/);
        expect(codeText).toMatch(/mem_table_max_size\s*=\s*"/);
        expect(codeText).toMatch(/block_size\s*=\s*"/);

        // Verify code has proper styling (monospace, colored, dark background)
        const styles = await codeElement.evaluate((el) => {
            const computed = window.getComputedStyle(el);
            const bgComputed = window.getComputedStyle(el.closest('.code-block'));
            return {
                fontFamily: computed.fontFamily,
                color: computed.color,
                backgroundColor: bgComputed.backgroundColor,
                lineHeight: computed.lineHeight
            };
        });

        // Verify monospace font
        const isMonospace = styles.fontFamily.toLowerCase().includes('monospace') ||
                           styles.fontFamily.toLowerCase().includes('fira') ||
                           styles.fontFamily.toLowerCase().includes('monaco') ||
                           styles.fontFamily.toLowerCase().includes('consolas');
        expect(isMonospace).toBe(true);

        // Verify the code is styled with a contrasting color
        const colorMatch = styles.color.match(/rgb[a]?\((\d+),\s*(\d+),\s*(\d+)/);
        if (colorMatch) {
            const [_, r, g, b] = colorMatch.map(Number);
            // Should have colored text (not just black or white)
            const hasColor = (r + g + b) > 100 && (r + g + b) < 700;
            expect(hasColor).toBe(true);
        }

        // Verify good line height for readability
        const lineHeightValue = parseFloat(styles.lineHeight);
        expect(lineHeightValue).toBeGreaterThan(1);
    });

    // Test Case 3: Check memcached protocol examples highlighting
    test('TC3: Memcached protocol commands have appropriate highlighting', async ({ page }) => {
        // Navigate to quick-start section where protocol examples are
        const quickStartSection = page.locator('#quick-start');
        await quickStartSection.scrollIntoViewIfNeeded();
        await expect(quickStartSection).toBeVisible();

        // Get the usage example code block (second code-block has protocol examples)
        const usageBlock = quickStartSection.locator('.code-block').nth(1);
        await expect(usageBlock).toBeVisible();

        // Get the code element
        const codeElement = usageBlock.locator('code');
        await expect(codeElement).toBeVisible();

        // Verify the protocol commands content
        const codeText = await codeElement.textContent();

        // Check for memcached protocol commands
        expect(codeText).toContain('set mykey');
        expect(codeText).toContain('get mykey');
        expect(codeText).toContain('STORED');
        expect(codeText).toContain('VALUE');
        expect(codeText).toContain('END');

        // Check for shell commands
        expect(codeText).toContain('mirdb-server');
        expect(codeText).toContain('telnet');

        // Verify code styling
        const styles = await codeElement.evaluate((el) => {
            const computed = window.getComputedStyle(el);
            return {
                fontFamily: computed.fontFamily,
                color: computed.color,
                fontSize: computed.fontSize
            };
        });

        // Verify monospace font for code
        const isMonospace = styles.fontFamily.toLowerCase().includes('monospace') ||
                           styles.fontFamily.toLowerCase().includes('fira') ||
                           styles.fontFamily.toLowerCase().includes('monaco') ||
                           styles.fontFamily.toLowerCase().includes('consolas');
        expect(isMonospace).toBe(true);

        // Verify readable font size (at least 14px equivalent, 0.9rem ≈ 14.4px)
        const fontSizeValue = parseFloat(styles.fontSize);
        expect(fontSizeValue).toBeGreaterThanOrEqual(13);
    });

    // Additional test: Verify commands table code elements have syntax styling
    test('Commands table code elements have proper monospace styling', async ({ page }) => {
        // Navigate to commands section
        const commandsSection = page.locator('#commands');
        await commandsSection.scrollIntoViewIfNeeded();
        await expect(commandsSection).toBeVisible();

        // Get code elements in the commands table
        const tableCode = commandsSection.locator('.commands-table code').first();
        await expect(tableCode).toBeVisible();

        // Verify inline code styling
        const styles = await tableCode.evaluate((el) => {
            const computed = window.getComputedStyle(el);
            return {
                fontFamily: computed.fontFamily,
                color: computed.color,
                backgroundColor: computed.backgroundColor,
                padding: computed.padding,
                borderRadius: computed.borderRadius
            };
        });

        // Verify monospace font
        const isMonospace = styles.fontFamily.toLowerCase().includes('monospace') ||
                           styles.fontFamily.toLowerCase().includes('fira') ||
                           styles.fontFamily.toLowerCase().includes('monaco') ||
                           styles.fontFamily.toLowerCase().includes('consolas');
        expect(isMonospace).toBe(true);

        // Verify inline code has background styling (pill style)
        const bgMatch = styles.backgroundColor.match(/rgb[a]?\((\d+),\s*(\d+),\s*(\d+)/);
        expect(bgMatch).toBeTruthy();

        // Verify border radius for pill styling
        expect(parseFloat(styles.borderRadius)).toBeGreaterThan(0);
    });

    // Additional test: Verify all code blocks have pre elements for proper formatting
    test('All code blocks have pre elements for whitespace preservation', async ({ page }) => {
        // Get all code blocks on the page
        const codeBlocks = page.locator('.code-block');
        const count = await codeBlocks.count();

        // Ensure there are code blocks to test
        expect(count).toBeGreaterThan(0);

        // Check each code block has a pre element
        for (let i = 0; i < count; i++) {
            const codeBlock = codeBlocks.nth(i);
            await codeBlock.scrollIntoViewIfNeeded();

            const preElement = codeBlock.locator('pre');
            await expect(preElement).toBeVisible();

            const codeElement = codeBlock.locator('code');
            await expect(codeElement).toBeVisible();
        }
    });
});
