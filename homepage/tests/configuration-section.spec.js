// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Configuration Options Display - Scenario 5', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
    });

    // Test Case 1: Check network configuration documentation
    test('TC1: should display network configuration with listen address default 0.0.0.0:12333', async ({ page }) => {
        // Navigate to configuration section
        const configSection = page.locator('#configuration');
        await expect(configSection).toBeVisible();

        // Check the section title
        const title = configSection.locator('h2');
        await expect(title).toHaveText('Configuration');

        // Check for network configuration - listen address with default
        const configContent = await configSection.textContent();
        expect(configContent).toContain('0.0.0.0:12333');

        // Verify addr parameter is documented
        expect(configContent).toMatch(/addr|listen.*address/i);
    });

    // Test Case 2: Check storage configuration documentation
    test('TC2: should display storage configuration with work directory and max LSM levels (default: 7)', async ({ page }) => {
        // Navigate to configuration section
        const configSection = page.locator('#configuration');
        await expect(configSection).toBeVisible();

        const configContent = await configSection.textContent();

        // Check for work directory
        expect(configContent).toMatch(/work_dir|work.*directory/i);
        expect(configContent).toContain('/tmp/mirdb');

        // Check for max LSM levels with default of 7
        expect(configContent).toMatch(/max_level|lsm.*level/i);
        expect(configContent).toContain('7');
    });

    // Test Case 3: Check memory configuration documentation
    test('TC3: should display memory configuration with memtable size (default: 4MB)', async ({ page }) => {
        // Navigate to configuration section
        const configSection = page.locator('#configuration');
        await expect(configSection).toBeVisible();

        const configContent = await configSection.textContent();

        // Check for memtable size configuration
        expect(configContent).toMatch(/mem_table_max_size|memtable.*size/i);

        // Check for 4MB default (can be "4M" or "4MB")
        expect(configContent).toMatch(/4M[B]?/i);
    });

    // Test Case 4: Check SSTable configuration documentation
    test('TC4: should display SSTable configuration with max size (default: 100MB) and block size (default: 4KB)', async ({ page }) => {
        // Navigate to configuration section
        const configSection = page.locator('#configuration');
        await expect(configSection).toBeVisible();

        const configContent = await configSection.textContent();

        // Check for SSTable max size with default of 100MB
        expect(configContent).toMatch(/sst_max_size|sstable.*size/i);
        expect(configContent).toMatch(/100M[B]?/i);

        // Check for block size with default of 4KB
        expect(configContent).toMatch(/block_size|block.*size/i);
        expect(configContent).toMatch(/4K[B]?/i);
    });

    // Additional test: Verify configuration section has code block with copy functionality
    test('should display configuration in a code block', async ({ page }) => {
        const configSection = page.locator('#configuration');
        await expect(configSection).toBeVisible();

        // Check for code block element
        const codeBlock = configSection.locator('.code-block');
        await expect(codeBlock).toBeVisible();

        // Check for pre/code elements
        const preElement = codeBlock.locator('pre');
        await expect(preElement).toBeVisible();

        const codeElement = codeBlock.locator('code');
        await expect(codeElement).toBeVisible();
    });

    // Additional test: Verify all major configuration categories are documented
    test('should document all major configuration categories', async ({ page }) => {
        const configSection = page.locator('#configuration');
        await expect(configSection).toBeVisible();

        const configContent = await configSection.textContent();

        // Network configuration
        expect(configContent).toContain('addr');

        // Storage configuration
        expect(configContent).toContain('max_level');
        expect(configContent).toContain('work_dir');

        // Memory configuration
        expect(configContent).toContain('mem_table_max_size');

        // SSTable configuration
        expect(configContent).toContain('sst_max_size');
        expect(configContent).toContain('block_size');
    });
});
