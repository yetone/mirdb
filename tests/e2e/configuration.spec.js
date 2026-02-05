/**
 * Configuration Section E2E Tests
 * Owner: Scenario 5 - Configuration Section Display
 *
 * Tests for:
 * - Configuration table presence
 * - Default port (12333)
 * - Default directory (/tmp/mirdb)
 * - Size settings display
 * - Table format with columns for setting name, value, and description
 */

const { test, expect } = require('@playwright/test');

test.describe('Configuration Section', () => {
    test.beforeEach(async ({ page, baseURL }) => {
        const url = baseURL || 'http://localhost:3000';
        await page.goto(url);
    });

    test('should display Configuration section with heading', async ({ page }) => {
        // Test Case 1: Locate Configuration section
        // Expected: Section with heading containing 'Configuration' or 'Settings' is visible
        const configSection = page.locator('#configuration');
        await expect(configSection).toBeVisible();

        // Check for a heading containing 'Configuration' or 'Settings'
        const heading = configSection.locator('h2, h3').first();
        await expect(heading).toBeVisible();
        const headingText = await heading.textContent();
        expect(
            headingText.toLowerCase().includes('configuration') ||
            headingText.toLowerCase().includes('settings')
        ).toBeTruthy();
    });

    test('should display default port value', async ({ page }) => {
        // Test Case 2: Check for default port value
        // Expected: Port '12333' or '0.0.0.0:12333' is displayed
        const configSection = page.locator('#configuration');

        // Look for either '12333' or '0.0.0.0:12333' in the configuration section
        const configText = await configSection.textContent();
        const hasPort = configText.includes('12333') || configText.includes('0.0.0.0:12333');
        expect(hasPort).toBeTruthy();
    });

    test('should display default work directory', async ({ page }) => {
        // Test Case 3: Check for default work directory
        // Expected: Directory '/tmp/mirdb' is displayed
        const configSection = page.locator('#configuration');

        // Look for '/tmp/mirdb' in the configuration section
        const configText = await configSection.textContent();
        expect(configText).toContain('/tmp/mirdb');
    });

    test('should display at least one size-related setting', async ({ page }) => {
        // Test Case 4: Check for size limits display
        // Expected: At least one size-related setting (SSTable size, memtable size, block size) is displayed
        const configSection = page.locator('#configuration');
        const configText = await configSection.textContent();

        // Check for any size-related settings
        const hasSizeSetting =
            configText.toLowerCase().includes('sstable') ||
            configText.toLowerCase().includes('memtable') ||
            configText.toLowerCase().includes('block size') ||
            configText.includes('MB') ||
            configText.includes('KB');

        expect(hasSizeSetting).toBeTruthy();
    });

    test('should display configuration in a table with proper columns', async ({ page }) => {
        // Test Case 5: Verify table format
        // Expected: Configuration is displayed in a table with columns for setting name, value, and description
        const configSection = page.locator('#configuration');

        // Check that there is a table element
        const table = configSection.locator('table');
        await expect(table).toBeVisible();

        // Check for table headers (setting name, value, description)
        const headers = table.locator('th');
        const headerCount = await headers.count();
        expect(headerCount).toBeGreaterThanOrEqual(3);

        // Verify header content
        const headerTexts = await headers.allTextContents();
        const headerTextLower = headerTexts.map(h => h.toLowerCase());

        // Check for columns related to setting/name, value, and description
        const hasSettingColumn = headerTextLower.some(h =>
            h.includes('setting') || h.includes('name') || h.includes('option')
        );
        const hasValueColumn = headerTextLower.some(h =>
            h.includes('value') || h.includes('default')
        );
        const hasDescriptionColumn = headerTextLower.some(h =>
            h.includes('description') || h.includes('desc')
        );

        expect(hasSettingColumn).toBeTruthy();
        expect(hasValueColumn).toBeTruthy();
        expect(hasDescriptionColumn).toBeTruthy();

        // Verify there are data rows
        const rows = table.locator('tbody tr');
        const rowCount = await rows.count();
        expect(rowCount).toBeGreaterThan(0);
    });
});
