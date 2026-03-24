/**
 * Configuration Reference Section E2E Tests
 * Owner: Scenario 6 - Configuration Reference
 *
 * Tests for:
 * - Configuration section presence
 * - Default configuration values from PRD
 * - Table layout and accessibility
 */

import { test, expect } from '@playwright/test';

test.describe('Configuration Reference Section', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
    });

    // Test Case 1: Configuration section is present on page
    test('should display configuration reference section', async ({ page }) => {
        const configSection = page.locator('#config');
        await expect(configSection).toBeVisible();

        // Check for section title
        const sectionTitle = configSection.locator('.section-title, h2');
        await expect(sectionTitle).toBeVisible();
        await expect(sectionTitle).toContainText(/configuration/i);
    });

    // Test Case 2: Default listen address is documented
    test('should display default listen address 0.0.0.0:12333', async ({ page }) => {
        const configSection = page.locator('#config');
        await expect(configSection).toBeVisible();

        // Find the listen_address row and verify value
        const listenAddressValue = configSection.locator('[data-config="listen-address"]');
        await expect(listenAddressValue).toBeVisible();
        await expect(listenAddressValue).toHaveText('0.0.0.0:12333');
    });

    // Test Case 3: Default max LSM levels is documented
    test('should display default max LSM levels (7)', async ({ page }) => {
        const configSection = page.locator('#config');
        await expect(configSection).toBeVisible();

        // Find the max_lsm_levels row and verify value
        const maxLsmLevelsValue = configSection.locator('[data-config="max-lsm-levels"]');
        await expect(maxLsmLevelsValue).toBeVisible();
        await expect(maxLsmLevelsValue).toHaveText('7');
    });

    // Test Case 4: Default work directory is documented
    test('should display default work directory (/tmp/mirdb)', async ({ page }) => {
        const configSection = page.locator('#config');
        await expect(configSection).toBeVisible();

        // Find the work_directory row and verify value
        const workDirectoryValue = configSection.locator('[data-config="work-directory"]');
        await expect(workDirectoryValue).toBeVisible();
        await expect(workDirectoryValue).toHaveText('/tmp/mirdb');
    });

    // Test Case 5: Default SSTable max size is documented
    test('should display default SSTable max size (100MB)', async ({ page }) => {
        const configSection = page.locator('#config');
        await expect(configSection).toBeVisible();

        // Find the sstable_max_size row and verify value
        const sstableMaxSizeValue = configSection.locator('[data-config="sstable-max-size"]');
        await expect(sstableMaxSizeValue).toBeVisible();
        await expect(sstableMaxSizeValue).toHaveText('100MB');
    });

    // Test Case 6: Default memtable max size is documented
    test('should display default memtable max size (4MB)', async ({ page }) => {
        const configSection = page.locator('#config');
        await expect(configSection).toBeVisible();

        // Find the memtable_max_size row and verify value
        const memtableMaxSizeValue = configSection.locator('[data-config="memtable-max-size"]');
        await expect(memtableMaxSizeValue).toBeVisible();
        await expect(memtableMaxSizeValue).toHaveText('4MB');
    });

    // Additional accessibility tests
    test('configuration table should have proper table role', async ({ page }) => {
        const configTable = page.locator('.config-table');
        await expect(configTable).toBeVisible();
        await expect(configTable).toHaveAttribute('role', 'table');
    });

    test('configuration table should have column headers', async ({ page }) => {
        const configTable = page.locator('.config-table');
        await expect(configTable).toBeVisible();

        const headers = configTable.locator('th');
        const headerCount = await headers.count();
        expect(headerCount).toBe(3); // Parameter, Default Value, Description

        await expect(headers.nth(0)).toContainText(/parameter/i);
        await expect(headers.nth(1)).toContainText(/default|value/i);
        await expect(headers.nth(2)).toContainText(/description/i);
    });

    test('configuration section should have intro paragraph', async ({ page }) => {
        const configIntro = page.locator('.config-intro');
        await expect(configIntro).toBeVisible();
        const introText = await configIntro.textContent();
        expect(introText?.toLowerCase()).toMatch(/config|parameter|default/i);
    });
});
