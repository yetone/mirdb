// @ts-check
const { test, expect } = require('@playwright/test');
const fs = require('fs');
const path = require('path');

/**
 * Unit Test for Configuration Accuracy
 * Scenario: Verify listed defaults match actual MirDB defaults
 * Tests that the HTML configuration display matches the TOML config file
 */

// Helper function to parse TOML config values
function parseTomlConfig(tomlContent) {
    const config = {};
    const lines = tomlContent.split('\n');

    for (const line of lines) {
        const trimmed = line.trim();
        if (!trimmed || trimmed.startsWith('#')) continue;

        const match = trimmed.match(/^(\w+)\s*=\s*"?([^"]+)"?$/);
        if (match) {
            const key = match[1];
            let value = match[2];

            // Normalize size values (4K -> 4KB, 100M -> 100MB)
            if (/^\d+[KMGT]$/.test(value)) {
                value = value + 'B';
            }

            config[key] = value;
        }
    }

    return config;
}

test.describe('Configuration Accuracy Tests', () => {
    let mirdbConfig;

    test.beforeAll(async () => {
        // Read the actual MirDB TOML configuration file
        const tomlPath = path.join(process.cwd(), 'etc', 'mirdb.toml');
        const tomlContent = fs.readFileSync(tomlPath, 'utf-8');
        mirdbConfig = parseTomlConfig(tomlContent);
    });

    test.beforeEach(async ({ page }) => {
        await page.goto('/');
    });

    /**
     * Test Case 4: Verify configuration accuracy - listed defaults match actual MirDB defaults
     */
    test('TC4: Listed defaults match actual MirDB defaults', async ({ page }) => {
        // Scroll to technical overview section
        const technicalOverviewSection = page.locator('[data-testid="technical-overview-section"]');
        await technicalOverviewSection.scrollIntoViewIfNeeded();

        // Verify port matches (addr in TOML)
        const portValue = await page.locator('[data-testid="config-port"]').textContent();
        expect(portValue.trim()).toBe(mirdbConfig.addr);

        // Verify work directory matches
        const workDirValue = await page.locator('[data-testid="config-work-dir"]').textContent();
        expect(workDirValue.trim()).toBe(mirdbConfig.work_dir);

        // Verify max level matches
        const maxLevelValue = await page.locator('[data-testid="config-max-level"]').textContent();
        expect(maxLevelValue.trim()).toBe(mirdbConfig.max_level);

        // Verify SSTable max size matches (100M -> 100MB)
        const sstSizeValue = await page.locator('[data-testid="config-sst-size"]').textContent();
        expect(sstSizeValue.trim()).toBe(mirdbConfig.sst_max_size);

        // Verify memtable max size matches (4M -> 4MB)
        const memtableSizeValue = await page.locator('[data-testid="config-memtable-size"]').textContent();
        expect(memtableSizeValue.trim()).toBe(mirdbConfig.mem_table_max_size);

        // Verify block size matches (4K -> 4KB)
        const blockSizeValue = await page.locator('[data-testid="config-block-size"]').textContent();
        expect(blockSizeValue.trim()).toBe(mirdbConfig.block_size);
    });

    /**
     * Test: All critical configuration values from TOML are displayed
     */
    test('All critical configuration values are displayed', async ({ page }) => {
        // Verify the TOML has expected keys
        expect(mirdbConfig).toHaveProperty('addr');
        expect(mirdbConfig).toHaveProperty('work_dir');
        expect(mirdbConfig).toHaveProperty('max_level');
        expect(mirdbConfig).toHaveProperty('sst_max_size');
        expect(mirdbConfig).toHaveProperty('mem_table_max_size');
        expect(mirdbConfig).toHaveProperty('block_size');

        // Verify each is displayed in the table
        const configTable = page.locator('[data-testid="config-table"]');
        await expect(configTable).toBeVisible();

        // Check table contains parameter names
        await expect(configTable).toContainText('addr');
        await expect(configTable).toContainText('work_dir');
        await expect(configTable).toContainText('max_level');
        await expect(configTable).toContainText('sst_max_size');
        await expect(configTable).toContainText('mem_table_max_size');
        await expect(configTable).toContainText('block_size');
    });

    /**
     * Test: Port 12333 is correctly displayed
     */
    test('Port 12333 is correctly displayed', async ({ page }) => {
        const portValue = await page.locator('[data-testid="config-port"]').textContent();

        // Verify port contains 12333
        expect(portValue).toContain('12333');

        // Verify it matches the TOML file
        expect(mirdbConfig.addr).toContain('12333');
        expect(portValue.trim()).toBe(mirdbConfig.addr);
    });
});
