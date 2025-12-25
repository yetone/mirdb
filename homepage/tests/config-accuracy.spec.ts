import { test, expect } from '@playwright/test';
import * as fs from 'fs';
import * as path from 'path';

/**
 * Integration tests to verify configuration documentation accuracy.
 * These tests ensure documented configuration values match the actual implementation
 * in the MirDB Rust source code (mirdb-server/src/options.rs and config.rs).
 */

// Expected configuration values derived from source code (options.rs and etc/mirdb.toml)
const SOURCE_CONFIG_VALUES = {
  // From options.rs default() and etc/mirdb.toml
  addr: '0.0.0.0:12333',
  max_level: '7',
  work_dir: '/tmp/mirdb',
  sst_max_size: '100M',
  mem_table_max_size: '4M',
  mem_table_max_height: '32',
  imm_mem_table_max_count: '16',
  block_size: '4K',
  block_restart_interval: '16',
  l0_compaction_trigger: '4',
  thread_sleep_ms: '500',
};

// Expected parameter names from the TOML config format
const EXPECTED_PARAM_NAMES = [
  'addr',
  'max_level',
  'work_dir',
  'sst_max_size',
  'mem_table_max_size',
  'mem_table_max_height',
  'imm_mem_table_max_count',
  'block_size',
  'block_restart_interval',
  'l0_compaction_trigger',
  'thread_sleep_ms',
];

test.describe('Content Accuracy - Configuration Values', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Documented memtable_size default matches implementation (4M)', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Get the config table
    const configTable = configSection.locator('[data-testid="config-table"]');
    await expect(configTable).toBeVisible();

    // Find the row containing mem_table_max_size parameter
    const memtableRow = configTable.locator('tbody tr').filter({
      has: page.locator('code').filter({ hasText: /mem_table_max_size/i })
    });
    await expect(memtableRow).toBeVisible();

    // Get the default value cell (2nd column - index 1)
    const defaultCell = memtableRow.locator('td').nth(1);
    const defaultValue = await defaultCell.textContent();

    // Verify the documented default matches the source code default (4M)
    expect(defaultValue?.trim()).toContain(SOURCE_CONFIG_VALUES.mem_table_max_size);
  });

  test('TC2: Documented port default matches implementation (12333)', async ({ page }) => {
    // The test case says port 11211, but the actual default in source is 12333
    // This test verifies the documentation shows the correct default port

    // Navigate to configuration section
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Get the config table
    const configTable = configSection.locator('[data-testid="config-table"]');
    await expect(configTable).toBeVisible();

    // Find the row containing addr parameter
    const addrRow = configTable.locator('tbody tr').filter({
      has: page.locator('code').filter({ hasText: /^addr$/i })
    });
    await expect(addrRow).toBeVisible();

    // Get the default value cell (2nd column - index 1)
    const defaultCell = addrRow.locator('td').nth(1);
    const defaultValue = await defaultCell.textContent();

    // Verify the documented default includes the correct port (12333, NOT 11211)
    // The actual implementation uses port 12333, not the memcached default 11211
    expect(defaultValue?.trim()).toContain('12333');
    expect(defaultValue?.trim()).toContain(SOURCE_CONFIG_VALUES.addr);
  });

  test('TC3: All documented parameters exist in the configuration system', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Get the config table
    const configTable = configSection.locator('[data-testid="config-table"]');
    await expect(configTable).toBeVisible();

    // Get all parameter names from the table
    const paramCells = configTable.locator('tbody tr td:first-child code');
    const paramCount = await paramCells.count();

    // Collect all documented parameter names
    const documentedParams: string[] = [];
    for (let i = 0; i < paramCount; i++) {
      const paramName = await paramCells.nth(i).textContent();
      if (paramName) {
        documentedParams.push(paramName.trim());
      }
    }

    // Verify all expected parameters are documented
    for (const expectedParam of EXPECTED_PARAM_NAMES) {
      const found = documentedParams.some(
        p => p.toLowerCase() === expectedParam.toLowerCase()
      );
      expect(found, `Parameter '${expectedParam}' should be documented`).toBeTruthy();
    }

    // Verify no extra (non-existent) parameters are documented
    for (const docParam of documentedParams) {
      const exists = EXPECTED_PARAM_NAMES.some(
        p => p.toLowerCase() === docParam.toLowerCase()
      );
      expect(exists, `Documented parameter '${docParam}' should exist in implementation`).toBeTruthy();
    }
  });

  test('TOML example uses correct parameter names', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Get the TOML example
    const tomlExample = configSection.locator('[data-testid="toml-example"]');
    await expect(tomlExample).toBeVisible();

    const tomlContent = await tomlExample.textContent();

    // Verify TOML uses correct parameter names (not incorrect ones like listen_addr, max_levels, etc.)
    // These are the correct TOML parameter names from etc/mirdb.toml
    expect(tomlContent).toContain('addr =');
    expect(tomlContent).toContain('max_level =');
    expect(tomlContent).toContain('work_dir =');
    expect(tomlContent).toContain('sst_max_size =');
    expect(tomlContent).toContain('mem_table_max_size =');
    expect(tomlContent).toContain('mem_table_max_height =');
    expect(tomlContent).toContain('imm_mem_table_max_count =');
    expect(tomlContent).toContain('block_size =');
    expect(tomlContent).toContain('block_restart_interval =');
    expect(tomlContent).toContain('l0_compaction_trigger =');
    expect(tomlContent).toContain('thread_sleep_ms =');

    // Verify incorrect parameter names are NOT used
    expect(tomlContent).not.toContain('listen_addr');
    expect(tomlContent).not.toContain('max_levels'); // plural is wrong
    expect(tomlContent).not.toContain('sstable_max_size'); // should be sst_max_size
    expect(tomlContent).not.toContain('memtable_max_size'); // should be mem_table_max_size
  });

  test('Default values in table match source code defaults', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Get the config table
    const configTable = configSection.locator('[data-testid="config-table"]');
    await expect(configTable).toBeVisible();

    // Check each configuration parameter's default value
    const rows = configTable.locator('tbody tr');
    const rowCount = await rows.count();

    const documentedDefaults: Record<string, string> = {};

    for (let i = 0; i < rowCount; i++) {
      const row = rows.nth(i);
      const paramCell = row.locator('td').nth(0).locator('code');
      const defaultCell = row.locator('td').nth(1).locator('code');

      const paramName = await paramCell.textContent();
      const defaultValue = await defaultCell.textContent();

      if (paramName && defaultValue) {
        documentedDefaults[paramName.trim()] = defaultValue.trim();
      }
    }

    // Verify documented defaults match source code
    for (const [param, expectedDefault] of Object.entries(SOURCE_CONFIG_VALUES)) {
      if (documentedDefaults[param]) {
        expect(
          documentedDefaults[param],
          `Default for '${param}' should match source code`
        ).toBe(expectedDefault);
      }
    }
  });
});
