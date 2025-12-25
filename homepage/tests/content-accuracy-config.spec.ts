import { test, expect } from '@playwright/test';
import * as fs from 'fs';
import * as path from 'path';

/**
 * Content Accuracy - Configuration Values Tests
 *
 * These tests verify that the configuration documentation on the homepage
 * matches the actual default values in the MirDB source code.
 *
 * Test scenario: Verify configuration documentation matches current default values
 */

// Source code constants - extracted from mirdb-server/src/options.rs and config.rs
const SOURCE_CODE_CONFIG = {
  // From options.rs - Options::default() implementation
  defaults: {
    addr: '0.0.0.0:12333',      // Config default from config.rs test
    max_level: 7,               // Options::default() line 42
    work_dir: '/tmp/mirdb',     // Options::default() line 43
    sst_max_size: '100M',       // 100 * MB (line 44)
    mem_table_max_size: '4M',   // 4 * MB (line 45)
    mem_table_max_height: 32,   // 1 << 5 = 32 (line 46)
    imm_mem_table_max_count: 16, // 1 << 4 = 16 (line 47)
    block_size: '4K',           // 4 * KB from BLOCK_MAX_SIZE
    block_restart_interval: 16, // line 39
    l0_compaction_trigger: 4,   // line 49
    thread_sleep_ms: 500,       // line 51
  },
  // From config.rs - Config struct field names (TOML parameter names)
  parameterNames: [
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
  ],
};

test.describe('Content Accuracy - Configuration Values', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Verify documented memtable_size default matches implementation
   *
   * Steps:
   * 1. Navigate to the configuration section
   * 2. Find the memtable size parameter row
   * 3. Extract the documented default value
   * 4. Compare with actual default from source code (4M)
   */
  test('TC1: memtable_size default value matches source code', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Look for the config table
    const configTable = configSection.locator('[data-testid="config-table"], table.config-table');
    await expect(configTable).toBeVisible();

    // Find the memtable size parameter row
    // The actual TOML parameter name is "mem_table_max_size" per config.rs
    const memtableRow = configTable.locator('tbody tr').filter({
      has: page.locator('code').filter({ hasText: /mem_table_max_size|memtable.*size/i })
    });

    // Verify the parameter exists in documentation
    const rowExists = await memtableRow.count();
    expect(rowExists, 'Memtable size parameter should be documented').toBeGreaterThan(0);

    if (rowExists > 0) {
      // Get all cells in the row
      const cells = memtableRow.first().locator('td');
      const cellCount = await cells.count();
      expect(cellCount, 'Row should have at least 2 cells (name and default)').toBeGreaterThanOrEqual(2);

      // Get the parameter name from first cell
      const paramNameCell = cells.first();
      const paramName = await paramNameCell.textContent();

      // Verify parameter name matches TOML format from config.rs
      // Correct name is "mem_table_max_size" (with underscores)
      expect(
        paramName?.trim().toLowerCase(),
        'Parameter name should match TOML format from config.rs'
      ).toContain('mem_table_max_size');

      // Get the default value (second cell)
      const defaultCell = cells.nth(1);
      const defaultValue = await defaultCell.textContent();

      // Verify default value matches source code (4M or 4MB)
      // Source: options.rs line 45: mem_table_max_size: MB * 4
      expect(
        defaultValue?.trim().toUpperCase(),
        'Default value should be 4M (4 megabytes) per options.rs'
      ).toMatch(/4\s*M/i);
    }
  });

  /**
   * Test Case 2: Verify documented port default matches implementation
   *
   * Steps:
   * 1. Navigate to the configuration section
   * 2. Find the address/port parameter row
   * 3. Extract the documented default port
   * 4. Compare with actual default (12333 per config.rs test)
   *
   * Note: The test case mentions port 11211 (standard memcached), but
   * MirDB's actual default is 12333 per config.rs
   */
  test('TC2: port default value matches source code', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Look for the config table
    const configTable = configSection.locator('[data-testid="config-table"], table.config-table');
    await expect(configTable).toBeVisible();

    // Find the address parameter row (contains port)
    // The actual TOML parameter name is "addr" per config.rs
    const addrRow = configTable.locator('tbody tr').filter({
      has: page.locator('code').filter({ hasText: /^addr$|listen.*addr|address/i })
    });

    // Verify the parameter exists in documentation
    const rowExists = await addrRow.count();
    expect(rowExists, 'Address/port parameter should be documented').toBeGreaterThan(0);

    if (rowExists > 0) {
      // Get all cells in the row
      const cells = addrRow.first().locator('td');

      // Get the parameter name from first cell
      const paramNameCell = cells.first();
      const paramName = await paramNameCell.textContent();

      // Verify parameter name matches TOML format from config.rs
      // Correct name is "addr" (not "listen_addr")
      expect(
        paramName?.trim().toLowerCase(),
        'Parameter name should be "addr" per config.rs'
      ).toMatch(/^addr$/i);

      // Get the default value (second cell)
      const defaultCell = cells.nth(1);
      const defaultValue = await defaultCell.textContent();

      // Verify default port is 12333 (MirDB's actual default)
      // Source: config.rs test line 103: addr = "0.0.0.0:12333"
      expect(
        defaultValue,
        'Default port should be 12333 per config.rs'
      ).toContain('12333');
    }
  });

  /**
   * Test Case 3: Verify all documented parameters exist in implementation
   *
   * Steps:
   * 1. Navigate to the configuration section
   * 2. Extract all documented parameter names
   * 3. Compare with actual parameters from config.rs Config struct
   * 4. Verify parameter names match TOML format exactly
   */
  test('TC3: all documented parameters exist in implementation', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Look for the config table
    const configTable = configSection.locator('[data-testid="config-table"], table.config-table');
    await expect(configTable).toBeVisible();

    // Get all parameter names from the documentation
    const paramCells = configTable.locator('tbody tr td:first-child code');
    const paramCount = await paramCells.count();

    expect(paramCount, 'Documentation should list configuration parameters').toBeGreaterThan(0);

    // Collect all documented parameter names
    const documentedParams: string[] = [];
    for (let i = 0; i < paramCount; i++) {
      const paramName = await paramCells.nth(i).textContent();
      if (paramName) {
        documentedParams.push(paramName.trim().toLowerCase());
      }
    }

    // Verify each documented parameter exists in the source code config
    // Based on config.rs Config struct fields
    const sourceCodeParams = SOURCE_CODE_CONFIG.parameterNames.map(p => p.toLowerCase());

    for (const param of documentedParams) {
      // Check if the parameter exists in source code OR is a valid alias
      const exists = sourceCodeParams.some(srcParam =>
        srcParam === param ||
        srcParam.replace(/_/g, '') === param.replace(/_/g, '')
      );

      expect(
        exists,
        `Documented parameter "${param}" should exist in source code config.rs`
      ).toBe(true);
    }

    // Also verify that key parameters are documented
    const requiredParams = ['addr', 'work_dir', 'max_level', 'mem_table_max_size', 'sst_max_size'];
    for (const required of requiredParams) {
      const isDocumented = documentedParams.some(doc =>
        doc === required || doc.includes(required.replace(/_/g, ''))
      );
      expect(
        isDocumented,
        `Required parameter "${required}" should be documented`
      ).toBe(true);
    }
  });

  /**
   * Additional validation: Verify TOML example matches parameter names
   */
  test('TOML example uses correct parameter names', async ({ page }) => {
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    const tomlExample = configSection.locator('[data-testid="toml-example"]');
    const tomlExists = await tomlExample.count();

    if (tomlExists > 0) {
      const tomlContent = await tomlExample.textContent();

      // Verify TOML example uses correct parameter names from config.rs
      expect(tomlContent, 'TOML should use "addr" not "listen_addr"').toContain('addr');
      expect(tomlContent, 'TOML should use "max_level" not "max_levels"').toContain('max_level');
      expect(tomlContent, 'TOML should use "sst_max_size" not "sstable_max_size"').toContain('sst_max_size');
      expect(tomlContent, 'TOML should use "mem_table_max_size"').toContain('mem_table_max_size');
    }
  });

  /**
   * Validation: Default values in TOML example match source code
   */
  test('TOML example default values match source code', async ({ page }) => {
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    const tomlExample = configSection.locator('[data-testid="toml-example"]');
    const tomlExists = await tomlExample.count();

    if (tomlExists > 0) {
      const tomlContent = await tomlExample.textContent() || '';

      // Verify default values match source code
      expect(tomlContent).toContain('12333'); // Default port
      expect(tomlContent).toContain('/tmp/mirdb'); // Default work_dir
      expect(tomlContent).toMatch(/max_level\s*=\s*7/); // max_level = 7
      expect(tomlContent).toMatch(/sst_max_size\s*=\s*["']?100M/); // sst_max_size = "100M"
      expect(tomlContent).toMatch(/mem_table_max_size\s*=\s*["']?4M/); // mem_table_max_size = "4M"
    }
  });
});
