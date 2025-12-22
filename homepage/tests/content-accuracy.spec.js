// @ts-check
const { test, expect } = require('@playwright/test');
const fs = require('fs');
const path = require('path');

/**
 * Content Accuracy Tests
 *
 * Scenario: Verify homepage content accurately represents current MirDB capabilities
 *
 * This test suite validates that displayed configuration values match the actual
 * defaults in the MirDB codebase (options.rs, mirdb.toml, etc.)
 *
 * Source of truth:
 * - Port: 12333 (from mirdb-server/src/options.rs and etc/mirdb.toml)
 * - Work Directory: /tmp/mirdb (from options.rs line 43)
 * - SSTable Max Size: 100MB (from options.rs line 44: MB * 100)
 * - Memtable Max Size: 4MB (from options.rs line 45: MB * 4)
 * - Block Size: 4KB (from options.rs line 9: 4 * KB, line 38)
 * - Max LSM Levels: 7 (from options.rs line 42)
 */

// Actual default values from the MirDB source code
const MIRDB_DEFAULTS = {
  port: '12333',
  listenAddress: '0.0.0.0:12333',
  workDirectory: '/tmp/mirdb',
  sstableMaxSize: '100MB',
  memtableMaxSize: '4MB',
  blockSize: '4KB',
  maxLsmLevels: '7',
};

test.describe('Content Accuracy - Configuration Values', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Verify default port displayed
   * Expected: Displayed port (12333) matches actual default in codebase
   *
   * Source: mirdb-server/src/options.rs - addr in mirdb.toml: "0.0.0.0:12333"
   */
  test('should display correct default port (12333)', async ({ page }) => {
    const quickstartSection = page.locator('[data-testid="quickstart-section"], #quickstart');
    await quickstartSection.scrollIntoViewIfNeeded();
    await expect(quickstartSection).toBeVisible();

    // Check config section for port value
    const configSection = page.locator('[data-testid="config-section"], .config-section');
    await expect(configSection).toBeVisible();

    const configText = await configSection.textContent();

    // Verify the displayed port matches the actual default
    expect(configText).toContain(MIRDB_DEFAULTS.port);

    // Check Listen Address specifically shows the correct value
    const listenAddressItem = configSection.locator('.config-item:has-text("Listen Address")');
    await expect(listenAddressItem).toBeVisible();
    const listenAddressValue = await listenAddressItem.locator('.config-value').textContent();
    expect(listenAddressValue.trim()).toBe(MIRDB_DEFAULTS.listenAddress);
  });

  /**
   * Test Case 2: Verify default work directory
   * Expected: Displayed work directory (/tmp/mirdb) matches actual default
   *
   * Source: mirdb-server/src/options.rs line 43: work_dir: "/tmp/mirdb".into()
   */
  test('should display correct default work directory (/tmp/mirdb)', async ({ page }) => {
    const configSection = page.locator('[data-testid="config-section"], .config-section');
    await configSection.scrollIntoViewIfNeeded();
    await expect(configSection).toBeVisible();

    // Check Work Directory config item
    const workDirItem = configSection.locator('.config-item:has-text("Work Directory")');
    await expect(workDirItem).toBeVisible();

    const workDirValue = await workDirItem.locator('.config-value').textContent();
    expect(workDirValue.trim()).toBe(MIRDB_DEFAULTS.workDirectory);
  });

  /**
   * Test Case 3: Verify SSTable max size
   * Expected: Displayed SSTable max size (100MB) matches actual default
   *
   * Source: mirdb-server/src/options.rs line 44: sst_max_size: MB * 100
   *         etc/mirdb.toml: sst_max_size = "100M"
   */
  test('should display correct SSTable max size (100MB)', async ({ page }) => {
    const configSection = page.locator('[data-testid="config-section"], .config-section');
    await configSection.scrollIntoViewIfNeeded();
    await expect(configSection).toBeVisible();

    // Check SSTable Size config item
    const sstableItem = configSection.locator('.config-item:has-text("SSTable")');
    await expect(sstableItem).toBeVisible();

    const sstableValue = await sstableItem.locator('.config-value').textContent();
    expect(sstableValue.trim()).toBe(MIRDB_DEFAULTS.sstableMaxSize);
  });

  /**
   * Test Case 4: Verify memtable max size
   * Expected: Displayed memtable max size (4MB) matches actual default
   *
   * Source: mirdb-server/src/options.rs line 45: mem_table_max_size: MB * 4
   *         etc/mirdb.toml: mem_table_max_size = "4M"
   */
  test('should display correct memtable max size (4MB)', async ({ page }) => {
    const configSection = page.locator('[data-testid="config-section"], .config-section');
    await configSection.scrollIntoViewIfNeeded();
    await expect(configSection).toBeVisible();

    // Check Memtable Size config item
    const memtableItem = configSection.locator('.config-item:has-text("Memtable")');
    await expect(memtableItem).toBeVisible();

    const memtableValue = await memtableItem.locator('.config-value').textContent();
    expect(memtableValue.trim()).toBe(MIRDB_DEFAULTS.memtableMaxSize);
  });

  /**
   * Additional test: Verify Block Size (4KB)
   *
   * Source: mirdb-server/src/options.rs line 9: const BLOCK_MAX_SIZE: usize = 4 * KB
   *         etc/mirdb.toml: block_size = "4K"
   */
  test('should display correct block size (4KB)', async ({ page }) => {
    const configSection = page.locator('[data-testid="config-section"], .config-section');
    await configSection.scrollIntoViewIfNeeded();
    await expect(configSection).toBeVisible();

    // Check Block Size config item
    const blockSizeItem = configSection.locator('.config-item:has-text("Block Size")');
    await expect(blockSizeItem).toBeVisible();

    const blockSizeValue = await blockSizeItem.locator('.config-value').textContent();
    expect(blockSizeValue.trim()).toBe(MIRDB_DEFAULTS.blockSize);
  });

  /**
   * Additional test: Verify Max LSM Levels (7)
   *
   * Source: mirdb-server/src/options.rs line 42: max_level: 7
   *         etc/mirdb.toml: max_level = 7
   */
  test('should display correct max LSM levels (7)', async ({ page }) => {
    const configSection = page.locator('[data-testid="config-section"], .config-section');
    await configSection.scrollIntoViewIfNeeded();
    await expect(configSection).toBeVisible();

    // Check Max LSM Levels config item
    const lsmLevelsItem = configSection.locator('.config-item:has-text("LSM Levels")');
    await expect(lsmLevelsItem).toBeVisible();

    const lsmLevelsValue = await lsmLevelsItem.locator('.config-value').textContent();
    expect(lsmLevelsValue.trim()).toBe(MIRDB_DEFAULTS.maxLsmLevels);
  });
});

test.describe('Content Accuracy - Implemented Features', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 5: Verify implemented features list accuracy
   * Expected: All listed implemented features are actually implemented
   *
   * Source: Knowledge file b3a7fcb8-a652-48c7-a55a-6638cd2d1b5c - Project Status
   *
   * Implemented features:
   * - Tokio-based async networking with memcached protocol
   * - Memtable with skip list data structure
   * - Minor compaction (memtable to SSTable)
   * - Major compaction (SSTable level compaction)
   */
  test('should list all actually implemented features', async ({ page }) => {
    const statusSection = page.locator('[data-testid="status-section"], #status');
    await statusSection.scrollIntoViewIfNeeded();
    await expect(statusSection).toBeVisible();

    const implementedSection = page.locator('[data-testid="implemented-features"], .status-implemented');
    await expect(implementedSection).toBeVisible();

    const implementedText = await implementedSection.textContent();

    // Verify each implemented feature is listed (based on knowledge docs)
    // 1. Tokio-based async networking
    expect(implementedText.toLowerCase()).toMatch(/tokio.*async|async.*networking|tokio-based/i);

    // 2. Memtable with skip list
    expect(implementedText.toLowerCase()).toMatch(/memtable.*skip\s*list|skip\s*list.*data\s*structure/i);

    // 3. Minor compaction
    expect(implementedText.toLowerCase()).toMatch(/minor\s*compaction/i);

    // 4. Major compaction
    expect(implementedText.toLowerCase()).toMatch(/major\s*compaction/i);
  });

  /**
   * Verify we're not claiming features that aren't implemented
   * Raft consensus should NOT be in implemented section
   */
  test('should not list unimplemented features as implemented', async ({ page }) => {
    const implementedSection = page.locator('[data-testid="implemented-features"], .status-implemented');
    await implementedSection.scrollIntoViewIfNeeded();
    await expect(implementedSection).toBeVisible();

    const implementedText = await implementedSection.textContent();

    // Raft consensus should NOT be in implemented features
    expect(implementedText.toLowerCase()).not.toMatch(/raft\s*consensus/i);
  });
});

test.describe('Content Accuracy - Planned Features', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 6: Verify planned features accuracy
   * Expected: Planned features (Raft consensus) accurately reflect roadmap
   *
   * Source: Knowledge file b3a7fcb8-a652-48c7-a55a-6638cd2d1b5c - Project Status
   *
   * Planned but not yet implemented:
   * - Raft consensus for distributed operation
   */
  test('should list Raft consensus as planned feature', async ({ page }) => {
    const statusSection = page.locator('[data-testid="status-section"], #status');
    await statusSection.scrollIntoViewIfNeeded();
    await expect(statusSection).toBeVisible();

    const plannedSection = page.locator('[data-testid="planned-features"], .status-planned');
    await expect(plannedSection).toBeVisible();

    const plannedText = await plannedSection.textContent();

    // Verify Raft consensus is listed as planned
    expect(plannedText.toLowerCase()).toMatch(/raft.*consensus|raft/i);

    // Verify it mentions distributed operation
    expect(plannedText.toLowerCase()).toMatch(/distributed/i);
  });

  /**
   * Verify planned features are not listed as implemented
   */
  test('should not list Raft as implemented', async ({ page }) => {
    const implementedSection = page.locator('[data-testid="implemented-features"], .status-implemented');
    await implementedSection.scrollIntoViewIfNeeded();

    const implementedText = await implementedSection.textContent();

    // Raft should not be in implemented section
    expect(implementedText.toLowerCase()).not.toContain('raft');
  });
});

test.describe('Content Accuracy - Code Examples', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Verify code examples use correct default port
   */
  test('should show correct port in code examples', async ({ page }) => {
    const quickstartSection = page.locator('[data-testid="quickstart-section"], #quickstart');
    await quickstartSection.scrollIntoViewIfNeeded();

    // Check Python client example
    const clientCodeBlock = page.locator('[data-testid="code-block-client"]');
    await expect(clientCodeBlock).toBeVisible();

    const codeContent = await clientCodeBlock.textContent();

    // Verify the correct port is used in the example
    expect(codeContent).toContain(MIRDB_DEFAULTS.port);
  });

  /**
   * Verify code comments mention correct default address
   */
  test('should mention correct default address in comments', async ({ page }) => {
    const quickstartSection = page.locator('[data-testid="quickstart-section"], #quickstart');
    await quickstartSection.scrollIntoViewIfNeeded();

    const sectionText = await quickstartSection.textContent();

    // Should mention the default address somewhere
    expect(sectionText).toMatch(/0\.0\.0\.0:12333|localhost:12333/);
  });
});
