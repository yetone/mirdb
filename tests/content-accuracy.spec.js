// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Test Suite: Content Accuracy
 * Scenario: Verify all displayed content accurately reflects current MirDB capabilities
 *
 * This test suite validates that the homepage content matches the actual MirDB
 * implementation by cross-referencing documented features, commands, and
 * configuration values against the source code.
 *
 * Reference: MirDB source code analysis:
 * - parser.rs: Supported commands (SET, ADD, REPLACE, APPEND, PREPEND, GET, GETS, DELETE, INFO, MAJOR_COMPACTION)
 * - options.rs: Default configuration values
 * - config.rs: TOML configuration format
 * - data_manager.rs: LSM tree implementation with minor/major compaction
 */

test.describe('Content Accuracy - Supported Commands', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Review listed supported commands
   * Validates that all commands listed on the homepage are actually supported by MirDB
   * Expected: All listed commands (SET, GET, DELETE, etc.) are actually supported by MirDB
   */
  test('TC1: All listed commands are actually supported by MirDB', async ({ page }) => {
    // Navigate to commands section
    const commandsSection = page.locator('#commands');
    await commandsSection.scrollIntoViewIfNeeded();
    await expect(commandsSection).toBeVisible();

    // Verified supported commands from parser.rs:
    // - Storage: set, add, replace, append, prepend
    // - Retrieval: get, gets
    // - Deletion: delete
    // - MirDB-specific: info, major_compaction

    const supportedCommands = {
      storage: ['SET', 'ADD', 'REPLACE', 'APPEND', 'PREPEND'],
      retrieval: ['GET', 'GETS'],
      deletion: ['DELETE'],
      mirdb: ['INFO', 'MAJOR_COMPACTION']
    };

    // Verify storage commands
    const storageCategory = page.locator('[data-testid="commands-storage"]');
    await expect(storageCategory).toBeVisible();
    for (const cmd of supportedCommands.storage) {
      const commandElement = page.locator(`[data-testid="command-${cmd.toLowerCase()}"]`);
      await expect(commandElement).toBeVisible();
      const commandName = commandElement.locator('.command-name');
      await expect(commandName).toContainText(cmd);
    }

    // Verify retrieval commands
    const retrievalCategory = page.locator('[data-testid="commands-retrieval"]');
    await expect(retrievalCategory).toBeVisible();
    for (const cmd of supportedCommands.retrieval) {
      const commandElement = page.locator(`[data-testid="command-${cmd.toLowerCase()}"]`);
      await expect(commandElement).toBeVisible();
      const commandName = commandElement.locator('.command-name');
      await expect(commandName).toContainText(cmd);
    }

    // Verify deletion commands
    const deletionCategory = page.locator('[data-testid="commands-deletion"]');
    await expect(deletionCategory).toBeVisible();
    for (const cmd of supportedCommands.deletion) {
      const commandElement = page.locator(`[data-testid="command-${cmd.toLowerCase()}"]`);
      await expect(commandElement).toBeVisible();
      const commandName = commandElement.locator('.command-name');
      await expect(commandName).toContainText(cmd);
    }

    // Verify MirDB-specific commands
    const mirdbCategory = page.locator('[data-testid="commands-mirdb"]');
    await expect(mirdbCategory).toBeVisible();
    for (const cmd of supportedCommands.mirdb) {
      const commandElement = page.locator(`[data-testid="command-${cmd.toLowerCase()}"]`);
      await expect(commandElement).toBeVisible();
      const commandName = commandElement.locator('.command-name');
      await expect(commandName).toContainText(cmd);
    }
  });

  /**
   * Test: Command syntax in examples matches memcached protocol
   * Verifies that the command syntax examples are technically accurate
   */
  test('Command syntax in examples matches memcached protocol', async ({ page }) => {
    const commandsSection = page.locator('#commands');
    await commandsSection.scrollIntoViewIfNeeded();

    // Verify SET command syntax follows memcached protocol:
    // set <key> <flags> <ttl> <bytes> [noreply]\r\n<data>\r\n
    const setCommand = page.locator('[data-testid="command-set"]');
    const setExample = setCommand.locator('.command-example code');
    const setContent = await setExample.textContent();
    expect(setContent).toContain('<key>');
    expect(setContent).toContain('<flags>');
    expect(setContent).toContain('<ttl>');
    expect(setContent).toContain('<bytes>');
    expect(setContent).toContain('[noreply]');

    // Verify GET command syntax
    const getCommand = page.locator('[data-testid="command-get"]');
    const getExample = getCommand.locator('.command-example code');
    const getContent = await getExample.textContent();
    expect(getContent).toContain('<key');

    // Verify DELETE command syntax
    const deleteCommand = page.locator('[data-testid="command-delete"]');
    const deleteExample = deleteCommand.locator('.command-example code');
    const deleteContent = await deleteExample.textContent();
    expect(deleteContent).toContain('<key>');
    expect(deleteContent).toContain('[noreply]');
  });
});

test.describe('Content Accuracy - Default Configuration Values', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 2: Verify default configuration values
   * Validates that displayed defaults match actual MirDB defaults
   * Reference: options.rs default values
   */
  test('TC2: Displayed defaults match actual MirDB defaults', async ({ page }) => {
    const configSection = page.locator('#configuration');
    await configSection.scrollIntoViewIfNeeded();
    await expect(configSection).toBeVisible();

    // Verified default values from options.rs:
    // - addr: "0.0.0.0:12333" (from config examples)
    // - max_level: 7
    // - work_dir: "/tmp/mirdb"
    // - sst_max_size: 100MB
    // - mem_table_max_size: 4MB
    // - block_size: 4KB

    const configTable = configSection.locator('.config-params-table');
    await expect(configTable).toBeVisible();

    // Verify addr default (port 12333)
    const addrRow = configTable.locator('[data-param="addr"]');
    await expect(addrRow).toBeVisible();
    await expect(addrRow.locator('.param-default')).toContainText('0.0.0.0:12333');

    // Verify max_level default (7)
    const maxLevelRow = configTable.locator('[data-param="max_level"]');
    await expect(maxLevelRow).toBeVisible();
    await expect(maxLevelRow.locator('.param-default')).toContainText('7');

    // Verify work_dir default (/tmp/mirdb)
    const workDirRow = configTable.locator('[data-param="work_dir"]');
    await expect(workDirRow).toBeVisible();
    await expect(workDirRow.locator('.param-default')).toContainText('/tmp/mirdb');

    // Verify sst_max_size default (100M)
    const sstMaxSizeRow = configTable.locator('[data-param="sst_max_size"]');
    await expect(sstMaxSizeRow).toBeVisible();
    await expect(sstMaxSizeRow.locator('.param-default')).toContainText('100M');

    // Verify mem_table_max_size default (4M)
    const memTableRow = configTable.locator('[data-param="mem_table_max_size"]');
    await expect(memTableRow).toBeVisible();
    await expect(memTableRow.locator('.param-default')).toContainText('4M');

    // Verify block_size default (4K)
    const blockSizeRow = configTable.locator('[data-param="block_size"]');
    await expect(blockSizeRow).toBeVisible();
    await expect(blockSizeRow.locator('.param-default')).toContainText('4K');
  });

  /**
   * Test: Default port 12333 is correctly documented throughout
   */
  test('Default port 12333 is consistently documented', async ({ page }) => {
    // Check in configuration section
    const configSection = page.locator('#configuration');
    await configSection.scrollIntoViewIfNeeded();
    const addrRow = configSection.locator('[data-param="addr"]');
    await expect(addrRow.locator('.param-default')).toContainText('12333');

    // Check in quick-start section (telnet example)
    const quickStartSection = page.locator('#quick-start');
    await quickStartSection.scrollIntoViewIfNeeded();
    const connectExample = page.locator('#connect-example');
    const connectContent = await connectExample.textContent();
    expect(connectContent).toContain('12333');

    // Check in configuration TOML example
    const tomlCodeBlock = configSection.locator('.code-block[data-language="toml"] code');
    const tomlContent = await tomlCodeBlock.textContent();
    expect(tomlContent).toContain('12333');
  });
});

test.describe('Content Accuracy - Feature Descriptions', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 3: Check feature descriptions accuracy
   * Validates that LSM Tree, Tokio async, compaction descriptions are technically accurate
   */
  test('TC3: LSM Tree description is technically accurate', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();

    // Verify LSM Tree feature describes actual MirDB implementation
    // Reference: data_manager.rs uses memtables, immutable memtables, and multi-level SSTable compaction
    const lsmTreeFeature = page.locator('[data-feature="lsm-tree"]');
    await expect(lsmTreeFeature).toBeVisible();

    const lsmDescription = lsmTreeFeature.locator('p');
    const lsmContent = await lsmDescription.textContent();

    // Must mention key LSM tree components that MirDB actually implements
    expect(lsmContent.toLowerCase()).toContain('log-structured merge-tree');
    expect(lsmContent.toLowerCase()).toContain('memtable');
    expect(lsmContent.toLowerCase()).toContain('sstable');
    expect(lsmContent.toLowerCase()).toContain('compaction');
  });

  /**
   * Test: Tokio async description is technically accurate
   */
  test('TC3b: Tokio async description is technically accurate', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();

    // Verify Async I/O feature describes actual MirDB implementation
    // Reference: main.rs uses tokio-proto and tokio-service for async networking
    const asyncFeature = page.locator('[data-feature="async-io"]');
    await expect(asyncFeature).toBeVisible();

    const asyncDescription = asyncFeature.locator('p');
    const asyncContent = await asyncDescription.textContent();

    // Must mention Tokio which MirDB actually uses
    expect(asyncContent).toContain('Tokio');
    expect(asyncContent.toLowerCase()).toContain('async');
  });

  /**
   * Test: Compaction description is technically accurate
   */
  test('TC3c: Compaction description is technically accurate', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();

    // Verify Compaction feature describes actual MirDB implementation
    // Reference: data_manager.rs implements both minor and major compaction
    const compactionFeature = page.locator('[data-feature="compaction"]');
    await expect(compactionFeature).toBeVisible();

    const compactionDescription = compactionFeature.locator('p');
    const compactionContent = await compactionDescription.textContent();

    // Must mention both types of compaction that MirDB implements
    expect(compactionContent.toLowerCase()).toContain('compaction');
    expect(compactionContent.toLowerCase()).toContain('memtable');
    expect(compactionContent.toLowerCase()).toContain('sstable');
  });

  /**
   * Test: Value propositions are accurate
   */
  test('Value propositions accurately reflect MirDB capabilities', async ({ page }) => {
    const valuePropsSection = page.locator('[data-testid="value-propositions-section"]');
    await valuePropsSection.scrollIntoViewIfNeeded();
    await expect(valuePropsSection).toBeVisible();

    // Verify Persistent Storage claim
    const persistentStorage = page.locator('[data-testid="value-prop-persistent-storage"]');
    await expect(persistentStorage).toBeVisible();
    const persistentDesc = persistentStorage.locator('[data-testid="value-prop-persistent-storage-description"]');
    const persistentContent = await persistentDesc.textContent();
    // MirDB does persist data using LSM tree and SSTables
    expect(persistentContent.toLowerCase()).toContain('persist');
    expect(persistentContent.toLowerCase()).toContain('disk');

    // Verify Memcached Compatible claim
    const memcachedCompat = page.locator('[data-testid="value-prop-memcached-compatible"]');
    await expect(memcachedCompat).toBeVisible();
    const memcachedDesc = memcachedCompat.locator('[data-testid="value-prop-memcached-compatible-description"]');
    const memcachedContent = await memcachedDesc.textContent();
    // MirDB implements memcached text protocol
    expect(memcachedContent.toLowerCase()).toContain('memcached');
    expect(memcachedContent.toLowerCase()).toContain('protocol');

    // Verify Written in Rust claim
    const rustFeature = page.locator('[data-testid="value-prop-rust"]');
    await expect(rustFeature).toBeVisible();
    const rustDesc = rustFeature.locator('[data-testid="value-prop-rust-description"]');
    const rustContent = await rustDesc.textContent();
    // MirDB is written in Rust and uses Tokio
    expect(rustContent).toContain('Rust');
    expect(rustContent).toContain('Tokio');
  });
});

test.describe('Content Accuracy - Project Status', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 4: Verify project status section
   * Validates that implemented vs planned features accurately reflect repository state
   *
   * Based on knowledge files analysis:
   * Implemented:
   * - Tokio-based async networking with memcached protocol
   * - Memtable with skip list data structure
   * - Minor compaction (memtable to SSTable)
   * - Major compaction (SSTable level compaction)
   *
   * Planned but not yet implemented:
   * - Raft consensus for distributed operation
   */
  test('TC4: Homepage content reflects implemented features accurately', async ({ page }) => {
    // The homepage doesn't have a dedicated "project status" section,
    // but the features and claims should only reflect what's implemented

    // Verify no claims about distributed/Raft functionality
    // (which is planned but not implemented)
    const pageContent = await page.content();

    // Should NOT claim distributed/Raft capabilities (not yet implemented)
    const lowerContent = pageContent.toLowerCase();
    const hasDistributedClaim = lowerContent.includes('distributed') &&
                                 lowerContent.includes('raft');
    expect(hasDistributedClaim).toBe(false);

    // Should accurately claim implemented features
    expect(lowerContent).toContain('persistent');
    expect(lowerContent).toContain('memcached');
    expect(lowerContent).toContain('lsm');
    expect(lowerContent).toContain('tokio');
    expect(lowerContent).toContain('compaction');
    // Note: "skip list" is an internal implementation detail of the memtable
    // and is correctly documented in the knowledge base, but not required
    // to be exposed on the public homepage
  });

  /**
   * Test: Quick-start guide code examples are functional
   * Verifies that the code examples use correct syntax and commands
   */
  test('Quick-start guide code examples use correct syntax', async ({ page }) => {
    const quickStartSection = page.locator('#quick-start');
    await quickStartSection.scrollIntoViewIfNeeded();
    await expect(quickStartSection).toBeVisible();

    // Verify build commands use correct cargo syntax
    const buildCommands = page.locator('#build-commands');
    const buildContent = await buildCommands.textContent();
    expect(buildContent).toContain('git clone');
    expect(buildContent).toContain('cargo build --release');

    // Verify run command syntax
    const runCommand = page.locator('#run-command');
    const runContent = await runCommand.textContent();
    expect(runContent).toContain('./target/release/mirdb');

    // Verify usage example uses correct memcached protocol syntax
    const usageExample = page.locator('#usage-example');
    const usageContent = await usageExample.textContent();

    // SET command should have correct format: set <key> <flags> <ttl> <bytes>
    expect(usageContent).toContain('set hello 0 0 5');
    expect(usageContent).toContain('world');
    expect(usageContent).toContain('STORED');

    // GET command should show correct response format
    expect(usageContent).toContain('get hello');
    expect(usageContent).toContain('VALUE hello 0 5');
    expect(usageContent).toContain('END');
  });

  /**
   * Test: TOML configuration example is valid and accurate
   */
  test('Configuration TOML example is valid', async ({ page }) => {
    const configSection = page.locator('#configuration');
    await configSection.scrollIntoViewIfNeeded();

    const tomlCodeBlock = configSection.locator('.code-block[data-language="toml"] code');
    const tomlContent = await tomlCodeBlock.textContent();

    // Verify TOML syntax (key = value pairs)
    // All these parameters exist in MirDB's config.rs
    expect(tomlContent).toMatch(/addr\s*=\s*["']0\.0\.0\.0:12333["']/);
    expect(tomlContent).toMatch(/max_level\s*=\s*7/);
    expect(tomlContent).toMatch(/work_dir\s*=\s*["']\/tmp\/mirdb["']/);
    expect(tomlContent).toMatch(/sst_max_size\s*=\s*["']100M["']/);
    expect(tomlContent).toMatch(/mem_table_max_size\s*=\s*["']4M["']/);
    expect(tomlContent).toMatch(/block_size\s*=\s*["']4K["']/);
  });
});
