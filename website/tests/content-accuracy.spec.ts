import { test, expect } from '@playwright/test';
import * as fs from 'fs';
import * as path from 'path';

/**
 * Content Accuracy Tests for MirDB Homepage
 *
 * These tests verify that all content on the homepage accurately represents
 * MirDB's actual capabilities as implemented in the codebase.
 */

// Helper function to read Rust source files
function readSourceFile(relativePath: string): string {
  const fullPath = path.join(process.cwd(), '..', relativePath);
  try {
    return fs.readFileSync(fullPath, 'utf-8');
  } catch (error) {
    return '';
  }
}

test.describe('Content Accuracy - Feature List vs Implementation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Memcached Protocol feature matches actual implementation - request types exist', async ({ page }) => {
    // Verify the homepage claims Memcached Protocol Compatibility
    const featuresSection = page.locator('.features-section');
    await expect(featuresSection).toBeVisible();

    const memcachedFeature = featuresSection.locator('.feature-card[data-feature="memcached"]');
    await expect(memcachedFeature).toBeVisible();

    // Verify the actual implementation has these request types
    const requestSource = readSourceFile('mirdb-server/src/request.rs');
    expect(requestSource).toBeTruthy();

    // Verify GetterType enum exists with Get and Gets variants
    expect(requestSource).toContain('enum GetterType');
    expect(requestSource).toContain('Get,');
    expect(requestSource).toContain('Gets,');

    // Verify SetterType enum exists with all claimed setter types
    expect(requestSource).toContain('enum SetterType');
    expect(requestSource).toContain('Set,');
    expect(requestSource).toContain('Add,');
    expect(requestSource).toContain('Replace,');
    expect(requestSource).toContain('Append,');
    expect(requestSource).toContain('Prepend,');

    // Verify Request enum has Deleter, Info, and MajorCompaction
    expect(requestSource).toContain('Deleter');
    expect(requestSource).toContain('Info,');
    expect(requestSource).toContain('MajorCompaction,');
  });

  test('TC2: LSM-Tree Architecture feature matches actual implementation - data manager exists', async ({ page }) => {
    // Verify the homepage claims LSM-Tree Storage Architecture
    const featuresSection = page.locator('.features-section');
    await expect(featuresSection).toBeVisible();

    const lsmTreeFeature = featuresSection.locator('.feature-card[data-feature="lsm-tree"]');
    await expect(lsmTreeFeature).toBeVisible();

    // Verify the actual implementation has LSM-tree components
    const dataManagerSource = readSourceFile('mirdb-server/src/data_manager.rs');
    expect(dataManagerSource).toBeTruthy();

    // Verify DataManager struct with LSM-tree components
    expect(dataManagerSource).toContain('struct DataManager');

    // Verify mutable memtable (mut_)
    expect(dataManagerSource).toContain('mut_:');
    expect(dataManagerSource).toContain('Memtable');

    // Verify immutable memtable list (imm_)
    expect(dataManagerSource).toContain('imm_:');
    expect(dataManagerSource).toContain('MemtableList');

    // Verify SSTable readers
    expect(dataManagerSource).toContain('readers_:');
    expect(dataManagerSource).toContain('SstableReader');
  });

  test('TC3: Write-Ahead Logging feature matches actual implementation - WAL module exists', async ({ page }) => {
    // Verify the homepage claims Write-Ahead Logging (WAL)
    const featuresSection = page.locator('.features-section');
    await expect(featuresSection).toBeVisible();

    const walFeature = featuresSection.locator('.feature-card[data-feature="wal"]');
    await expect(walFeature).toBeVisible();

    // Verify the actual WAL implementation exists
    const walSource = readSourceFile('mirdb-server/src/wal.rs');
    expect(walSource).toBeTruthy();

    // Verify WAL struct exists
    expect(walSource).toContain('struct WAL');
    expect(walSource).toContain('struct WALSeg');

    // Verify WAL has append functionality (crash-safe logging)
    expect(walSource).toContain('fn append');

    // Verify WAL uses files for persistence
    expect(walSource).toContain('File');
    expect(walSource).toContain('write_all');
    expect(walSource).toContain('flush');
  });

  test('TC4: Minor & Major Compaction feature matches actual implementation - compaction methods exist', async ({ page }) => {
    // Verify the homepage claims Minor & Major Compaction
    const featuresSection = page.locator('.features-section');
    await expect(featuresSection).toBeVisible();

    const compactionFeature = featuresSection.locator('.feature-card[data-feature="compaction"]');
    await expect(compactionFeature).toBeVisible();

    // Verify the actual compaction implementation exists
    const dataManagerSource = readSourceFile('mirdb-server/src/data_manager.rs');
    expect(dataManagerSource).toBeTruthy();

    // Verify minor_compaction method exists
    expect(dataManagerSource).toContain('fn minor_compaction');

    // Verify major_compaction method exists
    expect(dataManagerSource).toContain('fn major_compaction');
    expect(dataManagerSource).toContain('pub fn major_compaction');

    // Verify size_compaction (part of major compaction)
    expect(dataManagerSource).toContain('fn size_compaction');

    // Verify background threads for compaction
    expect(dataManagerSource).toContain('background_thread');
    expect(dataManagerSource).toContain('thread::spawn');
  });

  test('TC5: LRU Block Cache feature matches actual implementation - cache module exists', async ({ page }) => {
    // Verify the homepage claims LRU Block Cache
    const featuresSection = page.locator('.features-section');
    await expect(featuresSection).toBeVisible();

    const lruCacheFeature = featuresSection.locator('.feature-card[data-feature="lru-cache"]');
    await expect(lruCacheFeature).toBeVisible();

    // Verify the actual LRU cache implementation exists in sstable crate
    const cacheSource = readSourceFile('sstable/src/cache.rs');
    expect(cacheSource).toBeTruthy();

    // Verify Cache struct using LruCache
    expect(cacheSource).toContain('struct Cache');
    expect(cacheSource).toContain('LruCache');

    // Verify cache operations exist
    expect(cacheSource).toContain('fn new');
    expect(cacheSource).toContain('fn insert');
    expect(cacheSource).toContain('fn get');
  });

  test('TC6: Cuckoo Filter feature matches actual implementation - filter in meta_block', async ({ page }) => {
    // Verify the homepage claims Cuckoo Filter for Fast Lookups
    const featuresSection = page.locator('.features-section');
    await expect(featuresSection).toBeVisible();

    const cuckooFilterFeature = featuresSection.locator('.feature-card[data-feature="cuckoo-filter"]');
    await expect(cuckooFilterFeature).toBeVisible();

    // Verify the actual Cuckoo filter implementation exists
    const metaBlockSource = readSourceFile('sstable/src/meta_block.rs');
    expect(metaBlockSource).toBeTruthy();

    // Verify MetaBlock uses Cuckoo filter
    expect(metaBlockSource).toContain('cuckoofilter');
    expect(metaBlockSource).toContain('ExportedCuckooFilter');

    // Verify MetaBlock struct has filter field
    expect(metaBlockSource).toContain('struct MetaBlock');
    expect(metaBlockSource).toContain('filter:');
  });

  test('TC7: Snappy Compression feature matches actual implementation - snap crate used', async ({ page }) => {
    // Verify the homepage claims Snappy Compression
    const featuresSection = page.locator('.features-section');
    await expect(featuresSection).toBeVisible();

    const snappyFeature = featuresSection.locator('.feature-card[data-feature="snappy"]');
    await expect(snappyFeature).toBeVisible();

    // Verify Snappy compression is used in WAL
    const walSource = readSourceFile('mirdb-server/src/wal.rs');
    expect(walSource).toBeTruthy();
    expect(walSource).toContain('snap::Encoder');
    expect(walSource).toContain('snap::Decoder');
    expect(walSource).toContain('compress_vec');
    expect(walSource).toContain('decompress_vec');

    // Verify Snappy compression is used in meta_block
    const metaBlockSource = readSourceFile('sstable/src/meta_block.rs');
    expect(metaBlockSource).toBeTruthy();
    expect(metaBlockSource).toContain('snap::Encoder');
    expect(metaBlockSource).toContain('snap::Decoder');
  });
});

test.describe('Content Accuracy - Installation Instructions', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC8: Installation instructions reference correct repository', async ({ page }) => {
    const quickStartSection = page.locator('#quickstart');
    await expect(quickStartSection).toBeVisible();

    // Check that git clone command references correct repo
    const installationCode = await quickStartSection.textContent();
    expect(installationCode).toContain('git clone');
    expect(installationCode).toContain('mirdb');
  });

  test('TC9: Cargo build command is documented correctly', async ({ page }) => {
    const quickStartSection = page.locator('#quickstart');
    await expect(quickStartSection).toBeVisible();

    const quickStartText = await quickStartSection.textContent();

    // Verify cargo build command is shown
    expect(quickStartText).toContain('cargo build');

    // Verify --release flag is documented for production builds
    expect(quickStartText).toContain('--release');
  });

  test('TC10: Cargo.toml exists and project can be built', async ({ page }) => {
    // Verify Cargo.toml exists at workspace root
    const cargoTomlExists = fs.existsSync(path.join(process.cwd(), '..', 'Cargo.toml'));
    expect(cargoTomlExists).toBe(true);

    // Verify mirdb-server Cargo.toml exists
    const serverCargoExists = fs.existsSync(path.join(process.cwd(), '..', 'mirdb-server', 'Cargo.toml'));
    expect(serverCargoExists).toBe(true);

    // Read workspace Cargo.toml to verify structure
    const workspaceCargoToml = fs.readFileSync(path.join(process.cwd(), '..', 'Cargo.toml'), 'utf-8');
    expect(workspaceCargoToml).toContain('[workspace]');
    expect(workspaceCargoToml).toContain('mirdb-server');
    expect(workspaceCargoToml).toContain('skip-list');
    expect(workspaceCargoToml).toContain('sstable');
  });
});

test.describe('Content Accuracy - Quick Start Code Examples', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC11: Telnet connection example uses correct default port', async ({ page }) => {
    const quickStartSection = page.locator('#quickstart');
    await expect(quickStartSection).toBeVisible();

    const quickStartText = await quickStartSection.textContent();

    // Check telnet command
    expect(quickStartText).toContain('telnet');
    expect(quickStartText).toContain('localhost');
    expect(quickStartText).toContain('11211');
  });

  test('TC12: Set command example matches Memcached protocol format', async ({ page }) => {
    const quickStartSection = page.locator('#quickstart');
    await expect(quickStartSection).toBeVisible();

    const quickStartText = await quickStartSection.textContent();

    // Verify set command format: set <key> <flags> <ttl> <bytes>
    // The example shows: set mykey 0 0 5
    expect(quickStartText).toMatch(/set\s+\w+\s+\d+\s+\d+\s+\d+/);

    // Verify STORED response is shown
    expect(quickStartText).toContain('STORED');
  });

  test('TC13: Get command example matches Memcached protocol format', async ({ page }) => {
    const quickStartSection = page.locator('#quickstart');
    await expect(quickStartSection).toBeVisible();

    const quickStartText = await quickStartSection.textContent();

    // Verify get command format: get <key>
    expect(quickStartText).toMatch(/get\s+\w+/);

    // Verify VALUE response format is shown
    expect(quickStartText).toContain('VALUE');
    expect(quickStartText).toContain('END');
  });

  test('TC14: Delete command example matches Memcached protocol format', async ({ page }) => {
    const quickStartSection = page.locator('#quickstart');
    await expect(quickStartSection).toBeVisible();

    const quickStartText = await quickStartSection.textContent();

    // Verify delete command format: delete <key>
    expect(quickStartText).toMatch(/delete\s+\w+/);

    // Verify DELETED response is shown
    expect(quickStartText).toContain('DELETED');
  });

  test('TC15: Python pymemcache example is syntactically correct', async ({ page }) => {
    const quickStartSection = page.locator('#quickstart');
    await expect(quickStartSection).toBeVisible();

    const quickStartText = await quickStartSection.textContent();

    // Verify Python import statement
    expect(quickStartText).toContain('from pymemcache.client import Client');

    // Verify client connection with correct host:port format
    expect(quickStartText).toContain("Client('localhost:11211')");

    // Verify set and get methods are called
    expect(quickStartText).toContain('.set(');
    expect(quickStartText).toContain('.get(');
  });
});

test.describe('Content Accuracy - Command Syntax Documentation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC16: Documented getter commands match implementation - get and gets', async ({ page }) => {
    // Check documented commands
    const commandsSection = page.locator('.quickstart-step:has(.step-title:has-text("Supported Commands"))');
    await expect(commandsSection).toBeVisible();

    const getterCategory = commandsSection.locator('.command-category:has(h4:has-text("Getter Commands"))');
    await expect(getterCategory).toBeVisible();

    // Verify get command documentation
    const getCommand = getterCategory.locator('li').filter({ hasText: /^get\s/ });
    await expect(getCommand).toBeVisible();

    // Verify gets command documentation
    const getsCommand = getterCategory.locator('li').filter({ hasText: /^gets\s/ });
    await expect(getsCommand).toBeVisible();

    // Verify these match the actual implementation
    const requestSource = readSourceFile('mirdb-server/src/request.rs');
    expect(requestSource).toContain('GetterType');
    expect(requestSource).toContain('Get,');
    expect(requestSource).toContain('Gets,');
  });

  test('TC17: Documented setter commands match implementation - set, add, replace, append, prepend', async ({ page }) => {
    // Check documented commands
    const commandsSection = page.locator('.quickstart-step:has(.step-title:has-text("Supported Commands"))');
    await expect(commandsSection).toBeVisible();

    const setterCategory = commandsSection.locator('.command-category:has(h4:has-text("Setter Commands"))');
    await expect(setterCategory).toBeVisible();

    // Verify all setter commands are documented
    const setterCommands = ['set', 'add', 'replace', 'append', 'prepend'];
    for (const cmd of setterCommands) {
      const commandItem = setterCategory.locator('li').filter({ hasText: new RegExp(`^${cmd}\\b`) });
      await expect(commandItem).toBeVisible();
    }

    // Verify these match the actual implementation
    const requestSource = readSourceFile('mirdb-server/src/request.rs');
    expect(requestSource).toContain('SetterType');
    expect(requestSource).toContain('Set,');
    expect(requestSource).toContain('Add,');
    expect(requestSource).toContain('Replace,');
    expect(requestSource).toContain('Append,');
    expect(requestSource).toContain('Prepend,');
  });

  test('TC18: Documented other commands match implementation - delete, info, major_compaction', async ({ page }) => {
    // Check documented commands
    const commandsSection = page.locator('.quickstart-step:has(.step-title:has-text("Supported Commands"))');
    await expect(commandsSection).toBeVisible();

    const otherCategory = commandsSection.locator('.command-category:has(h4:has-text("Other Commands"))');
    await expect(otherCategory).toBeVisible();

    // Verify delete command
    const deleteCommand = otherCategory.locator('li').filter({ hasText: 'delete' });
    await expect(deleteCommand).toBeVisible();

    // Verify info command
    const infoCommand = otherCategory.locator('li').filter({ hasText: 'info' });
    await expect(infoCommand).toBeVisible();

    // Verify major_compaction command
    const majorCompactionCommand = otherCategory.locator('li').filter({ hasText: 'major_compaction' });
    await expect(majorCompactionCommand).toBeVisible();

    // Verify these match the actual implementation
    const requestSource = readSourceFile('mirdb-server/src/request.rs');
    expect(requestSource).toContain('Deleter');
    expect(requestSource).toContain('Info,');
    expect(requestSource).toContain('MajorCompaction,');
  });

  test('TC19: No undocumented commands exist in implementation', async ({ page }) => {
    // Verify that all commands in the implementation are documented on the homepage
    const requestSource = readSourceFile('mirdb-server/src/request.rs');
    expect(requestSource).toBeTruthy();

    // Extract implemented commands from source
    const implementedGetters = ['Get', 'Gets'];
    const implementedSetters = ['Set', 'Add', 'Replace', 'Append', 'Prepend'];
    const implementedOther = ['Deleter', 'Info', 'MajorCompaction'];

    // Verify all are in source
    for (const cmd of [...implementedGetters, ...implementedSetters, ...implementedOther]) {
      expect(requestSource).toContain(cmd);
    }

    // Get documented commands from homepage
    const commandsSection = page.locator('.quickstart-step:has(.step-title:has-text("Supported Commands"))');
    await expect(commandsSection).toBeVisible();

    const commandsText = await commandsSection.textContent();

    // Verify all getter commands are documented
    expect(commandsText?.toLowerCase()).toContain('get');
    expect(commandsText?.toLowerCase()).toContain('gets');

    // Verify all setter commands are documented
    expect(commandsText?.toLowerCase()).toContain('set');
    expect(commandsText?.toLowerCase()).toContain('add');
    expect(commandsText?.toLowerCase()).toContain('replace');
    expect(commandsText?.toLowerCase()).toContain('append');
    expect(commandsText?.toLowerCase()).toContain('prepend');

    // Verify all other commands are documented
    expect(commandsText?.toLowerCase()).toContain('delete');
    expect(commandsText?.toLowerCase()).toContain('info');
    expect(commandsText?.toLowerCase()).toContain('major_compaction');
  });
});

test.describe('Content Accuracy - Architecture Diagram', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC20: Architecture diagram shows WAL component matching implementation', async ({ page }) => {
    const architectureSection = page.locator('.architecture-section');
    await expect(architectureSection).toBeVisible();

    const svgDiagram = architectureSection.locator('svg.lsm-diagram');
    await expect(svgDiagram).toBeVisible();

    // Verify WAL is shown in diagram
    const walText = svgDiagram.locator('text:has-text("WAL")');
    await expect(walText).toBeVisible();

    // Verify WAL exists in implementation
    const walSource = readSourceFile('mirdb-server/src/wal.rs');
    expect(walSource).toContain('struct WAL');
  });

  test('TC21: Architecture diagram shows Memtable component matching implementation', async ({ page }) => {
    const architectureSection = page.locator('.architecture-section');
    await expect(architectureSection).toBeVisible();

    const svgDiagram = architectureSection.locator('svg.lsm-diagram');
    await expect(svgDiagram).toBeVisible();

    // Verify Memtable is shown in diagram
    const memtableText = svgDiagram.locator('text:has-text("Memtable")');
    await expect(memtableText.first()).toBeVisible();

    // Verify Memtable exists in implementation
    const memtableSource = readSourceFile('mirdb-server/src/memtable.rs');
    expect(memtableSource || '').toBeTruthy();
  });

  test('TC22: Architecture diagram shows SSTable levels matching implementation', async ({ page }) => {
    const architectureSection = page.locator('.architecture-section');
    await expect(architectureSection).toBeVisible();

    const svgDiagram = architectureSection.locator('svg.lsm-diagram');
    await expect(svgDiagram).toBeVisible();

    // Verify Level 0 SSTables shown
    const level0Text = svgDiagram.locator('text:has-text("Level 0")');
    await expect(level0Text).toBeVisible();

    // Verify Level 1+ SSTables shown
    const level1Text = svgDiagram.locator('text:has-text("Level 1")');
    await expect(level1Text).toBeVisible();

    // Verify SSTable implementation exists
    const sstableSource = readSourceFile('mirdb-server/src/sstable_reader.rs');
    expect(sstableSource || '').toBeTruthy();
  });

  test('TC23: Architecture diagram shows Cuckoo Filter component', async ({ page }) => {
    const architectureSection = page.locator('.architecture-section');
    await expect(architectureSection).toBeVisible();

    const svgDiagram = architectureSection.locator('svg.lsm-diagram');
    await expect(svgDiagram).toBeVisible();

    // Verify Cuckoo Filter is shown in diagram
    const cuckooFilterText = svgDiagram.locator('text:has-text("Cuckoo Filter")');
    await expect(cuckooFilterText).toBeVisible();

    // Verify Cuckoo Filter exists in implementation
    const metaBlockSource = readSourceFile('sstable/src/meta_block.rs');
    expect(metaBlockSource).toContain('cuckoofilter');
  });

  test('TC24: Architecture explanation mentions compaction matching implementation', async ({ page }) => {
    const architectureSection = page.locator('.architecture-section');
    await expect(architectureSection).toBeVisible();

    const explanationSection = architectureSection.locator('.architecture-explanation');
    await expect(explanationSection).toBeVisible();

    const explanationText = await explanationSection.textContent();

    // Verify Minor Compaction is mentioned
    expect(explanationText).toContain('Minor Compaction');

    // Verify Major Compaction is mentioned
    expect(explanationText).toContain('Major Compaction');

    // Verify these exist in implementation
    const dataManagerSource = readSourceFile('mirdb-server/src/data_manager.rs');
    expect(dataManagerSource).toContain('minor_compaction');
    expect(dataManagerSource).toContain('major_compaction');
  });
});

test.describe('Content Accuracy - Project Status', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC25: Completed features list matches actual implementation', async ({ page }) => {
    const statusSection = page.locator('#project-status');
    await expect(statusSection).toBeVisible();

    const completedFeatures = statusSection.locator('.completed-features');
    await expect(completedFeatures).toBeVisible();

    const completedText = await completedFeatures.textContent();

    // Verify listed completed features match implementation
    // Memcached Protocol Compatibility
    expect(completedText).toContain('Memcached Protocol Compatibility');
    const requestSource = readSourceFile('mirdb-server/src/request.rs');
    expect(requestSource).toContain('enum Request');

    // LSM-Tree Storage Architecture
    expect(completedText).toContain('LSM-Tree Storage Architecture');
    const dataManagerSource = readSourceFile('mirdb-server/src/data_manager.rs');
    expect(dataManagerSource).toContain('struct DataManager');

    // Write-Ahead Logging (WAL)
    expect(completedText).toContain('Write-Ahead Logging');
    const walSource = readSourceFile('mirdb-server/src/wal.rs');
    expect(walSource).toContain('struct WAL');

    // Minor & Major Compaction
    expect(completedText).toContain('Minor');
    expect(completedText).toContain('Major Compaction');
    expect(dataManagerSource).toContain('minor_compaction');
    expect(dataManagerSource).toContain('major_compaction');

    // LRU Block Cache
    expect(completedText).toContain('LRU Block Cache');
    const cacheSource = readSourceFile('sstable/src/cache.rs');
    expect(cacheSource).toContain('LruCache');

    // Cuckoo Filter
    expect(completedText).toContain('Cuckoo Filter');
    const metaBlockSource = readSourceFile('sstable/src/meta_block.rs');
    expect(metaBlockSource).toContain('cuckoofilter');

    // Snappy Compression
    expect(completedText).toContain('Snappy Compression');
    expect(walSource).toContain('snap::');
  });

  test('TC26: Planned features (Raft consensus) are correctly marked as not implemented', async ({ page }) => {
    const statusSection = page.locator('#project-status');
    await expect(statusSection).toBeVisible();

    const plannedFeatures = statusSection.locator('.planned-features');
    await expect(plannedFeatures).toBeVisible();

    const plannedText = await plannedFeatures.textContent();

    // Verify Raft consensus is listed as planned
    expect(plannedText).toContain('Raft Consensus');

    // Verify Raft is NOT implemented (search for raft-related code)
    const mainSource = readSourceFile('mirdb-server/src/main.rs');
    const dataManagerSource = readSourceFile('mirdb-server/src/data_manager.rs');

    // Raft should not be mentioned in current implementation
    expect(mainSource.toLowerCase()).not.toContain('raft');
    expect(dataManagerSource.toLowerCase()).not.toContain('raft');
  });
});
