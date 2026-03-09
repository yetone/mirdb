/**
 * Content Accuracy Tests
 * Owner: Scenario 20 - Content Accuracy and Consistency
 *
 * Tests:
 * - Features align with README
 * - Port 12333 matches code
 * - Memtable 4MB matches code
 * - SSTable 100MB matches code
 * - Memcached protocol accurate
 *
 * Validates NFR-6: Homepage design shall be consistent with GitHub README content
 */

const { test, expect } = require('@playwright/test');
const fs = require('fs');
const path = require('path');

// Path to the HTML file and source files
const htmlFilePath = path.join(__dirname, '../../docs/index.html');
const readmePath = path.join(__dirname, '../../README.md');
const optionsRsPath = path.join(__dirname, '../../mirdb-server/src/options.rs');
const configRsPath = path.join(__dirname, '../../mirdb-server/src/config.rs');

// Default configuration values from MirDB source code (options.rs)
// These are the canonical values that the homepage should match
const MIRDB_DEFAULTS = {
  port: 12333,
  address: '0.0.0.0:12333',
  memtableMaxSize: 4, // MB
  sstableMaxSize: 100, // MB
  maxLevels: 7,
  blockSize: 4, // KB
  workDir: '/tmp/mirdb',
};

test.describe('Content Accuracy and Consistency', () => {
  let htmlContent;
  let readmeContent;
  let optionsRsContent;
  let configRsContent;

  test.beforeAll(() => {
    htmlContent = fs.readFileSync(htmlFilePath, 'utf-8');
    readmeContent = fs.readFileSync(readmePath, 'utf-8');
    optionsRsContent = fs.readFileSync(optionsRsPath, 'utf-8');
    configRsContent = fs.readFileSync(configRsPath, 'utf-8');
  });

  test.describe('TC1: Features align with README', () => {
    test('Homepage features match README feature descriptions', async ({ page }) => {
      await page.goto('/');

      // README mentions these features as completed:
      // - tokio with memcached protocol
      // - memtable with skiplist
      // - minor compaction
      // - major compaction

      // Verify Memcached Protocol feature is present
      const memcachedFeature = page.locator('[data-feature="memcached"]');
      await expect(memcachedFeature).toBeVisible();
      const memcachedText = await memcachedFeature.textContent();
      expect(memcachedText.toLowerCase()).toContain('memcached');
      expect(memcachedText.toLowerCase()).toContain('protocol');

      // Verify Skip List Memtable feature is present (README: "memtable with skiplist")
      const skiplistFeature = page.locator('[data-feature="skiplist"]');
      await expect(skiplistFeature).toBeVisible();
      const skiplistText = await skiplistFeature.textContent();
      expect(skiplistText.toLowerCase()).toContain('skip');
      expect(skiplistText.toLowerCase()).toContain('memtable');

      // Verify Compaction feature is present (README: minor and major compaction)
      const compactionFeature = page.locator('[data-feature="compaction"]');
      await expect(compactionFeature).toBeVisible();
      const compactionText = await compactionFeature.textContent();
      expect(compactionText.toLowerCase()).toContain('compaction');
      // Should mention both minor and major
      expect(compactionText.toLowerCase()).toContain('minor');
      expect(compactionText.toLowerCase()).toContain('major');
    });

    test('README completed features are marked as implemented in roadmap', async ({ page }) => {
      await page.goto('/');

      // README TODO shows completed: memcached protocol, memtable with skiplist, minor compaction, major compaction
      // These should be marked as implemented in the roadmap

      // Check Memcached Protocol is implemented
      const implementedItems = page.locator('.roadmap-item.implemented');
      const implementedCount = await implementedItems.count();
      expect(implementedCount).toBeGreaterThan(0);

      // Get all implemented item texts
      const implementedTexts = await implementedItems.allTextContents();
      const allImplementedText = implementedTexts.join(' ').toLowerCase();

      // Verify memcached protocol is marked as implemented
      expect(allImplementedText).toContain('memcached');

      // Verify compaction is marked as implemented
      expect(allImplementedText).toContain('compaction');

      // Verify skip list/memtable is marked as implemented
      expect(allImplementedText).toContain('memtable');
    });

    test('README planned feature (Raft) is marked as planned', async ({ page }) => {
      await page.goto('/');

      // README shows: "[ ] raft" as planned
      const plannedItems = page.locator('.roadmap-item.planned');
      const plannedTexts = await plannedItems.allTextContents();
      const allPlannedText = plannedTexts.join(' ').toLowerCase();

      // Raft should be in planned features
      expect(allPlannedText).toContain('raft');
    });
  });

  test.describe('TC2: Verify default port matches code', () => {
    test('Documented port 12333 matches actual MirDB default', async ({ page }) => {
      await page.goto('/');

      // Verify the port value in the source code
      // options.rs doesn't have the port, but config.rs test shows: addr = "0.0.0.0:12333"
      expect(configRsContent).toContain('0.0.0.0:12333');

      // Verify the homepage documents the correct port (may appear in multiple places)
      const portCells = page.locator('code:text("0.0.0.0:12333")');
      const portCount = await portCells.count();
      expect(portCount).toBeGreaterThan(0);
      await expect(portCells.first()).toBeVisible();

      // Also verify port 12333 is mentioned in quick start section
      const quickstartSection = page.locator('#quickstart');
      const quickstartText = await quickstartSection.textContent();
      expect(quickstartText).toContain('12333');
    });

    test('Port 12333 is documented consistently across sections', async ({ page }) => {
      await page.goto('/');

      // Check the port is mentioned in the configuration table
      const configTable = page.locator('.config-table');
      const tableText = await configTable.textContent();
      expect(tableText).toContain('12333');

      // Check it's mentioned in quick start section
      const quickstart = page.locator('#quickstart');
      const quickstartText = await quickstart.textContent();
      expect(quickstartText).toContain('12333');
    });
  });

  test.describe('TC3: Verify memtable size matches code', () => {
    test('Documented memtable size 4MB matches actual default', async ({ page }) => {
      await page.goto('/');

      // Verify in source code: mem_table_max_size: MB * 4 (options.rs)
      expect(optionsRsContent).toContain('mem_table_max_size: MB * 4');

      // Verify homepage documents 4MB for memtable
      const configTable = page.locator('.config-table');
      const configText = await configTable.textContent();

      // Should mention mem_table_max_size and 4MB
      expect(configText).toContain('mem_table_max_size');
      expect(configText).toContain('4MB');
    });

    test('Memtable size 4MB is mentioned in architecture section', async ({ page }) => {
      await page.goto('/');

      // The architecture section should explain memtable behavior
      const archSection = page.locator('#architecture');
      const archText = await archSection.textContent();

      // Should mention the 4MB threshold
      expect(archText).toContain('4MB');
    });
  });

  test.describe('TC4: Verify SSTable size matches code', () => {
    test('Documented SSTable size 100MB matches actual default', async ({ page }) => {
      await page.goto('/');

      // Verify in source code: sst_max_size: MB * 100 (options.rs)
      expect(optionsRsContent).toContain('sst_max_size: MB * 100');

      // Verify homepage documents 100MB for SSTable
      const configTable = page.locator('.config-table');
      const configText = await configTable.textContent();

      // Should mention sst_max_size and 100MB
      expect(configText).toContain('sst_max_size');
      expect(configText).toContain('100MB');
    });
  });

  test.describe('TC5: Verify memcached protocol examples are accurate', () => {
    test('SET command syntax matches actual memcached protocol', async ({ page }) => {
      await page.goto('/');

      // Standard memcached SET format: set <key> <flags> <exptime> <bytes> [noreply]\r\n<data>\r\n
      const commandsSection = page.locator('#commands');
      const commandsText = await commandsSection.textContent();

      // Verify SET command format is documented
      expect(commandsText).toContain('set');
      expect(commandsText).toContain('key');
      expect(commandsText).toContain('flags');
      expect(commandsText).toContain('exptime');
      expect(commandsText).toContain('bytes');

      // Verify SET response is documented
      expect(commandsText).toContain('STORED');
    });

    test('GET command syntax matches actual memcached protocol', async ({ page }) => {
      await page.goto('/');

      // Standard memcached GET format: get <key> [<key2> ...]
      const commandsSection = page.locator('#commands');
      const commandsText = await commandsSection.textContent();

      // Verify GET command format is documented
      expect(commandsText).toContain('get');

      // Verify GET response format is documented (VALUE <key> <flags> <bytes>)
      expect(commandsText).toContain('VALUE');
      expect(commandsText).toContain('END');
    });

    test('DELETE command syntax matches actual memcached protocol', async ({ page }) => {
      await page.goto('/');

      // Standard memcached DELETE format: delete <key> [noreply]
      const commandsSection = page.locator('#commands');
      const commandsText = await commandsSection.textContent();

      // Verify DELETE command is documented
      expect(commandsText).toContain('delete');
      expect(commandsText).toContain('DELETED');
    });

    test('Quick start SET/GET examples use correct format', async ({ page }) => {
      await page.goto('/');

      const quickstart = page.locator('#quickstart');
      const quickstartText = await quickstart.textContent();

      // Verify SET example has correct format: set <key> <flags> <ttl> <bytes>
      expect(quickstartText).toContain('set mykey 0 0 5');
      expect(quickstartText).toContain('hello');
      expect(quickstartText).toContain('STORED');

      // Verify GET example has correct format
      expect(quickstartText).toContain('get mykey');
      expect(quickstartText).toContain('VALUE mykey 0 5');
      expect(quickstartText).toContain('END');
    });
  });
});

// Additional configuration value tests
test.describe('Configuration Accuracy', () => {
  let optionsRsContent;

  test.beforeAll(() => {
    optionsRsContent = fs.readFileSync(optionsRsPath, 'utf-8');
  });

  test('Homepage documents correct max_level value (7)', async ({ page }) => {
    await page.goto('/');

    // Source code: max_level: 7 (options.rs)
    expect(optionsRsContent).toContain('max_level: 7');

    // Homepage should document 7 levels
    const configTable = page.locator('.config-table');
    const configText = await configTable.textContent();
    expect(configText).toContain('max_level');
    expect(configText).toContain('7');
  });

  test('Homepage documents correct block_size value (4KB)', async ({ page }) => {
    await page.goto('/');

    // Source code: BLOCK_MAX_SIZE: usize = 4 * KB (options.rs)
    expect(optionsRsContent).toContain('BLOCK_MAX_SIZE: usize = 4 * KB');

    // Homepage should document 4KB block size
    const configTable = page.locator('.config-table');
    const configText = await configTable.textContent();
    expect(configText).toContain('block_size');
    expect(configText).toContain('4KB');
  });

  test('Homepage documents correct work_dir value (/tmp/mirdb)', async ({ page }) => {
    await page.goto('/');

    // Source code: work_dir: "/tmp/mirdb".into() (options.rs)
    expect(optionsRsContent).toContain('"/tmp/mirdb"');

    // Homepage should document /tmp/mirdb
    const configTable = page.locator('.config-table');
    const configText = await configTable.textContent();
    expect(configText).toContain('work_dir');
    expect(configText).toContain('/tmp/mirdb');
  });
});

// README content alignment tests
test.describe('README Content Alignment', () => {
  let readmeContent;

  test.beforeAll(() => {
    readmeContent = fs.readFileSync(readmePath, 'utf-8');
  });

  test('Homepage tagline matches README title', async ({ page }) => {
    await page.goto('/');

    // README title: "MirDB: A Persistent Key-Value Store with Memcached protocol"
    expect(readmeContent).toContain('Persistent Key-Value Store');
    expect(readmeContent).toContain('Memcached');

    // Homepage should have similar tagline
    const tagline = page.locator('.tagline');
    const taglineText = await tagline.textContent();
    expect(taglineText.toLowerCase()).toContain('persistent');
    expect(taglineText.toLowerCase()).toContain('key-value');
    expect(taglineText.toLowerCase()).toContain('memcached');
  });

  test('Homepage persistence feature aligns with README description', async ({ page }) => {
    await page.goto('/');

    // Homepage should emphasize persistence (key differentiator from memcached)
    const persistenceFeature = page.locator('[data-feature="persistence"]');
    await expect(persistenceFeature).toBeVisible();
    const persistenceText = await persistenceFeature.textContent();
    expect(persistenceText.toLowerCase()).toContain('persist');
    expect(persistenceText.toLowerCase()).toContain('durable');
  });

  test('Homepage LSM tree feature aligns with architecture description', async ({ page }) => {
    await page.goto('/');

    // Homepage should document LSM tree architecture
    const lsmFeature = page.locator('[data-feature="lsm"]');
    await expect(lsmFeature).toBeVisible();
    const lsmText = await lsmFeature.textContent();
    expect(lsmText.toLowerCase()).toContain('lsm');
    expect(lsmText.toLowerCase()).toContain('tree');

    // Architecture section should also explain LSM
    const archSection = page.locator('#architecture');
    const archText = await archSection.textContent();
    expect(archText.toLowerCase()).toContain('lsm');
  });
});
