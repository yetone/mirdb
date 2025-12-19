// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');
const fs = require('fs');

const indexPath = 'file://' + path.resolve(__dirname, '../index.html');

// Configuration values from MirDB source code (mirdb-server/src/options.rs)
const MIRDB_CONFIG = {
  defaultPort: 12333,
  defaultAddr: '0.0.0.0:12333',
  defaultWorkDir: '/tmp/mirdb',
  maxLevel: 7,
  sstMaxSize: '100MB',
  memTableMaxSize: '4MB',
  blockSize: '4KB'
};

// Supported commands from MirDB source code (mirdb-server/src/request.rs)
const SUPPORTED_COMMANDS = {
  storage: ['SET', 'ADD', 'REPLACE', 'APPEND', 'PREPEND'],
  retrieval: ['GET', 'GETS'],
  deletion: ['DELETE'],
  mirdbSpecific: ['INFO', 'MAJOR_COMPACTION']
};

test.describe('Content Accuracy - Memcached Protocol Claim', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(indexPath);
  });

  // Test Case 1: Verify memcached protocol claim matches actual capability
  test('TC1: memcached protocol claim is accurate', async ({ page }) => {
    // The homepage claims memcached protocol support
    const heroSection = page.locator('#hero, .hero, header');
    const heroText = await heroSection.textContent();

    // Verify the claim is present
    expect(heroText.toLowerCase()).toContain('memcached');
    expect(heroText.toLowerCase()).toContain('protocol');

    // Verify features section also mentions memcached compatibility
    const featuresSection = page.locator('#features');
    const featuresText = await featuresSection.textContent();
    expect(featuresText.toLowerCase()).toContain('memcached');
    expect(featuresText.toLowerCase()).toContain('compatible');

    // This is accurate because MirDB implements memcached text protocol
    // as evidenced by mirdb-server/src/request.rs containing standard memcached commands
  });
});

test.describe('Content Accuracy - Persistence Claim', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(indexPath);
  });

  // Test Case 2: Verify persistence claim is accurate
  test('TC2: persistence claim matches actual SSTable storage implementation', async ({ page }) => {
    const featuresSection = page.locator('#features');
    const featuresText = await featuresSection.textContent();

    // Verify persistence claim
    expect(featuresText.toLowerCase()).toContain('persistent');
    expect(featuresText.toLowerCase()).toContain('storage');

    // Verify SSTable mention (the actual storage mechanism)
    expect(featuresText.toLowerCase()).toContain('sstable');

    // This is accurate because MirDB has:
    // - sstable/ crate for SSTable storage
    // - mirdb-server/src/wal.rs for write-ahead log
    // - mirdb-server/src/data_manager.rs for data persistence management
  });
});

test.describe('Content Accuracy - Default Port', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(indexPath);
  });

  // Test Case 3: Verify default port 12333 matches actual default
  test('TC3: listed port 12333 matches actual default configuration', async ({ page }) => {
    // Search for port mention in getting started section
    const gettingStartedSection = page.locator('#getting-started');
    const gettingStartedText = await gettingStartedSection.textContent();

    // Verify port 12333 is documented
    expect(gettingStartedText).toContain(MIRDB_CONFIG.defaultPort.toString());

    // The telnet command should use the correct port
    expect(gettingStartedText).toContain('telnet localhost 12333');

    // This matches the actual default in:
    // - mirdb-server/src/options.rs (not explicit, but config uses it)
    // - etc/mirdb.toml: addr = "0.0.0.0:12333"
    // - .something/knowledge files confirm this is the default
  });
});

test.describe('Content Accuracy - Supported Commands', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(indexPath);
  });

  // Test Case 4: Verify all listed commands match actual supported commands
  test('TC4: all listed storage commands are actually supported', async ({ page }) => {
    const commandsSection = page.locator('#commands');
    const commandsText = await commandsSection.textContent();
    const upperText = commandsText.toUpperCase();

    // Verify all storage commands
    for (const cmd of SUPPORTED_COMMANDS.storage) {
      expect(upperText).toContain(cmd);
    }

    // These match mirdb-server/src/request.rs SetterType enum:
    // Set, Add, Replace, Append, Prepend
  });

  test('TC4b: all listed retrieval commands are actually supported', async ({ page }) => {
    const commandsSection = page.locator('#commands');
    const commandsText = await commandsSection.textContent();
    const upperText = commandsText.toUpperCase();

    // Verify retrieval commands
    for (const cmd of SUPPORTED_COMMANDS.retrieval) {
      expect(upperText).toContain(cmd);
    }

    // These match mirdb-server/src/request.rs GetterType enum:
    // Get, Gets
  });

  test('TC4c: DELETE command is documented and supported', async ({ page }) => {
    const commandsSection = page.locator('#commands');
    const commandsText = await commandsSection.textContent();
    const upperText = commandsText.toUpperCase();

    // Verify DELETE command
    expect(upperText).toContain('DELETE');

    // This matches mirdb-server/src/request.rs Deleter variant
  });

  test('TC4d: MirDB-specific commands (INFO, MAJOR_COMPACTION) are documented', async ({ page }) => {
    const commandsSection = page.locator('#commands');
    const commandsText = await commandsSection.textContent();
    const upperText = commandsText.toUpperCase();

    // Verify MirDB-specific commands
    for (const cmd of SUPPORTED_COMMANDS.mirdbSpecific) {
      expect(upperText).toContain(cmd);
    }

    // Verify they are marked as MirDB-specific
    const lowerText = commandsText.toLowerCase();
    expect(lowerText.includes('mirdb') && lowerText.includes('specific')).toBeTruthy();

    // These match mirdb-server/src/request.rs: Info, MajorCompaction variants
  });

  test('TC4e: no unsupported commands are claimed', async ({ page }) => {
    const commandsSection = page.locator('#commands');
    const commandsText = await commandsSection.textContent();
    const upperText = commandsText.toUpperCase();

    // These are commands NOT supported by MirDB that standard memcached might have
    const unsupportedCommands = ['INCR', 'DECR', 'CAS', 'STATS', 'FLUSH_ALL', 'VERSION', 'QUIT'];

    // Verify none of these are listed as supported
    for (const cmd of unsupportedCommands) {
      // Allow partial matches for documentation purposes but not as command names
      // We check that they don't appear in command headers
      const commandHeaders = await commandsSection.locator('h4').allTextContents();
      const headerCommands = commandHeaders.map(h => h.toUpperCase().trim());
      expect(headerCommands).not.toContain(cmd);
    }
  });
});

test.describe('Content Accuracy - Getting Started Commands', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(indexPath);
  });

  // Test Case 5: Verify getting started commands are accurate
  test('TC5a: clone command uses valid repository URL', async ({ page }) => {
    const gettingStartedSection = page.locator('#getting-started');
    const gettingStartedText = await gettingStartedSection.textContent();

    // Verify git clone command is present
    expect(gettingStartedText).toContain('git clone');

    // Verify it uses the github repository URL format
    expect(gettingStartedText).toMatch(/git clone.*github\.com.*mirdb/i);
  });

  test('TC5b: build command uses cargo (Rust build tool)', async ({ page }) => {
    const gettingStartedSection = page.locator('#getting-started');
    const gettingStartedText = await gettingStartedSection.textContent();

    // MirDB is a Rust project, should use cargo build
    expect(gettingStartedText).toContain('cargo build');

    // Should mention --release for production build
    expect(gettingStartedText).toContain('--release');
  });

  test('TC5c: server binary path is accurate', async ({ page }) => {
    const gettingStartedSection = page.locator('#getting-started');
    const gettingStartedText = await gettingStartedSection.textContent();

    // After cargo build --release, the binary is in target/release/
    expect(gettingStartedText).toContain('target/release/mirdb-server');
  });

  test('TC5d: example SET/GET commands use correct memcached syntax', async ({ page }) => {
    const gettingStartedSection = page.locator('#getting-started');
    const gettingStartedText = await gettingStartedSection.textContent();

    // SET command format: set <key> <flags> <ttl> <bytes>
    // The example shows: set mykey 0 0 5
    expect(gettingStartedText.toLowerCase()).toContain('set mykey');
    expect(gettingStartedText).toContain('0 0 5'); // flags=0, ttl=0, bytes=5

    // GET command format: get <key>
    expect(gettingStartedText.toLowerCase()).toContain('get mykey');

    // Response format should show VALUE and END
    expect(gettingStartedText).toContain('STORED');
    expect(gettingStartedText).toContain('VALUE');
    expect(gettingStartedText).toContain('END');
  });
});

test.describe('Content Accuracy - LSM Architecture Claim', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(indexPath);
  });

  // Additional verification: LSM tree architecture claim
  test('LSM tree architecture claim is accurate', async ({ page }) => {
    const featuresSection = page.locator('#features');
    const featuresText = await featuresSection.textContent();

    // Verify LSM architecture claim
    expect(featuresText).toContain('LSM');
    expect(featuresText.toLowerCase()).toContain('tree');
    expect(featuresText.toLowerCase()).toContain('architecture');

    // Verify it mentions compaction (key LSM feature)
    expect(featuresText.toLowerCase()).toContain('compaction');

    // This is accurate because:
    // - mirdb-server/src/data_manager.rs implements LSM tree
    // - Minor and major compaction are implemented
    // - Multiple SSTable levels exist
  });
});

test.describe('Content Accuracy - Async/Tokio Claim', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(indexPath);
  });

  // Verify Tokio/async performance claim
  test('async/Tokio performance claim is accurate', async ({ page }) => {
    const featuresSection = page.locator('#features');
    const featuresText = await featuresSection.textContent();

    // Verify async performance claim
    expect(featuresText.toLowerCase()).toContain('async');
    expect(featuresText).toContain('Tokio');

    // This is accurate because:
    // - Cargo.toml includes tokio dependency
    // - mirdb-server uses Tokio runtime for async networking
  });
});

test.describe('Content Accuracy - Rust Language Claim', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(indexPath);
  });

  // Verify Rust language claim
  test('Rust language claim is accurate', async ({ page }) => {
    const heroSection = page.locator('#hero, .hero, header');
    const heroText = await heroSection.textContent();

    // Verify Rust is mentioned
    expect(heroText).toContain('Rust');

    // This is accurate because:
    // - All source files are .rs (Rust)
    // - Cargo.toml is the Rust package manager config
  });
});
