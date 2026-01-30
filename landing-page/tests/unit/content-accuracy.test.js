/**
 * Content Accuracy Unit Tests
 * Owner: Scenario 20 - Content Accuracy
 *
 * Tests for verifying all content accurately reflects MirDB's current capabilities (v0.1.0)
 * Based on knowledge files and PRD requirements:
 * - TC1: Feature descriptions match PRD (manual verification via code review)
 * - TC2: Code examples use correct Memcached protocol syntax
 * - TC3: Architecture diagram accuracy (manual verification)
 * - TC4: Version number displayed as v0.1.0
 */
const fs = require('fs');
const path = require('path');

describe('Content Accuracy', () => {
  let htmlContent;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf-8');
  });

  describe('TC1: Feature Descriptions Match MirDB v0.1.0 Capabilities', () => {
    /**
     * MirDB v0.1.0 implemented features (from knowledge files):
     * - Tokio-based async networking with memcached protocol
     * - Memtable with skip list data structure
     * - Minor compaction (memtable to SSTable)
     * - Major compaction (SSTable level compaction)
     * - Persistent storage using SSTables
     * - WAL for durability
     */

    test('Page mentions Persistent Storage feature', () => {
      expect(htmlContent).toMatch(/Persistent Storage/i);
    });

    test('Persistent Storage description is accurate', () => {
      // Should mention data survives restarts and persists to disk
      expect(htmlContent).toMatch(/survives restarts/i);
    });

    test('Page mentions Memcached Protocol feature', () => {
      expect(htmlContent).toMatch(/Memcached Protocol/i);
    });

    test('Memcached Protocol description mentions drop-in replacement', () => {
      expect(htmlContent).toMatch(/drop-in replacement/i);
    });

    test('Page mentions LSM-Tree Architecture feature', () => {
      expect(htmlContent).toMatch(/LSM[-\s]?Tree Architecture/i);
    });

    test('LSM-Tree description mentions write-heavy workloads', () => {
      expect(htmlContent).toMatch(/write[-\s]?heavy workloads/i);
    });

    test('Page mentions Compaction feature', () => {
      expect(htmlContent).toMatch(/Compaction/i);
    });

    test('Compaction description mentions minor and major compaction', () => {
      expect(htmlContent).toMatch(/minor/i);
      expect(htmlContent).toMatch(/major/i);
    });

    test('Page mentions Rust-Powered feature', () => {
      expect(htmlContent).toMatch(/Rust[-\s]?Powered/i);
    });

    test('Rust-Powered description mentions memory safety', () => {
      expect(htmlContent).toMatch(/memory safety/i);
    });

    test('Page mentions Async I/O feature', () => {
      expect(htmlContent).toMatch(/Async I\/O/i);
    });

    test('Async I/O description mentions Tokio', () => {
      expect(htmlContent).toMatch(/Tokio/i);
    });

    test('Page does NOT falsely claim Raft consensus is implemented', () => {
      // Raft is planned but NOT yet implemented in v0.1.0
      // The page should NOT claim it as an available feature
      const raftFeaturePattern = /<article[^>]*class="feature-card"[^>]*>[\s\S]*?Raft[\s\S]*?<\/article>/i;
      const hasRaftFeature = raftFeaturePattern.test(htmlContent);
      expect(hasRaftFeature).toBe(false);
    });

    test('All six features are present in feature cards', () => {
      const expectedFeatures = [
        'Persistent Storage',
        'Memcached Protocol',
        'LSM-Tree',
        'Compaction',
        'Rust',
        'Async'
      ];

      expectedFeatures.forEach(feature => {
        expect(htmlContent).toMatch(new RegExp(feature, 'i'));
      });
    });
  });

  describe('TC2: Code Examples Use Correct Memcached Protocol Syntax', () => {
    /**
     * Memcached protocol syntax (from knowledge files):
     * SET: set <key> <flags> <ttl> <bytes> [noreply]\r\n<data>\r\n
     * GET: get <key1> [<key2> ...]\r\n
     * Response: VALUE <key> <flags> <bytes>\n<data>\nEND
     * Response: STORED
     */

    test('Page contains SET command example', () => {
      expect(htmlContent).toMatch(/SET\s+\w+/i);
    });

    test('SET command includes flags parameter', () => {
      // SET <key> <flags> <ttl> <bytes>
      // Example: SET mykey 0 0 5
      const setCommandPattern = /SET\s+\w+\s+\d+/i;
      expect(htmlContent).toMatch(setCommandPattern);
    });

    test('SET command includes exptime (ttl) parameter', () => {
      // SET <key> <flags> <ttl> <bytes>
      // Should have at least 3 numeric parameters after key
      const setWithTtlPattern = /SET\s+\w+\s+\d+\s+\d+/i;
      expect(htmlContent).toMatch(setWithTtlPattern);
    });

    test('SET command includes bytes parameter', () => {
      // SET <key> <flags> <ttl> <bytes>
      // Should have 4 parameters: key flags ttl bytes
      const setFullPattern = /SET\s+\w+\s+\d+\s+\d+\s+\d+/i;
      expect(htmlContent).toMatch(setFullPattern);
    });

    test('Page contains GET command example', () => {
      expect(htmlContent).toMatch(/GET\s+\w+/i);
    });

    test('Page shows STORED response for SET command', () => {
      expect(htmlContent).toMatch(/STORED/);
    });

    test('Page shows VALUE response format for GET command', () => {
      // VALUE <key> <flags> <bytes>
      const valueResponsePattern = /VALUE\s+\w+\s+\d+\s+\d+/i;
      expect(htmlContent).toMatch(valueResponsePattern);
    });

    test('Page shows END response for GET command', () => {
      expect(htmlContent).toMatch(/END/);
    });

    test('Code examples show correct telnet usage', () => {
      expect(htmlContent).toMatch(/telnet\s+localhost\s+11211/i);
    });

    test('Server start command includes port configuration', () => {
      expect(htmlContent).toMatch(/--port\s+11211/i);
    });

    test('Server start command includes data directory configuration', () => {
      expect(htmlContent).toMatch(/--data-dir/i);
    });
  });

  describe('TC3: Architecture Diagram Accuracy', () => {
    /**
     * LSM-Tree components (from knowledge files):
     * - Memtable (in-memory skip list)
     * - WAL (Write-Ahead Log)
     * - SSTables organized in levels
     * - Minor compaction (memtable -> Level 0)
     * - Major compaction (level N -> level N+1)
     */

    test('Architecture section exists', () => {
      expect(htmlContent).toMatch(/<section[^>]*id="architecture"/);
    });

    test('Architecture section contains Mermaid diagram', () => {
      expect(htmlContent).toMatch(/class="[^"]*mermaid[^"]*"/);
    });

    test('Diagram mentions Memtable component', () => {
      expect(htmlContent).toMatch(/Memtable/);
    });

    test('Diagram mentions WAL (Write-Ahead Log) component', () => {
      expect(htmlContent).toMatch(/WAL/);
    });

    test('Diagram mentions SSTable component', () => {
      expect(htmlContent).toMatch(/SSTable/i);
    });

    test('Diagram shows Write Path', () => {
      expect(htmlContent).toMatch(/Write Path/i);
    });

    test('Diagram shows Read Path', () => {
      expect(htmlContent).toMatch(/Read Path/i);
    });

    test('Diagram shows Compaction Process', () => {
      expect(htmlContent).toMatch(/Compaction/i);
    });

    test('Diagram shows Level 0 in SSTable hierarchy', () => {
      expect(htmlContent).toMatch(/Level 0/i);
    });

    test('Memtable description mentions skip list', () => {
      expect(htmlContent).toMatch(/skip list/i);
    });

    test('Memtable description mentions in-memory', () => {
      expect(htmlContent).toMatch(/in-memory/i);
    });

    test('SSTable description mentions sorted', () => {
      expect(htmlContent).toMatch(/sorted/i);
    });

    test('SSTable description mentions immutable or on-disk', () => {
      const hasImmutableOrOnDisk = /immutable/i.test(htmlContent) || /on-disk/i.test(htmlContent);
      expect(hasImmutableOrOnDisk).toBe(true);
    });

    test('WAL description mentions durability or crash recovery', () => {
      const hasDurabilityOrRecovery = /durability/i.test(htmlContent) || /crash/i.test(htmlContent) || /recovery/i.test(htmlContent);
      expect(hasDurabilityOrRecovery).toBe(true);
    });

    test('Compaction description mentions merging SSTables', () => {
      const hasMerge = /merge/i.test(htmlContent);
      expect(hasMerge).toBe(true);
    });
  });

  describe('TC4: Version Number Display', () => {
    /**
     * Current version is v0.1.0 (from PRD and knowledge files)
     */

    test('Page displays version number', () => {
      const versionPattern = /v?0\.1\.0/i;
      expect(htmlContent).toMatch(versionPattern);
    });

    test('JSON-LD contains correct software version', () => {
      const jsonLdMatch = htmlContent.match(/<script[^>]*type="application\/ld\+json"[^>]*>([\s\S]*?)<\/script>/);
      expect(jsonLdMatch).not.toBeNull();

      const jsonLd = JSON.parse(jsonLdMatch[1]);
      expect(jsonLd.softwareVersion).toBe('0.1.0');
    });

    test('Version is displayed in a user-visible location (not just metadata)', () => {
      // Version should appear somewhere visible to users, not just in JSON-LD
      // This could be in the hero, footer, or any visible section
      // We check that the version appears outside of script tags
      const contentWithoutScripts = htmlContent.replace(/<script[\s\S]*?<\/script>/g, '');
      const versionPattern = /v?0\.1\.0/i;
      expect(contentWithoutScripts).toMatch(versionPattern);
    });
  });

  describe('Installation Instructions Accuracy', () => {
    /**
     * Installation should use cargo install
     * Configuration uses TOML format
     */

    test('Installation section mentions cargo install', () => {
      expect(htmlContent).toMatch(/cargo install/i);
    });

    test('Installation mentions mirdb package', () => {
      expect(htmlContent).toMatch(/cargo install mirdb/i);
    });

    test('Configuration example uses TOML format', () => {
      expect(htmlContent).toMatch(/\.toml/i);
    });

    test('Configuration shows port setting', () => {
      expect(htmlContent).toMatch(/port\s*=\s*11211/i);
    });

    test('Configuration shows data directory setting', () => {
      expect(htmlContent).toMatch(/data_dir/i);
    });
  });

  describe('GitHub Repository Link Accuracy', () => {
    test('Page links to correct GitHub repository', () => {
      expect(htmlContent).toMatch(/https:\/\/github\.com\/yetone\/mirdb/);
    });

    test('GitHub link appears in navigation', () => {
      const navSection = htmlContent.match(/<!-- SECTION: Navigation[\s\S]*?<!-- END SECTION: Navigation -->/);
      expect(navSection).not.toBeNull();
      expect(navSection[0]).toMatch(/github\.com\/yetone\/mirdb/);
    });

    test('GitHub link appears in footer', () => {
      const footerSection = htmlContent.match(/<!-- SECTION: Footer[\s\S]*?<!-- END SECTION: Footer -->/);
      expect(footerSection).not.toBeNull();
      expect(footerSection[0]).toMatch(/github\.com\/yetone\/mirdb/);
    });

    test('GitHub link appears in hero CTA', () => {
      const heroSection = htmlContent.match(/<!-- SECTION: Hero[\s\S]*?<!-- END SECTION: Hero -->/);
      expect(heroSection).not.toBeNull();
      expect(heroSection[0]).toMatch(/github\.com\/yetone\/mirdb/);
    });
  });

  describe('Author Attribution Accuracy', () => {
    test('Page credits author yetone', () => {
      expect(htmlContent).toMatch(/yetone/);
    });

    test('Author link points to correct GitHub profile', () => {
      expect(htmlContent).toMatch(/https:\/\/github\.com\/yetone/);
    });
  });

  describe('Technical Claims Accuracy', () => {
    test('Page correctly describes MirDB as persistent', () => {
      expect(htmlContent).toMatch(/persistent/i);
    });

    test('Page correctly describes MirDB as key-value store', () => {
      expect(htmlContent).toMatch(/key-value/i);
    });

    test('Page correctly describes Memcached protocol compatibility', () => {
      expect(htmlContent).toMatch(/Memcached protocol/i);
    });

    test('Page correctly describes LSM-tree architecture', () => {
      expect(htmlContent).toMatch(/LSM/i);
    });

    test('Page correctly describes Rust implementation', () => {
      expect(htmlContent).toMatch(/Rust/);
    });

    test('Page correctly describes Tokio async runtime', () => {
      expect(htmlContent).toMatch(/Tokio/);
    });

    test('Default port mentioned is 11211 (standard Memcached port)', () => {
      expect(htmlContent).toMatch(/11211/);
    });
  });
});
