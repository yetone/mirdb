// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Content Accuracy Validation Tests
 *
 * These tests verify that all technical content on the MirDB homepage
 * accurately represents the product's capabilities as defined in the PRD
 * and knowledge base documentation.
 */
test.describe('Content Accuracy Validation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Memcached protocol claim - Content accurately states Memcached protocol compatibility', async ({ page }) => {
    // Verify the hero tagline mentions Memcached Protocol
    const heroSection = page.locator('[data-testid="hero-section"], section.hero, #hero');
    await expect(heroSection).toBeVisible();

    const tagline = heroSection.locator('[data-testid="tagline"], .tagline');
    await expect(tagline).toContainText('Memcached Protocol');

    // Verify the features section accurately describes Memcached Protocol compatibility
    const featuresSection = page.locator('[data-testid="features-section"], #features');
    await expect(featuresSection).toBeVisible();

    // Find Memcached Protocol feature card
    const memcachedCard = featuresSection.locator('.feature-card').filter({ hasText: 'Memcached Protocol' });
    await expect(memcachedCard).toBeVisible();

    // Verify accurate description: "Drop-in compatibility with existing memcached clients"
    const memcachedDescription = memcachedCard.locator('p');
    await expect(memcachedDescription).toContainText('Drop-in compatibility with existing memcached clients');

    // Verify meta description also mentions Memcached protocol
    const metaDescription = await page.locator('meta[name="description"]').getAttribute('content');
    expect(metaDescription).toContain('Memcached protocol');
  });

  test('TC2: Port number - Default port 12333 is correctly mentioned', async ({ page }) => {
    // Check configuration section for listen address
    const configSection = page.locator('[data-testid="configuration-section"], #configuration');
    await expect(configSection).toBeVisible();

    // Verify listen address shows 0.0.0.0:12333
    const listenAddressValue = page.locator('[data-testid="config-value-listen-address"]');
    await expect(listenAddressValue).toHaveText('0.0.0.0:12333');

    // Verify port 12333 is also mentioned in quick start section
    const quickStartSection = page.locator('[data-testid="quick-start"], #quick-start');
    await expect(quickStartSection).toBeVisible();

    const codeBlock = quickStartSection.locator('pre').first();
    const codeContent = await codeBlock.textContent();
    expect(codeContent).toContain('12333');
  });

  test('TC3: Command list completeness - All 9 supported commands are listed', async ({ page }) => {
    // Navigate to commands section
    const commandsSection = page.locator('[data-testid="commands-section"], #commands');
    await expect(commandsSection).toBeVisible();

    // Define all 9 expected commands per PRD
    const expectedCommands = [
      { name: 'SET', description: 'Store a key-value pair' },
      { name: 'GET', description: 'Retrieve value(s) by key' },
      { name: 'DELETE', description: 'Remove a key' },
      { name: 'ADD', description: 'Store only if key doesn\'t exist' },
      { name: 'REPLACE', description: 'Store only if key exists' },
      { name: 'APPEND', description: 'Append to existing value' },
      { name: 'PREPEND', description: 'Prepend to existing value' },
      { name: 'INFO', description: 'Display database status' },
      { name: 'MAJOR_COMPACTION', description: 'Trigger manual compaction' }
    ];

    // Verify each command is present with correct description
    for (const cmd of expectedCommands) {
      const commandItem = commandsSection.locator(`[data-testid="command-${cmd.name}"]`);
      await expect(commandItem, `Command ${cmd.name} should be visible`).toBeVisible();

      // Verify command code element
      const codeElement = commandItem.locator('code');
      await expect(codeElement).toHaveText(cmd.name);

      // Verify description is accurate
      const descriptionElement = commandItem.locator('span');
      await expect(descriptionElement).toContainText(cmd.description);
    }

    // Verify exactly 9 commands (no more, no less)
    const commandItems = commandsSection.locator('.command-item');
    await expect(commandItems).toHaveCount(9);
  });

  test('TC4: LSM Tree claim - Content accurately describes LSM Tree architecture', async ({ page }) => {
    // Verify LSM Tree Engine feature is present and accurate
    const featuresSection = page.locator('[data-testid="features-section"], #features');
    await expect(featuresSection).toBeVisible();

    // Find LSM Tree Engine feature card
    const lsmCard = featuresSection.locator('.feature-card').filter({ hasText: 'LSM Tree Engine' });
    await expect(lsmCard).toBeVisible();

    // Verify LSM Tree heading
    const lsmHeading = lsmCard.locator('h3');
    await expect(lsmHeading).toHaveText('LSM Tree Engine');

    // Verify description mentions high-performance writes with background compaction
    // Per PRD: "High-performance writes with background compaction"
    const lsmDescription = lsmCard.locator('p');
    await expect(lsmDescription).toContainText('High-performance writes with background compaction');

    // Verify project status mentions compaction-related features (minor & major compaction)
    const statusSection = page.locator('[data-testid="status-section"], #status');
    await expect(statusSection).toBeVisible();

    // Check that Minor Compaction is listed as implemented
    const minorCompaction = statusSection.locator('[data-testid="status-item-minor-compaction"]');
    await expect(minorCompaction).toBeVisible();
    const minorCompactionStatus = await minorCompaction.getAttribute('data-status');
    expect(minorCompactionStatus).toBe('implemented');

    // Check that Major Compaction is listed as implemented
    const majorCompaction = statusSection.locator('[data-testid="status-item-major-compaction"]');
    await expect(majorCompaction).toBeVisible();
    const majorCompactionStatus = await majorCompaction.getAttribute('data-status');
    expect(majorCompactionStatus).toBe('implemented');
  });

  test('TC5: Persistence claim - Content accurately describes SSTable-based persistence', async ({ page }) => {
    // Verify Persistent Storage feature is present and accurate
    const featuresSection = page.locator('[data-testid="features-section"], #features');
    await expect(featuresSection).toBeVisible();

    // Find Persistent Storage feature card
    const persistentCard = featuresSection.locator('.feature-card').filter({ hasText: 'Persistent Storage' });
    await expect(persistentCard).toBeVisible();

    // Verify Persistent Storage heading
    const persistentHeading = persistentCard.locator('h3');
    await expect(persistentHeading).toHaveText('Persistent Storage');

    // Verify description mentions SSTable file format
    // Per PRD: "Data survives restarts via SSTable file format"
    const persistentDescription = persistentCard.locator('p');
    await expect(persistentDescription).toContainText('Data survives restarts via SSTable file format');

    // Verify meta description also mentions persistence
    const metaDescription = await page.locator('meta[name="description"]').getAttribute('content');
    expect(metaDescription?.toLowerCase()).toContain('persistent');
  });

  test('Comprehensive accuracy: All technical claims match PRD requirements', async ({ page }) => {
    // This test provides a comprehensive validation of all technical claims

    // 1. Verify Async I/O / Tokio claim
    const asyncCard = page.locator('.feature-card').filter({ hasText: 'Async I/O' });
    await expect(asyncCard).toBeVisible();
    const asyncDescription = asyncCard.locator('p');
    await expect(asyncDescription).toContainText('Built on Tokio for efficient concurrent connections');

    // 2. Verify Configurable / TOML claim
    const configCard = page.locator('.feature-card').filter({ hasText: 'Configurable' });
    await expect(configCard).toBeVisible();
    const configDescription = configCard.locator('p');
    await expect(configDescription).toContainText('TOML-based configuration for tuning performance');

    // 3. Verify all default configuration values per PRD
    // Listen Address: 0.0.0.0:12333
    const listenValue = page.locator('[data-testid="config-value-listen-address"]');
    await expect(listenValue).toHaveText('0.0.0.0:12333');

    // Memtable Max Size: 4MB
    const memtableValue = page.locator('[data-testid="config-value-memtable-size"]');
    await expect(memtableValue).toHaveText('4MB');

    // SSTable Max Size: 100MB
    const sstableValue = page.locator('[data-testid="config-value-sstable-size"]');
    await expect(sstableValue).toHaveText('100MB');

    // Work Directory: /tmp/mirdb
    const workDirValue = page.locator('[data-testid="config-value-work-directory"]');
    await expect(workDirValue).toHaveText('/tmp/mirdb');

    // 4. Verify project status accuracy
    // Async Networking - implemented
    const asyncNetworking = page.locator('[data-testid="status-item-async-networking"]');
    await expect(asyncNetworking).toHaveAttribute('data-status', 'implemented');

    // Memtable (Skip List) - implemented
    const memtable = page.locator('[data-testid="status-item-memtable"]');
    await expect(memtable).toHaveAttribute('data-status', 'implemented');

    // Raft Consensus - planned (not implemented)
    const raftConsensus = page.locator('[data-testid="status-item-raft-consensus"]');
    await expect(raftConsensus).toHaveAttribute('data-status', 'planned');
  });
});
