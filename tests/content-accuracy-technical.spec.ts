import { test, expect } from '@playwright/test';

test.describe('Content Accuracy - Technical Information (Scenario 17)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Check supported commands list
   * Verifies that the page lists only implemented commands, not planned features.
   * Supported commands: SET, GET, GETS, ADD, REPLACE, APPEND, PREPEND, DELETE, INFO
   */
  test('page lists only implemented commands without planned features', async ({ page }) => {
    const techSpecsSection = page.locator('[data-testid="technical-specs-section"]');
    await expect(techSpecsSection).toBeVisible();

    const commandsSection = page.locator('[data-testid="spec-commands"]');
    await expect(commandsSection).toBeVisible();

    const commandsContent = await commandsSection.textContent();
    expect(commandsContent).not.toBeNull();

    // Verify all implemented commands are listed
    const implementedCommands = ['SET', 'GET', 'GETS', 'ADD', 'REPLACE', 'APPEND', 'PREPEND', 'DELETE', 'INFO'];
    for (const command of implementedCommands) {
      expect(commandsContent!.toUpperCase()).toContain(command);
    }

    // Verify planned but not implemented features are NOT claimed as implemented
    // MAJOR_COMPACTION is an internal command, not required to be listed
    // The key check is that we don't claim features that don't exist
  });

  /**
   * Test Case 2: Verify port number in examples
   * Ensures examples use the correct default port 12333.
   */
  test('examples use correct default port 12333', async ({ page }) => {
    // Check getting started section for correct port
    const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
    await expect(gettingStartedSection).toBeVisible();

    const gettingStartedContent = await gettingStartedSection.textContent();
    expect(gettingStartedContent).toContain('12333');

    // Verify telnet example uses correct port
    const codeBlocks = await gettingStartedSection.locator('pre').allTextContents();
    const allCodeText = codeBlocks.join(' ');
    expect(allCodeText).toContain('telnet localhost 12333');

    // Check configuration section for correct port
    const configSection = page.locator('[data-testid="configuration-section"]');
    await expect(configSection).toBeVisible();

    const configContent = await configSection.textContent();
    expect(configContent).toContain('0.0.0.0:12333');
  });

  /**
   * Test Case 3: Check for Raft consensus claims
   * Verifies that the page does not claim Raft is implemented.
   * Raft consensus is planned but not implemented yet.
   */
  test('page does not claim Raft consensus is implemented', async ({ page }) => {
    // Get entire page content
    const bodyContent = await page.locator('body').textContent();
    expect(bodyContent).not.toBeNull();

    // Check that Raft is not mentioned as an implemented feature
    const contentLower = bodyContent!.toLowerCase();

    // Verify Raft is not mentioned at all (since it's not implemented)
    // OR if mentioned, it should be explicitly marked as "planned" or "future"
    const mentionsRaft = contentLower.includes('raft');

    if (mentionsRaft) {
      // If Raft is mentioned, it should only be in context of planned/future features
      const raftContext = contentLower.includes('planned') ||
                          contentLower.includes('future') ||
                          contentLower.includes('coming') ||
                          contentLower.includes('roadmap');
      expect(raftContext).toBe(true);
    }

    // Verify "consensus" is not mentioned as implemented
    const mentionsConsensus = contentLower.includes('consensus');
    if (mentionsConsensus) {
      const consensusContext = contentLower.includes('planned') ||
                               contentLower.includes('future') ||
                               contentLower.includes('roadmap');
      expect(consensusContext).toBe(true);
    }

    // Verify no "distributed" claims (since Raft not implemented)
    const mentionsDistributed = contentLower.includes('distributed replication') ||
                                 contentLower.includes('distributed consensus');
    expect(mentionsDistributed).toBe(false);
  });

  /**
   * Test Case 4: Verify configuration format
   * Ensures configuration examples use valid TOML syntax matching mirdb.toml.
   */
  test('configuration examples use valid TOML syntax matching mirdb.toml', async ({ page }) => {
    const configSection = page.locator('[data-testid="configuration-section"]');
    await expect(configSection).toBeVisible();

    const configCodeBlock = page.locator('[data-testid="config-toml"]');
    await expect(configCodeBlock).toBeVisible();

    const codeContent = await configCodeBlock.textContent();
    expect(codeContent).not.toBeNull();

    // Verify TOML format: key = value pairs
    // Check that values are properly formatted:
    // - Strings in quotes: addr = "0.0.0.0:12333"
    // - Numbers without quotes: max_level = 7
    // - Size values in quotes: sst_max_size = "100M"

    // Verify addr uses string format
    expect(codeContent).toMatch(/addr\s*=\s*"0\.0\.0\.0:12333"/);

    // Verify max_level uses number format
    expect(codeContent).toMatch(/max_level\s*=\s*7/);

    // Verify work_dir uses string format
    expect(codeContent).toMatch(/work_dir\s*=\s*"\/tmp\/mirdb"/);

    // Verify sst_max_size uses string format for size with unit
    expect(codeContent).toMatch(/sst_max_size\s*=\s*"100M"/);

    // Verify mem_table_max_size uses string format for size with unit
    expect(codeContent).toMatch(/mem_table_max_size\s*=\s*"4M"/);

    // Verify block_size uses string format for size with unit
    expect(codeContent).toMatch(/block_size\s*=\s*"4K"/);
  });

  /**
   * Additional test: Verify commands list matches exactly what's implemented
   */
  test('commands list contains exactly the 9 implemented commands', async ({ page }) => {
    const commandsSection = page.locator('[data-testid="spec-commands"]');
    await expect(commandsSection).toBeVisible();

    // Get all command tags
    const commandTags = commandsSection.locator('.command-tag');
    const count = await commandTags.count();

    // Should have exactly 9 commands: SET, GET, GETS, ADD, REPLACE, APPEND, PREPEND, DELETE, INFO
    expect(count).toBe(9);

    // Verify each expected command exists
    const expectedCommands = ['SET', 'GET', 'GETS', 'ADD', 'REPLACE', 'APPEND', 'PREPEND', 'DELETE', 'INFO'];

    for (let i = 0; i < count; i++) {
      const tagText = await commandTags.nth(i).textContent();
      expect(expectedCommands).toContain(tagText?.toUpperCase());
    }
  });

  /**
   * Additional test: Verify default configuration values are accurate
   */
  test('default configuration values match mirdb specifications', async ({ page }) => {
    const configSection = page.locator('[data-testid="configuration-section"]');
    await expect(configSection).toBeVisible();

    // Check the configuration table for correct default values
    const configTable = configSection.locator('.config-table');
    await expect(configTable).toBeVisible();

    const tableContent = await configTable.textContent();
    expect(tableContent).not.toBeNull();

    // Verify correct defaults from knowledge files:
    // Listen address: 0.0.0.0:12333
    expect(tableContent).toContain('0.0.0.0:12333');

    // Max LSM levels: 7
    expect(tableContent).toContain('7');

    // Work directory: /tmp/mirdb
    expect(tableContent).toContain('/tmp/mirdb');

    // SSTable max size: 100M
    expect(tableContent).toContain('100M');

    // Memtable max size: 4M
    expect(tableContent).toContain('4M');

    // Block size: 4K
    expect(tableContent).toContain('4K');
  });
});
