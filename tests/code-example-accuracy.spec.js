// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Integration Tests for Code Example Accuracy
 * Scenario: Verify all code examples are accurate and copy-paste functional
 * Related Requirements: REQ-4, US-2, Success Criteria
 *
 * These tests verify that:
 * 1. Installation commands are accurate
 * 2. SET, GET, DELETE command examples match actual memcached protocol syntax
 * 3. TOML configuration example is valid and works with MirDB
 */

test.describe('Code Example Accuracy', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Verify installation command accuracy
   * Input: Copy and run installation command
   * Expected: Installation command successfully installs MirDB
   */
  test('TC1: Installation commands are accurate and functional', async ({ page }) => {
    // Navigate to getting-started section
    const gettingStartedSection = page.locator('#getting-started');
    await gettingStartedSection.scrollIntoViewIfNeeded();
    await expect(gettingStartedSection).toBeVisible();

    // Check cargo install command
    const cargoInstallBlock = page.locator('[data-testid="cargo-install"]');
    await expect(cargoInstallBlock).toBeVisible();

    const cargoInstallCode = await cargoInstallBlock.locator('code').textContent();

    // Verify cargo install command syntax is correct
    // Format: cargo install <package-name>
    expect(cargoInstallCode).toMatch(/cargo\s+install\s+mirdb/);

    // Check build from source commands
    const buildFromSourceBlock = page.locator('[data-testid="build-from-source"]');
    await expect(buildFromSourceBlock).toBeVisible();

    const buildFromSourceCode = await buildFromSourceBlock.locator('code').textContent();

    // Verify git clone command syntax
    expect(buildFromSourceCode).toMatch(/git\s+clone\s+https:\/\/github\.com\/[\w-]+\/mirdb/);

    // Verify cd command
    expect(buildFromSourceCode).toMatch(/cd\s+mirdb/);

    // Verify cargo build command syntax
    expect(buildFromSourceCode).toMatch(/cargo\s+build\s+--release/);

    // Verify server startup command
    const serverStartupBlock = page.locator('[data-testid="server-startup"]');
    await expect(serverStartupBlock).toBeVisible();

    const serverStartupCode = await serverStartupBlock.locator('code').textContent();

    // Verify server binary path is correct
    expect(serverStartupCode).toMatch(/\.\/target\/release\/mirdb-server/);

    // Verify config flag usage
    expect(serverStartupCode).toMatch(/-c\s+\S+\.toml/);
  });

  /**
   * Test Case 2: Verify SET command example syntax
   * Input: Copy and execute SET command example
   * Expected: SET command executes successfully against running MirDB instance
   */
  test('TC2: SET command example matches memcached protocol syntax', async ({ page }) => {
    // Navigate to code examples section
    const codeExamplesSection = page.locator('[data-testid="code-examples-section"]');
    await codeExamplesSection.scrollIntoViewIfNeeded();
    await expect(codeExamplesSection).toBeVisible();

    // Get SET example
    const setExample = page.locator('[data-testid="set-example"]');
    await expect(setExample).toBeVisible();

    const setCodeBlock = page.locator('[data-testid="set-code-block"]');
    const setCodeContent = await setCodeBlock.textContent();

    // Memcached SET protocol: set <key> <flags> <ttl> <bytes>\r\n<data>\r\n
    // Verify the example shows correct syntax comment
    expect(setCodeContent).toContain('set <key> <flags> <ttl> <bytes>');

    // Verify the actual SET command example has correct format
    // Expected format: set mykey 0 0 5
    const setCommandMatch = setCodeContent.match(/set\s+(\w+)\s+(\d+)\s+(\d+)\s+(\d+)/);
    expect(setCommandMatch).toBeTruthy();

    // Extract and validate the parameters
    const [, key, flags, ttl, bytes] = setCommandMatch;
    expect(key).toBe('mykey');
    expect(parseInt(flags)).toBeGreaterThanOrEqual(0); // flags should be non-negative integer
    expect(parseInt(ttl)).toBeGreaterThanOrEqual(0); // ttl should be non-negative integer
    expect(parseInt(bytes)).toBeGreaterThan(0); // bytes should be positive integer

    // Verify the data follows the command (should be 'hello' which is 5 bytes)
    expect(setCodeContent).toContain('hello');

    // The bytes count should match the data length
    // 'hello' = 5 bytes, which matches bytes=5 in the command
    expect(parseInt(bytes)).toBe(5);

    // Verify expected response is shown
    expect(setCodeContent).toContain('STORED');
  });

  /**
   * Test Case 3: Verify GET command example syntax
   * Input: Copy and execute GET command example
   * Expected: GET command retrieves value successfully from running MirDB instance
   */
  test('TC3: GET command example matches memcached protocol syntax', async ({ page }) => {
    // Navigate to code examples section
    const codeExamplesSection = page.locator('[data-testid="code-examples-section"]');
    await codeExamplesSection.scrollIntoViewIfNeeded();
    await expect(codeExamplesSection).toBeVisible();

    // Get GET example
    const getExample = page.locator('[data-testid="get-example"]');
    await expect(getExample).toBeVisible();

    const getCodeBlock = page.locator('[data-testid="get-code-block"]');
    const getCodeContent = await getCodeBlock.textContent();

    // Memcached GET protocol: get <key1> [<key2> ...]\r\n
    // Verify the example shows correct syntax comment
    expect(getCodeContent).toContain('get <key');

    // Verify the actual GET command has correct format
    // Expected format: get mykey
    const getCommandMatch = getCodeContent.match(/get\s+(\w+)/);
    expect(getCommandMatch).toBeTruthy();

    // Verify the key is the same as used in SET example for consistency
    const [, key] = getCommandMatch;
    expect(key).toBe('mykey');

    // Verify expected response format is shown
    // Response format: VALUE <key> <flags> <bytes>\r\n<data>\r\nEND\r\n
    expect(getCodeContent).toContain('VALUE');
    expect(getCodeContent).toContain('mykey');

    // Verify the VALUE response line has correct format
    const valueLineMatch = getCodeContent.match(/VALUE\s+(\w+)\s+(\d+)\s+(\d+)/);
    expect(valueLineMatch).toBeTruthy();

    const [, valueKey, valueFlags, valueBytes] = valueLineMatch;
    expect(valueKey).toBe('mykey');
    expect(parseInt(valueFlags)).toBe(0); // Should match flags from SET
    expect(parseInt(valueBytes)).toBe(5); // Should match bytes from SET

    // Verify the data and END marker
    expect(getCodeContent).toContain('hello');
    expect(getCodeContent).toContain('END');
  });

  /**
   * Test Case 4: Verify DELETE command example syntax
   * Input: Copy and execute DELETE command example
   * Expected: DELETE command removes value successfully from running MirDB instance
   */
  test('TC4: DELETE command example matches memcached protocol syntax', async ({ page }) => {
    // Navigate to code examples section
    const codeExamplesSection = page.locator('[data-testid="code-examples-section"]');
    await codeExamplesSection.scrollIntoViewIfNeeded();
    await expect(codeExamplesSection).toBeVisible();

    // Get DELETE example
    const deleteExample = page.locator('[data-testid="delete-example"]');
    await expect(deleteExample).toBeVisible();

    const deleteCodeBlock = page.locator('[data-testid="delete-code-block"]');
    const deleteCodeContent = await deleteCodeBlock.textContent();

    // Memcached DELETE protocol: delete <key> [noreply]\r\n
    // Verify the example shows correct syntax comment
    expect(deleteCodeContent).toContain('delete <key>');

    // Verify optional noreply is documented
    expect(deleteCodeContent).toContain('[noreply]');

    // Verify the actual DELETE command has correct format
    // Expected format: delete mykey
    const deleteCommandMatch = deleteCodeContent.match(/delete\s+(\w+)/);
    expect(deleteCommandMatch).toBeTruthy();

    // Verify the key is the same as used in other examples for consistency
    const [, key] = deleteCommandMatch;
    expect(key).toBe('mykey');

    // Verify expected response is shown
    // Response: DELETED
    expect(deleteCodeContent).toContain('DELETED');
  });

  /**
   * Test Case 5: Verify TOML configuration example is valid
   * Input: Verify TOML configuration example syntax
   * Expected: TOML configuration example is valid TOML and works with MirDB
   */
  test('TC5: TOML configuration example is valid and contains all required parameters', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('[data-testid="configuration-section"]');
    await configSection.scrollIntoViewIfNeeded();
    await expect(configSection).toBeVisible();

    // Get TOML config example
    const tomlConfigBlock = page.locator('[data-testid="toml-config-example"]');
    await expect(tomlConfigBlock).toBeVisible();

    const tomlContent = await tomlConfigBlock.textContent();

    // Verify all required MirDB configuration parameters are present
    // Network configuration
    expect(tomlContent).toMatch(/addr\s*=\s*"[^"]+"/);
    expect(tomlContent).toContain('0.0.0.0:12333');

    // Storage configuration
    expect(tomlContent).toMatch(/max_level\s*=\s*\d+/);
    expect(tomlContent).toMatch(/work_dir\s*=\s*"[^"]+"/);

    // SSTable configuration
    expect(tomlContent).toMatch(/sst_max_size\s*=\s*"[^"]+"/);

    // Memtable configuration
    expect(tomlContent).toMatch(/mem_table_max_size\s*=\s*"[^"]+"/);
    expect(tomlContent).toMatch(/mem_table_max_height\s*=\s*\d+/);

    // Immutable memtable configuration
    expect(tomlContent).toMatch(/imm_mem_table_max_count\s*=\s*\d+/);

    // Block configuration
    expect(tomlContent).toMatch(/block_size\s*=\s*"[^"]+"/);
    expect(tomlContent).toMatch(/block_restart_interval\s*=\s*\d+/);

    // Compaction configuration
    expect(tomlContent).toMatch(/l0_compaction_trigger\s*=\s*\d+/);

    // Thread configuration
    expect(tomlContent).toMatch(/thread_sleep_ms\s*=\s*\d+/);

    // Verify size values use correct MirDB size unit format (K, M, G, T)
    // sst_max_size should be like "100M"
    const sstSizeMatch = tomlContent.match(/sst_max_size\s*=\s*"(\d+[KMGT])"/);
    expect(sstSizeMatch).toBeTruthy();

    // mem_table_max_size should be like "4M"
    const memSizeMatch = tomlContent.match(/mem_table_max_size\s*=\s*"(\d+[KMGT])"/);
    expect(memSizeMatch).toBeTruthy();

    // block_size should be like "4K"
    const blockSizeMatch = tomlContent.match(/block_size\s*=\s*"(\d+[KMGT])"/);
    expect(blockSizeMatch).toBeTruthy();

    // Verify the values match MirDB's default configuration
    expect(tomlContent).toContain('max_level = 7');
    expect(tomlContent).toContain('work_dir = "/tmp/mirdb"');
    expect(tomlContent).toContain('sst_max_size = "100M"');
    expect(tomlContent).toContain('mem_table_max_size = "4M"');
    expect(tomlContent).toContain('mem_table_max_height = 32');
    expect(tomlContent).toContain('imm_mem_table_max_count = 16');
    expect(tomlContent).toContain('block_size = "4K"');
    expect(tomlContent).toContain('block_restart_interval = 16');
    expect(tomlContent).toContain('l0_compaction_trigger = 4');
    expect(tomlContent).toContain('thread_sleep_ms = 500');
  });

  /**
   * Additional Test: Verify configuration parameter table matches TOML example
   */
  test('TC5b: Configuration parameter table is consistent with TOML example', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('[data-testid="configuration-section"]');
    await configSection.scrollIntoViewIfNeeded();
    await expect(configSection).toBeVisible();

    // Get parameter table
    const paramsTable = page.locator('[data-testid="config-params-table"]');
    await expect(paramsTable).toBeVisible();

    const tableContent = await paramsTable.textContent();

    // Verify key parameters are documented in the table
    expect(tableContent).toContain('addr');
    expect(tableContent).toContain('0.0.0.0:12333');

    expect(tableContent).toContain('max_level');
    expect(tableContent).toContain('7');

    expect(tableContent).toContain('work_dir');
    expect(tableContent).toContain('/tmp/mirdb');

    expect(tableContent).toContain('sst_max_size');
    expect(tableContent).toContain('100MB');

    expect(tableContent).toContain('mem_table_max_size');
    expect(tableContent).toContain('4MB');

    expect(tableContent).toContain('block_size');
    expect(tableContent).toContain('4KB');
  });

  /**
   * Additional Test: Verify memcached commands list matches actual supported commands
   */
  test('TC6: Supported commands list matches MirDB implementation', async ({ page }) => {
    // Navigate to project status section
    const projectStatusSection = page.locator('[data-testid="project-status-section"]');
    await projectStatusSection.scrollIntoViewIfNeeded();
    await expect(projectStatusSection).toBeVisible();

    // Check storage commands
    const storageCommandsList = page.locator('[data-testid="storage-commands-list"]');
    await expect(storageCommandsList).toBeVisible();

    const storageContent = await storageCommandsList.textContent();

    // Verify all storage commands from MirDB parser are listed
    expect(storageContent).toContain('SET');
    expect(storageContent).toContain('ADD');
    expect(storageContent).toContain('REPLACE');
    expect(storageContent).toContain('APPEND');
    expect(storageContent).toContain('PREPEND');

    // Check retrieval commands
    const retrievalCommandsList = page.locator('[data-testid="retrieval-commands-list"]');
    await expect(retrievalCommandsList).toBeVisible();

    const retrievalContent = await retrievalCommandsList.textContent();

    // Verify all retrieval commands from MirDB parser are listed
    expect(retrievalContent).toContain('GET');
    expect(retrievalContent).toContain('GETS');

    // Check deletion commands
    const deletionCommandsList = page.locator('[data-testid="deletion-commands-list"]');
    await expect(deletionCommandsList).toBeVisible();

    const deletionContent = await deletionCommandsList.textContent();

    // Verify deletion command from MirDB parser is listed
    expect(deletionContent).toContain('DELETE');

    // Check MirDB-specific commands
    const mirdbCommandsList = page.locator('[data-testid="mirdb-commands-list"]');
    await expect(mirdbCommandsList).toBeVisible();

    const mirdbContent = await mirdbCommandsList.textContent();

    // Verify MirDB-specific commands from parser are listed
    expect(mirdbContent).toContain('INFO');
    expect(mirdbContent).toContain('MAJOR_COMPACTION');
  });

  /**
   * Additional Test: Verify code examples are copy-paste ready (no HTML artifacts)
   */
  test('TC7: Code examples contain clean, copy-paste ready code', async ({ page }) => {
    // Navigate to code examples section
    const codeExamplesSection = page.locator('[data-testid="code-examples-section"]');
    await codeExamplesSection.scrollIntoViewIfNeeded();

    // Get all code blocks
    const setCodeBlock = page.locator('[data-testid="set-code-block"]');
    const getCodeBlock = page.locator('[data-testid="get-code-block"]');
    const deleteCodeBlock = page.locator('[data-testid="delete-code-block"]');

    // Extract text content (what would be copied)
    const setCode = await setCodeBlock.textContent();
    const getCode = await getCodeBlock.textContent();
    const deleteCode = await deleteCodeBlock.textContent();

    // Verify no actual HTML tags leaked into visible text
    // Note: <key>, <flags>, etc. are intentional protocol syntax placeholders
    // We check for common HTML tags that would indicate rendering issues
    const htmlTagPattern = /<(div|span|code|pre|html|body|head|script|style|p|a|br|hr)\b/i;
    expect(setCode).not.toMatch(htmlTagPattern);
    expect(getCode).not.toMatch(htmlTagPattern);
    expect(deleteCode).not.toMatch(htmlTagPattern);

    // Verify no undefined or null values
    expect(setCode).not.toContain('undefined');
    expect(getCode).not.toContain('undefined');
    expect(deleteCode).not.toContain('undefined');
    expect(setCode).not.toContain('null');
    expect(getCode).not.toContain('null');
    expect(deleteCode).not.toContain('null');

    // Verify examples contain expected protocol syntax documentation
    // The placeholders <key>, <flags>, <ttl>, <bytes>, <data> should be present
    expect(setCode).toContain('<key>');
    expect(setCode).toContain('<flags>');
    expect(setCode).toContain('<ttl>');
    expect(setCode).toContain('<bytes>');
    expect(getCode).toContain('<key');
    expect(deleteCode).toContain('<key>');
  });
});
