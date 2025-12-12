import { test, expect } from '@playwright/test';

/**
 * E2E Tests for Content Accuracy - MirDB Information
 * Scenario: Verify that all displayed information about MirDB is accurate
 * and matches the actual product capabilities
 *
 * Based on knowledge base specifications:
 * - MirDB is a persistent key-value store written in Rust
 * - Implements Memcached protocol
 * - Default port: 12333
 * - Commands: SET, GET, DELETE, ADD, REPLACE, APPEND, PREPEND, INFO, MAJOR_COMPACTION
 */

test.describe('Content Accuracy - MirDB Information', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to homepage before each test
    await page.goto('/');
  });

  /**
   * Test Case 1: Review product description
   * Input: Review product description
   * Expected: Description accurately states MirDB is a persistent key-value store with memcached protocol
   */
  test('should accurately describe MirDB as a persistent key-value store with memcached protocol', async ({ page }) => {
    // Verify hero section exists and contains accurate product description
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Verify headline accurately describes MirDB as a key-value store
    const headline = page.locator('[data-testid="hero-headline"]');
    await expect(headline).toBeVisible();
    const headlineText = await headline.textContent();
    expect(headlineText).toBeTruthy();

    // Verify headline mentions "Persistent Key-Value Store" - accurate per knowledge base
    expect(headlineText).toContain('Persistent Key-Value Store');
    expect(headlineText).toContain('MirDB');

    // Verify subheadline mentions memcached compatibility - accurate per knowledge base
    const subheadline = page.locator('[data-testid="hero-subheadline"]');
    await expect(subheadline).toBeVisible();
    const subheadlineText = await subheadline.textContent();
    expect(subheadlineText).toBeTruthy();

    // Knowledge base states: "Compatible with the standard memcached text protocol"
    expect(subheadlineText?.toLowerCase()).toContain('memcached');

    // Knowledge base states: "MirDB persists data to disk"
    expect(subheadlineText?.toLowerCase()).toMatch(/(persist|durabl|disk|storage)/);
  });

  /**
   * Test Case 2: Check default port mentioned
   * Input: Check default port mentioned
   * Expected: Default port is correctly stated as 12333
   */
  test('should correctly state default port as 12333', async ({ page }) => {
    // Knowledge base states: "Listen address: 0.0.0.0:12333"
    // Check in Getting Started section
    const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
    await expect(gettingStartedSection).toBeVisible();

    // Verify the usage code block shows connection to port 12333
    const usageCodeBlock = page.locator('[data-testid="usage-code-block"]');
    await expect(usageCodeBlock).toBeVisible();
    const usageContent = await usageCodeBlock.textContent();
    expect(usageContent).toBeTruthy();

    // Verify port 12333 is mentioned in the telnet example
    expect(usageContent).toContain('12333');
    expect(usageContent).toMatch(/localhost\s+12333|localhost:12333/);

    // Check in Configuration section
    const configSection = page.locator('[data-testid="configuration-section"]');
    await expect(configSection).toBeVisible();

    // Verify the addr parameter shows 12333
    const addrParam = page.locator('[data-testid="config-param-addr"]');
    await expect(addrParam).toBeVisible();
    const addrText = await addrParam.textContent();
    expect(addrText).toContain('12333');

    // Verify the config example code block shows correct default address
    const configCodeBlock = page.locator('[data-testid="config-example-code-block"]');
    const configContent = await configCodeBlock.textContent();
    expect(configContent).toContain('0.0.0.0:12333');
  });

  /**
   * Test Case 3: Verify supported commands list
   * Input: Verify supported commands list
   * Expected: All listed commands (SET, GET, DELETE, ADD, REPLACE, APPEND, PREPEND, INFO, MAJOR_COMPACTION) are correct
   */
  test('should list all correct supported commands: SET, GET, DELETE, ADD, REPLACE, APPEND, PREPEND, INFO, MAJOR_COMPACTION', async ({ page }) => {
    // Knowledge base lists these commands:
    // Storage: SET, ADD, REPLACE, APPEND, PREPEND
    // Retrieval: GET, GETS
    // Deletion: DELETE
    // MirDB-Specific: INFO, MAJOR_COMPACTION

    const commandsSection = page.locator('[data-testid="commands-section"]');
    await expect(commandsSection).toBeVisible();

    // Define all expected commands from the knowledge base
    const expectedCommands = [
      { id: 'command-set', name: 'SET', category: 'Storage' },
      { id: 'command-get', name: 'GET', category: 'Retrieval' },
      { id: 'command-delete', name: 'DELETE', category: 'Deletion' },
      { id: 'command-add', name: 'ADD', category: 'Storage' },
      { id: 'command-replace', name: 'REPLACE', category: 'Storage' },
      { id: 'command-append', name: 'APPEND', category: 'Storage' },
      { id: 'command-prepend', name: 'PREPEND', category: 'Storage' },
      { id: 'command-info', name: 'INFO', category: 'Admin' },
      { id: 'command-major-compaction', name: 'MAJOR_COMPACTION', category: 'Admin' },
    ];

    // Verify each expected command is present and documented
    for (const cmd of expectedCommands) {
      const commandElement = page.locator(`[data-testid="${cmd.id}"]`);
      await expect(commandElement, `${cmd.name} command should be visible`).toBeVisible();

      // Verify the command heading is correct
      const commandHeading = commandElement.locator('h4');
      const headingText = await commandHeading.textContent();
      expect(headingText?.toUpperCase()).toContain(cmd.name.replace('_', '_'));
    }

    // Verify command categories are accurate per knowledge base
    // Storage Commands
    const storageHeading = page.locator('[data-testid="storage-commands-heading"]');
    await expect(storageHeading).toBeVisible();
    await expect(storageHeading).toContainText('Storage Commands');

    // Retrieval Commands
    const retrievalHeading = page.locator('[data-testid="retrieval-commands-heading"]');
    await expect(retrievalHeading).toBeVisible();
    await expect(retrievalHeading).toContainText('Retrieval Commands');

    // Deletion Commands
    const deletionHeading = page.locator('[data-testid="deletion-commands-heading"]');
    await expect(deletionHeading).toBeVisible();
    await expect(deletionHeading).toContainText('Deletion Commands');

    // Admin Commands (MirDB-Specific)
    const adminHeading = page.locator('[data-testid="admin-commands-heading"]');
    await expect(adminHeading).toBeVisible();
    await expect(adminHeading).toContainText('Admin Commands');
  });

  /**
   * Test Case 4: Check code examples accuracy
   * Input: Check code examples accuracy
   * Expected: Code examples use correct syntax and show accurate expected responses
   */
  test('should display code examples with correct syntax and accurate expected responses', async ({ page }) => {
    // Knowledge base specifies:
    // SET syntax: set <key> <flags> <ttl> <bytes> [noreply]\r\n<data>\r\n
    // Response: STORED
    // GET syntax: get <key1> [<key2> ...]\r\n
    // Response: VALUE <key> <flags> <bytes>\r\n<data>\r\nEND
    // DELETE syntax: delete <key> [noreply]\r\n
    // Response: DELETED

    // Verify SET command example has correct syntax
    const setCommand = page.locator('[data-testid="command-set"]');
    await expect(setCommand).toBeVisible();
    const setCode = await setCommand.locator('code').textContent();
    expect(setCode).toBeTruthy();

    // Verify SET syntax includes key, flags, ttl, bytes parameters
    expect(setCode?.toLowerCase()).toContain('set');
    expect(setCode).toMatch(/<key>|mykey/);
    expect(setCode).toMatch(/<flags>|0/);
    expect(setCode).toMatch(/<ttl>|0/);
    expect(setCode).toMatch(/<bytes>|\d+/);

    // Verify correct response code per knowledge base
    expect(setCode).toContain('STORED');

    // Verify GET command example has correct syntax
    const getCommand = page.locator('[data-testid="command-get"]');
    await expect(getCommand).toBeVisible();
    const getCode = await getCommand.locator('code').textContent();
    expect(getCode).toBeTruthy();

    // Verify GET syntax
    expect(getCode?.toLowerCase()).toContain('get');

    // Verify correct response format per knowledge base
    expect(getCode).toContain('VALUE');
    expect(getCode).toContain('END');

    // Verify DELETE command example has correct syntax
    const deleteCommand = page.locator('[data-testid="command-delete"]');
    await expect(deleteCommand).toBeVisible();
    const deleteCode = await deleteCommand.locator('code').textContent();
    expect(deleteCode).toBeTruthy();

    // Verify DELETE syntax
    expect(deleteCode?.toLowerCase()).toContain('delete');

    // Verify correct response code per knowledge base
    expect(deleteCode).toContain('DELETED');

    // Verify INFO command example (MirDB-specific)
    const infoCommand = page.locator('[data-testid="command-info"]');
    await expect(infoCommand).toBeVisible();
    const infoCode = await infoCommand.locator('code').textContent();
    expect(infoCode).toBeTruthy();
    expect(infoCode?.toLowerCase()).toContain('info');

    // Verify MAJOR_COMPACTION command example (MirDB-specific)
    const majorCompactionCommand = page.locator('[data-testid="command-major-compaction"]');
    await expect(majorCompactionCommand).toBeVisible();
    const majorCompactionCode = await majorCompactionCommand.locator('code').textContent();
    expect(majorCompactionCode).toBeTruthy();
    expect(majorCompactionCode?.toLowerCase()).toContain('major_compaction');

    // Verify Getting Started code examples are accurate
    const usageCodeBlock = page.locator('[data-testid="usage-code-block"]');
    await expect(usageCodeBlock).toBeVisible();
    const usageCode = await usageCodeBlock.textContent();
    expect(usageCode).toBeTruthy();

    // Verify usage example shows correct SET/GET sequence with proper responses
    expect(usageCode).toContain('set mykey 0 0 5');
    expect(usageCode).toContain('hello');
    expect(usageCode).toContain('STORED');
    expect(usageCode).toContain('get mykey');
    expect(usageCode).toContain('VALUE mykey 0 5');
    expect(usageCode).toContain('END');
  });

  /**
   * Additional test: Verify technical specifications accuracy
   * This tests Step 2 of the scenario: Verify technical details (port, defaults, etc.)
   */
  test('should display accurate technical specifications including default configurations', async ({ page }) => {
    // Knowledge base states these defaults:
    // - Listen address: 0.0.0.0:12333
    // - Work directory: /tmp/mirdb
    // - Max LSM levels: 7
    // - SSTable max size: 100MB
    // - Memtable max size: 4MB
    // - Block size: 4KB

    const configSection = page.locator('[data-testid="configuration-section"]');
    await expect(configSection).toBeVisible();

    // Verify work_dir default is /tmp/mirdb
    const workDirParam = page.locator('[data-testid="config-param-work-dir"]');
    await expect(workDirParam).toBeVisible();
    const workDirText = await workDirParam.textContent();
    expect(workDirText).toContain('/tmp/mirdb');

    // Verify max_level default is 7
    const maxLevelParam = page.locator('[data-testid="config-param-max-level"]');
    await expect(maxLevelParam).toBeVisible();
    const maxLevelText = await maxLevelParam.textContent();
    expect(maxLevelText).toContain('7');

    // Verify sst_max_size default is 100M
    const sstMaxSizeParam = page.locator('[data-testid="config-param-sst-max-size"]');
    await expect(sstMaxSizeParam).toBeVisible();
    const sstMaxSizeText = await sstMaxSizeParam.textContent();
    expect(sstMaxSizeText).toContain('100M');

    // Verify mem_table_max_size default is 4M
    const memTableMaxSizeParam = page.locator('[data-testid="config-param-mem-table-max-size"]');
    await expect(memTableMaxSizeParam).toBeVisible();
    const memTableMaxSizeText = await memTableMaxSizeParam.textContent();
    expect(memTableMaxSizeText).toContain('4M');

    // Verify block_size default is 4K
    const blockSizeParam = page.locator('[data-testid="config-param-block-size"]');
    await expect(blockSizeParam).toBeVisible();
    const blockSizeText = await blockSizeParam.textContent();
    expect(blockSizeText).toContain('4K');
  });

  /**
   * Additional test: Verify response codes are documented correctly
   * Knowledge base lists: STORED, NOT_STORED, EXISTS, NOT_FOUND, DELETED, ERROR, CLIENT_ERROR, SERVER_ERROR
   */
  test('should display accurate response codes documentation', async ({ page }) => {
    const commandsSection = page.locator('[data-testid="commands-section"]');
    await expect(commandsSection).toBeVisible();

    // Verify Response Codes section exists
    const responseCodesHeading = page.locator('[data-testid="response-codes-heading"]');
    await expect(responseCodesHeading).toBeVisible();
    await expect(responseCodesHeading).toContainText('Response Codes');

    // Get the response codes list content
    const responseCodesSection = commandsSection.locator('ul').filter({ hasText: 'STORED' });
    await expect(responseCodesSection).toBeVisible();
    const responseCodesText = await responseCodesSection.textContent();
    expect(responseCodesText).toBeTruthy();

    // Verify all response codes from knowledge base are documented
    const expectedResponseCodes = [
      'STORED',
      'NOT_STORED',
      'EXISTS',
      'NOT_FOUND',
      'DELETED',
      'ERROR',
      'CLIENT_ERROR',
      'SERVER_ERROR',
    ];

    for (const code of expectedResponseCodes) {
      expect(responseCodesText, `Response code ${code} should be documented`).toContain(code);
    }
  });
});
