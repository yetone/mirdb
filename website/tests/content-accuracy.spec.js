/**
 * Content Accuracy Tests
 *
 * These tests verify that all technical content on the MirDB homepage matches
 * the actual MirDB capabilities and documentation from the knowledge base.
 */
const { test, expect } = require('@playwright/test');
const path = require('path');

const indexHtmlPath = 'file://' + path.resolve(__dirname, '../dist/index.html');

test.describe('Content Accuracy - Command Syntax', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(indexHtmlPath);
  });

  test('SET command syntax matches format: set <key> <flags> <ttl> <bytes>', async ({ page }) => {
    // Check for SET command documentation
    const setCommandElement = await page.locator('[data-testid="example-set"]');
    await expect(setCommandElement).toBeVisible();

    // Get the code content
    const codeContent = await setCommandElement.locator('code').textContent();

    // Verify SET command format includes: set <key> <flags> <ttl> <bytes>
    expect(codeContent).toContain('set');
    expect(codeContent).toMatch(/set\s+\S+\s+\d+\s+\d+\s+\d+/); // set key flags ttl bytes format

    // Also check the comment showing the format
    expect(codeContent).toMatch(/<key>.*<flags>.*<ttl>.*<bytes>/);
  });

  test('GET command syntax matches format: get <key1> [<key2> ...]', async ({ page }) => {
    // Check for GET command documentation
    const getCommandElement = await page.locator('[data-testid="example-get"]');
    await expect(getCommandElement).toBeVisible();

    // Get the code content
    const codeContent = await getCommandElement.locator('code').textContent();

    // Verify GET command format
    expect(codeContent).toContain('get');
    expect(codeContent).toMatch(/get\s+\S+/); // get followed by at least one key

    // Check the comment showing the format - should indicate get <key>
    expect(codeContent).toMatch(/<key>/);
  });

  test('DELETE command syntax is correct', async ({ page }) => {
    // Check for DELETE command documentation
    const deleteCommandElement = await page.locator('[data-testid="example-delete"]');
    await expect(deleteCommandElement).toBeVisible();

    // Get the code content
    const codeContent = await deleteCommandElement.locator('code').textContent();

    // Verify DELETE command format
    expect(codeContent).toContain('delete');
    expect(codeContent).toMatch(/delete\s+\S+/); // delete followed by key
  });

  test('Command descriptions in table are accurate', async ({ page }) => {
    // Verify SET command description
    const setRow = await page.locator('[data-testid="command-set"]');
    await expect(setRow).toContainText('Store a key-value pair');

    // Verify GET command description
    const getRow = await page.locator('[data-testid="command-get"]');
    await expect(getRow).toContainText('Retrieve one or more keys');

    // Verify DELETE command description
    const deleteRow = await page.locator('[data-testid="command-delete"]');
    await expect(deleteRow).toContainText('Remove a key');

    // Verify ADD command description
    const addRow = await page.locator('[data-testid="command-add"]');
    await expect(addRow).toContainText('Store only if key doesn\'t exist');

    // Verify REPLACE command description
    const replaceRow = await page.locator('[data-testid="command-replace"]');
    await expect(replaceRow).toContainText('Store only if key exists');

    // Verify APPEND command description
    const appendRow = await page.locator('[data-testid="command-append"]');
    await expect(appendRow).toContainText('Append data to existing value');

    // Verify PREPEND command description
    const prependRow = await page.locator('[data-testid="command-prepend"]');
    await expect(prependRow).toContainText('Prepend data to existing value');

    // Verify GETS command description
    const getsRow = await page.locator('[data-testid="command-gets"]');
    await expect(getsRow).toContainText('Retrieve with CAS token');
  });
});

test.describe('Content Accuracy - Default Port', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(indexHtmlPath);
  });

  test('Default port 12333 is correctly stated', async ({ page }) => {
    // Check configuration example shows correct port
    const configExample = await page.locator('[data-testid="config-example"]');
    await expect(configExample).toBeVisible();
    const configContent = await configExample.locator('code').textContent();
    expect(configContent).toContain('0.0.0.0:12333');

    // Check network configuration table shows correct default port
    const addrRow = await page.locator('[data-testid="config-param-addr"]');
    await expect(addrRow).toContainText('0.0.0.0:12333');

    // Check client connection example uses correct port
    const clientExample = await page.locator('[data-testid="client-connection-example"]');
    await expect(clientExample).toBeVisible();
    const clientContent = await clientExample.locator('code').textContent();
    expect(clientContent).toContain('localhost:12333');
  });
});

test.describe('Content Accuracy - INFO Command', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(indexHtmlPath);
  });

  test('INFO command is accurately described as displaying database status', async ({ page }) => {
    // Check INFO command documentation
    const infoCommand = await page.locator('[data-testid="command-info"]');
    await expect(infoCommand).toBeVisible();

    // Get the description text
    const infoDescription = await infoCommand.locator('p').textContent();

    // Verify INFO command description mentions database status and level information
    expect(infoDescription.toLowerCase()).toContain('database status');
    expect(infoDescription.toLowerCase()).toContain('level information');

    // Additional verification: should mention memtable, SSTable, or compaction
    expect(infoDescription.toLowerCase()).toMatch(/(memtable|sstable|compaction)/);
  });

  test('MAJOR_COMPACTION command is documented', async ({ page }) => {
    // Check MAJOR_COMPACTION command documentation
    const majorCompactionCommand = await page.locator('[data-testid="command-major-compaction"]');
    await expect(majorCompactionCommand).toBeVisible();

    // Get the description text
    const description = await majorCompactionCommand.locator('p').textContent();

    // Verify MAJOR_COMPACTION command description mentions compaction
    expect(description.toLowerCase()).toContain('compaction');
  });

  test('MirDB-specific commands section exists', async ({ page }) => {
    const mirdbCommands = await page.locator('[data-testid="mirdb-commands"]');
    await expect(mirdbCommands).toBeVisible();

    // Should contain both INFO and MAJOR_COMPACTION
    await expect(mirdbCommands).toContainText('INFO');
    await expect(mirdbCommands).toContainText('MAJOR_COMPACTION');
  });
});

test.describe('Content Accuracy - TOML Configuration', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(indexHtmlPath);
  });

  test('Configuration example uses correct TOML syntax and valid parameters', async ({ page }) => {
    // Check configuration example exists
    const configExample = await page.locator('[data-testid="config-example"]');
    await expect(configExample).toBeVisible();

    // Get the configuration content
    const configContent = await configExample.locator('code').textContent();

    // Verify TOML syntax - key = value format with quotes for strings
    expect(configContent).toMatch(/addr\s*=\s*"[^"]+"/); // addr = "..."
    expect(configContent).toMatch(/work_dir\s*=\s*"[^"]+"/); // work_dir = "..."

    // Verify valid parameter names from knowledge base
    expect(configContent).toContain('addr');
    expect(configContent).toContain('work_dir');
    expect(configContent).toContain('max_level');
    expect(configContent).toMatch(/mem_table_max_size/);
    expect(configContent).toMatch(/sst_max_size/);

    // Verify size format with units (e.g., "4M", "100M")
    expect(configContent).toMatch(/"\d+[KMG]"/);
  });

  test('Configuration parameter defaults match knowledge base', async ({ page }) => {
    // Verify addr default
    const addrRow = await page.locator('[data-testid="config-param-addr"]');
    await expect(addrRow).toContainText('0.0.0.0:12333');

    // Verify work_dir default
    const workDirRow = await page.locator('[data-testid="config-param-work-dir"]');
    await expect(workDirRow).toContainText('/tmp/mirdb');

    // Verify max_level default
    const maxLevelRow = await page.locator('[data-testid="config-param-max-level"]');
    await expect(maxLevelRow).toContainText('7');

    // Verify sst_max_size default
    const sstMaxSizeRow = await page.locator('[data-testid="config-param-sst-max-size"]');
    await expect(sstMaxSizeRow).toContainText('100M');

    // Verify mem_table_max_size default
    const memTableMaxSizeRow = await page.locator('[data-testid="config-param-mem-table-max-size"]');
    await expect(memTableMaxSizeRow).toContainText('4M');

    // Verify mem_table_max_height default
    const memTableMaxHeightRow = await page.locator('[data-testid="config-param-mem-table-max-height"]');
    await expect(memTableMaxHeightRow).toContainText('32');

    // Verify imm_mem_table_max_count default
    const immMemTableMaxCountRow = await page.locator('[data-testid="config-param-imm-mem-table-max-count"]');
    await expect(immMemTableMaxCountRow).toContainText('16');

    // Verify block_size default
    const blockSizeRow = await page.locator('[data-testid="config-param-block-size"]');
    await expect(blockSizeRow).toContainText('4K');

    // Verify l0_compaction_trigger default
    const l0CompactionTriggerRow = await page.locator('[data-testid="config-param-l0-compaction-trigger"]');
    await expect(l0CompactionTriggerRow).toContainText('4');

    // Verify thread_sleep_ms default
    const threadSleepMsRow = await page.locator('[data-testid="config-param-thread-sleep-ms"]');
    await expect(threadSleepMsRow).toContainText('500');
  });

  test('Configuration section describes TOML format', async ({ page }) => {
    // Check configuration section exists
    const configSection = await page.locator('[data-testid="configuration-section"]');
    await expect(configSection).toBeVisible();

    // Should mention TOML configuration files
    await expect(configSection).toContainText('TOML');
  });
});

test.describe('Content Accuracy - Response Codes', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(indexHtmlPath);
  });

  test('Response codes match memcached protocol documentation', async ({ page }) => {
    // Verify STORED response
    const storedRow = await page.locator('[data-testid="response-stored"]');
    await expect(storedRow).toContainText('STORED');
    await expect(storedRow).toContainText('stored successfully');

    // Verify NOT_STORED response
    const notStoredRow = await page.locator('[data-testid="response-not-stored"]');
    await expect(notStoredRow).toContainText('NOT_STORED');
    await expect(notStoredRow).toContainText('not stored');

    // Verify EXISTS response
    const existsRow = await page.locator('[data-testid="response-exists"]');
    await expect(existsRow).toContainText('EXISTS');
    await expect(existsRow).toContainText('CAS');

    // Verify NOT_FOUND response
    const notFoundRow = await page.locator('[data-testid="response-not-found"]');
    await expect(notFoundRow).toContainText('NOT_FOUND');
    await expect(notFoundRow).toContainText('not found');

    // Verify DELETED response
    const deletedRow = await page.locator('[data-testid="response-deleted"]');
    await expect(deletedRow).toContainText('DELETED');
    await expect(deletedRow).toContainText('deleted');

    // Verify ERROR response
    const errorRow = await page.locator('[data-testid="response-error"]');
    await expect(errorRow).toContainText('ERROR');
  });
});
