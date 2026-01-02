// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Content Accuracy Test Suite
 *
 * Verifies that all technical content on the MirDB landing page
 * accurately represents MirDB's actual capabilities.
 */
test.describe('Content Accuracy - Scenario 15', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Verify default port claim - port 12333', async ({ page }) => {
    // The landing page should accurately mention port 12333 as the default port
    // This matches the actual MirDB default configuration: addr = "0.0.0.0:12333"

    // Check in the quick-start section
    const quickStartSection = page.locator('#quickstart');
    await expect(quickStartSection).toBeVisible();

    // Find the configuration example
    const configStep = quickStartSection.locator('.quickstart-step').nth(1);
    const configCode = await configStep.locator('pre code').textContent();

    // Verify port 12333 is mentioned
    expect(configCode).toContain('12333');
    expect(configCode).toContain('0.0.0.0:12333');

    // Also check in the telnet example (Connect & Use step)
    const usageStep = quickStartSection.locator('.quickstart-step').nth(2);
    const usageCode = await usageStep.locator('pre code').textContent();

    // Verify telnet command uses port 12333
    expect(usageCode).toContain('telnet localhost 12333');

    // Also check in the configuration section
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    const configSectionCode = await configSection.locator('pre code').first().textContent();
    expect(configSectionCode).toContain('0.0.0.0:12333');

    // Check the options description
    const optionsList = configSection.locator('.option-list');
    const addrOption = await optionsList.locator('dt:has-text("addr")').first();
    await expect(addrOption).toBeVisible();

    // Verify default port is mentioned in the description
    const addrDescription = optionsList.locator('dd').first();
    const descText = await addrDescription.textContent();
    expect(descText).toContain('12333');
  });

  test('TC2: Verify SET command syntax - set <key> <flags> <ttl> <bytes>', async ({ page }) => {
    // The SET command should follow the correct memcached protocol format:
    // set <key> <flags> <ttl> <bytes> [noreply]\r\n
    // followed by <data>\r\n

    // Check in the quick-start section
    const quickStartSection = page.locator('#quickstart');
    await expect(quickStartSection).toBeVisible();

    // Find the usage step (third step)
    const usageStep = quickStartSection.locator('.quickstart-step').nth(2);
    const usageCode = await usageStep.locator('pre code').textContent();

    // Verify SET command example follows correct format: set <key> <flags> <ttl> <bytes>
    // Example: set mykey 0 0 5
    // - mykey is the key
    // - 0 is the flags
    // - 0 is the ttl (expiration time in seconds, 0 = no expiration)
    // - 5 is the number of bytes in the value
    expect(usageCode).toMatch(/set\s+\w+\s+\d+\s+\d+\s+\d+/);

    // Verify the specific example: set mykey 0 0 5
    expect(usageCode).toContain('set mykey 0 0 5');

    // Verify the response STORED is shown
    expect(usageCode).toContain('STORED');

    // Verify the data follows the set command
    expect(usageCode).toContain('hello');

    // Check the commands section for SET command documentation
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    const storageCategory = commandsSection.locator('.command-category[data-category="storage"]');
    await expect(storageCategory).toBeVisible();

    // Verify SET command is listed as a storage command
    const setCmd = storageCategory.locator('code:has-text("SET")');
    await expect(setCmd).toBeVisible();
  });

  test('TC3: Verify GET command syntax - get <key>', async ({ page }) => {
    // The GET command should follow the correct memcached protocol format:
    // get <key1> [<key2> ...]\r\n

    // Check in the quick-start section
    const quickStartSection = page.locator('#quickstart');
    await expect(quickStartSection).toBeVisible();

    // Find the usage step (third step)
    const usageStep = quickStartSection.locator('.quickstart-step').nth(2);
    const usageCode = await usageStep.locator('pre code').textContent();

    // Verify GET command example follows correct format: get <key>
    expect(usageCode).toMatch(/get\s+\w+/);

    // Verify the specific example: get mykey
    expect(usageCode).toContain('get mykey');

    // Verify the response format: VALUE <key> <flags> <bytes>\n<data>\nEND
    expect(usageCode).toContain('VALUE mykey');
    expect(usageCode).toContain('END');

    // Check the commands section for GET command documentation
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    const retrievalCategory = commandsSection.locator('.command-category[data-category="retrieval"]');
    await expect(retrievalCategory).toBeVisible();

    // Verify GET command is listed as a retrieval command
    const getCmd = retrievalCategory.getByRole('listitem').filter({ hasText: 'GET' }).first();
    await expect(getCmd).toBeVisible();
    await expect(getCmd.locator('code')).toHaveText('GET');
  });

  test('TC4: Verify TOML configuration validity', async ({ page }) => {
    // Configuration examples should be valid TOML syntax

    // Check the configuration section
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Get the configuration code example
    const codeBlock = configSection.locator('.config-example pre code');
    await expect(codeBlock).toBeVisible();

    const codeContent = await codeBlock.textContent();

    // Verify TOML syntax is valid
    // TOML requires key = value pairs with proper quoting

    // Check addr is a valid TOML string value
    expect(codeContent).toMatch(/addr\s*=\s*"[^"]+"/);

    // Check work_dir is a valid TOML string value
    expect(codeContent).toMatch(/work_dir\s*=\s*"[^"]+"/);

    // Check mem_table_max_size is a valid TOML string value with size suffix
    expect(codeContent).toMatch(/mem_table_max_size\s*=\s*"[^"]+"/);

    // Check sst_max_size is a valid TOML string value with size suffix
    expect(codeContent).toMatch(/sst_max_size\s*=\s*"[^"]+"/);

    // Verify specific values match MirDB defaults
    expect(codeContent).toContain('addr = "0.0.0.0:12333"');
    expect(codeContent).toContain('mem_table_max_size = "4M"');
    expect(codeContent).toContain('sst_max_size = "100M"');

    // Check the quick-start configuration example as well
    const quickStartSection = page.locator('#quickstart');
    const configStep = quickStartSection.locator('.quickstart-step').nth(1);
    const quickStartConfig = await configStep.locator('pre code').textContent();

    // Verify quick-start TOML is also valid
    expect(quickStartConfig).toMatch(/addr\s*=\s*"[^"]+"/);
    expect(quickStartConfig).toMatch(/work_dir\s*=\s*"[^"]+"/);
    expect(quickStartConfig).toContain('addr = "0.0.0.0:12333"');
  });

  test('Additional: Verify supported commands are accurately listed', async ({ page }) => {
    // Verify that the commands listed match MirDB's actual capabilities
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Storage commands - these should match MirDB's actual implementation
    const storageCategory = commandsSection.locator('.command-category[data-category="storage"]');
    const storageCommands = ['SET', 'ADD', 'REPLACE', 'APPEND', 'PREPEND'];
    for (const cmd of storageCommands) {
      const cmdElement = storageCategory.locator(`code:has-text("${cmd}")`);
      await expect(cmdElement).toBeVisible();
    }

    // Retrieval commands
    const retrievalCategory = commandsSection.locator('.command-category[data-category="retrieval"]');
    const retrievalCommands = ['GET', 'GETS'];
    for (const cmd of retrievalCommands) {
      // Use more specific locator for GET to avoid matching GETS
      if (cmd === 'GET') {
        const getCmd = retrievalCategory.getByRole('listitem').filter({ hasText: 'GET' }).first();
        await expect(getCmd).toBeVisible();
      } else {
        const cmdElement = retrievalCategory.locator(`code:has-text("${cmd}")`);
        await expect(cmdElement).toBeVisible();
      }
    }

    // Deletion commands
    const deletionCategory = commandsSection.locator('.command-category[data-category="deletion"]');
    const deleteCmd = deletionCategory.locator('code:has-text("DELETE")');
    await expect(deleteCmd).toBeVisible();

    // MirDB extensions - unique to MirDB, not standard memcached
    const extensionsCategory = commandsSection.locator('.command-category[data-category="mirdb-extensions"]');
    const mirdbCommands = ['INFO', 'MAJOR_COMPACTION'];
    for (const cmd of mirdbCommands) {
      const cmdElement = extensionsCategory.locator(`code:has-text("${cmd}")`);
      await expect(cmdElement).toBeVisible();
    }
  });

  test('Additional: Verify default configuration values accuracy', async ({ page }) => {
    // Verify that default values mentioned match MirDB's actual defaults
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Check the options list for accuracy
    const optionsList = configSection.locator('.option-list');

    // addr default should be 0.0.0.0:12333
    const addrDesc = await optionsList.locator('dd').first().textContent();
    expect(addrDesc).toContain('0.0.0.0:12333');

    // mem_table_max_size default should be 4M
    const memTableDesc = await optionsList.locator('dd').nth(2).textContent();
    expect(memTableDesc).toContain('4M') || expect(memTableDesc).toContain('4 megabytes');

    // sst_max_size default should be 100M
    const sstDesc = await optionsList.locator('dd').nth(3).textContent();
    expect(sstDesc).toContain('100M') || expect(sstDesc).toContain('100 megabytes');
  });
});
