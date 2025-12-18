// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = path.resolve(__dirname, '../index.html');

/**
 * Code Examples Accuracy Tests
 * Scenario: Verify all code examples are accurate and match MirDB's actual API
 */
test.describe('Code Examples Accuracy', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(`file://${indexPath}`);
  });

  /**
   * Test Case 1: Check server startup command syntax
   * Input: Check server startup command syntax
   * Expected: Command follows 'mirdb -c <config-file>' or documented format
   */
  test('TC1: server startup command follows documented format mirdb -c <config-file>', async ({ page }) => {
    // Navigate to quick-start section
    const quickStartSection = page.locator('[data-testid="quickstart-section"]');
    await expect(quickStartSection).toBeVisible();

    // Check the server startup code block
    const serverStartCode = page.locator('#server-start-code');
    await expect(serverStartCode).toBeVisible();

    const codeContent = await serverStartCode.textContent();

    // Verify command follows 'mirdb -c <config-file>' format
    // According to knowledge base: "mirdb -c /path/to/config.toml"
    expect(codeContent).toMatch(/mirdb\s+-c\s+\S+\.toml/);

    // Specifically verify it contains 'mirdb -c config.toml'
    expect(codeContent).toContain('mirdb -c config.toml');
  });

  /**
   * Test Case 2: Verify SET command example format
   * Input: Verify SET command example format
   * Expected: SET example follows 'set <key> <flags> <ttl> <bytes>' format
   */
  test('TC2: SET command example follows memcached protocol format', async ({ page }) => {
    const quickStartSection = page.locator('[data-testid="quickstart-section"]');
    await expect(quickStartSection).toBeVisible();

    // Check the SET command code block
    const setCode = page.locator('#set-code');
    await expect(setCode).toBeVisible();

    const codeContent = await setCode.textContent();

    // Verify SET format documentation: set <key> <flags> <ttl> <bytes>
    expect(codeContent).toMatch(/<key>.*<flags>.*<ttl>.*<bytes>/);

    // Verify actual example follows the format (set mykey 0 0 5)
    // Breaking down: set (command) mykey (key) 0 (flags) 0 (ttl) 5 (bytes)
    expect(codeContent).toMatch(/set\s+\w+\s+\d+\s+\d+\s+\d+/);

    // Verify the example values are correct
    expect(codeContent).toContain('set mykey 0 0 5');

    // Verify response is shown (STORED)
    expect(codeContent).toContain('STORED');
  });

  /**
   * Test Case 3: Verify GET command example format
   * Input: Verify GET command example format
   * Expected: GET example follows 'get <key>' format
   */
  test('TC3: GET command example follows memcached protocol format', async ({ page }) => {
    const quickStartSection = page.locator('[data-testid="quickstart-section"]');
    await expect(quickStartSection).toBeVisible();

    // Check the GET command code block
    const getCode = page.locator('#get-code');
    await expect(getCode).toBeVisible();

    const codeContent = await getCode.textContent();

    // Verify GET format: get <key>
    expect(codeContent).toMatch(/get\s+\w+/);
    expect(codeContent).toContain('get mykey');

    // Verify response format: VALUE <key> <flags> <bytes>\n<value>\nEND
    expect(codeContent).toContain('VALUE mykey 0 5');
    expect(codeContent).toContain('hello');
    expect(codeContent).toContain('END');
  });

  /**
   * Test Case 4: Verify connection port in examples
   * Input: Verify connection port in examples
   * Expected: Examples use port 12333 (default) or explain custom port usage
   */
  test('TC4: connection examples use default port 12333', async ({ page }) => {
    const quickStartSection = page.locator('[data-testid="quickstart-section"]');
    await expect(quickStartSection).toBeVisible();

    // Check the connection code block
    const connectCode = page.locator('#connect-code');
    await expect(connectCode).toBeVisible();

    const connectContent = await connectCode.textContent();

    // Verify port 12333 is used (matches default addr 0.0.0.0:12333)
    expect(connectContent).toContain('12333');
    expect(connectContent).toContain('localhost 12333');

    // Also verify the port is mentioned in the quick-start section text
    const sectionText = await quickStartSection.textContent();
    expect(sectionText).toContain('12333');

    // Verify configuration section shows the same default port
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    const configText = await configSection.textContent();
    expect(configText).toContain('0.0.0.0:12333');
  });

  /**
   * Test Case 5: Verify TOML configuration syntax
   * Input: Verify TOML configuration syntax
   * Expected: Any TOML config examples are syntactically valid
   */
  test('TC5: TOML configuration is mentioned and format is documented', async ({ page }) => {
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    const configText = await configSection.textContent();

    // Verify TOML is mentioned as the configuration format
    expect(configText.toLowerCase()).toContain('toml');

    // Verify the quick-start section references config.toml
    const quickStartSection = page.locator('[data-testid="quickstart-section"]');
    const quickStartText = await quickStartSection.textContent();
    expect(quickStartText).toContain('config.toml');

    // Verify configuration parameters match those in etc/mirdb.toml:
    // addr, work_dir, mem_table_max_size, sst_max_size, max_level
    const configTable = configSection.locator('table');
    await expect(configTable).toBeVisible();

    const tableText = await configTable.textContent();
    expect(tableText).toContain('addr');
    expect(tableText).toContain('work_dir');
    expect(tableText).toContain('mem_table_max_size');
    expect(tableText).toContain('sst_max_size');
    expect(tableText).toContain('max_level');

    // Verify default values match the actual TOML configuration
    expect(tableText).toContain('0.0.0.0:12333');
    expect(tableText).toContain('/tmp/mirdb');
    expect(tableText).toContain('4MB');
    expect(tableText).toContain('100MB');
    expect(tableText).toContain('7');
  });

  /**
   * Additional Test: Verify SET command format in commands reference section
   */
  test('SET command reference matches memcached protocol spec', async ({ page }) => {
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    const setFormat = page.locator('[data-testid="set-format"]');
    await expect(setFormat).toBeVisible();

    const formatText = await setFormat.textContent();

    // According to knowledge base: set <key> <flags> <ttl> <bytes> [noreply]\r\n
    expect(formatText).toContain('set');
    expect(formatText).toContain('<key>');
    expect(formatText).toContain('<flags>');
    expect(formatText).toContain('<ttl>');
    expect(formatText).toContain('<bytes>');
    expect(formatText).toContain('[noreply]');
  });

  /**
   * Additional Test: Verify GET command format in commands reference section
   */
  test('GET command reference matches memcached protocol spec', async ({ page }) => {
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    const getFormat = page.locator('[data-testid="get-format"]');
    await expect(getFormat).toBeVisible();

    const formatText = await getFormat.textContent();

    // According to knowledge base: get <key1> [<key2> ...]\r\n
    expect(formatText).toContain('get');
    expect(formatText).toContain('<key');
    // Should support multiple keys
    expect(formatText).toMatch(/\[.*\.\.\.\]|<key2>/);
  });

  /**
   * Additional Test: Complete example session matches documented API
   */
  test('Complete example session demonstrates correct API usage', async ({ page }) => {
    const completeCode = page.locator('#complete-code');
    await expect(completeCode).toBeVisible();

    const content = await completeCode.textContent();

    // Verify server startup command
    expect(content).toContain('mirdb -c config.toml');

    // Verify telnet connection on correct port
    expect(content).toContain('telnet localhost 12333');

    // Verify SET command with correct format
    expect(content).toContain('set mykey 0 0 5');
    expect(content).toContain('hello');
    expect(content).toContain('STORED');

    // Verify GET command with correct format and response
    expect(content).toContain('get mykey');
    expect(content).toContain('VALUE mykey 0 5');
    expect(content).toContain('END');

    // Verify DELETE command and response
    expect(content).toContain('delete mykey');
    expect(content).toContain('DELETED');
  });
});
