// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = path.resolve(__dirname, '../index.html');

/**
 * Commands Reference Section Tests
 * Scenario: Verify the commands reference section lists all supported memcached commands and MirDB-specific commands
 */
test.describe('Commands Reference Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(`file://${indexPath}`);
  });

  /**
   * Test Case 1: SET command documented with format
   * Input: Query commands section for SET command
   * Expected: SET command documented with format 'set <key> <flags> <ttl> <bytes>'
   */
  test('TC1: SET command documented with correct format', async ({ page }) => {
    // Navigate to commands section
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Look for SET command in the table
    const setCommandRow = page.locator('#commands table tbody tr').filter({
      has: page.locator('code', { hasText: 'SET' })
    });
    await expect(setCommandRow).toBeVisible();

    // Verify format is documented
    const commandFormat = page.locator('[data-testid="set-format"]');
    await expect(commandFormat).toBeVisible();

    const formatText = await commandFormat.textContent();
    expect(formatText).toContain('set');
    expect(formatText).toContain('<key>');
    expect(formatText).toContain('<flags>');
    expect(formatText).toContain('<ttl>');
    expect(formatText).toContain('<bytes>');
  });

  /**
   * Test Case 2: GET command documented with format
   * Input: Query commands section for GET command
   * Expected: GET command documented with format 'get <key1> [<key2> ...]'
   */
  test('TC2: GET command documented with correct format', async ({ page }) => {
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Look for GET command
    const getCommandRow = page.locator('#commands table tbody tr').filter({
      has: page.locator('code', { hasText: /^GET$/ })
    });
    await expect(getCommandRow).toBeVisible();

    // Verify format is documented
    const commandFormat = page.locator('[data-testid="get-format"]');
    await expect(commandFormat).toBeVisible();

    const formatText = await commandFormat.textContent();
    expect(formatText).toContain('get');
    expect(formatText).toContain('<key');
  });

  /**
   * Test Case 3: DELETE command documented with format
   * Input: Query commands section for DELETE command
   * Expected: DELETE command documented with format 'delete <key> [noreply]'
   */
  test('TC3: DELETE command documented with correct format', async ({ page }) => {
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Look for DELETE command - use exact match to avoid matching DELETED
    const deleteCommandRow = page.locator('#commands table tbody tr').filter({
      has: page.locator('code', { hasText: /^DELETE$/ })
    });
    await expect(deleteCommandRow).toBeVisible();

    // Verify format is documented
    const commandFormat = page.locator('[data-testid="delete-format"]');
    await expect(commandFormat).toBeVisible();

    const formatText = await commandFormat.textContent();
    expect(formatText).toContain('delete');
    expect(formatText).toContain('<key>');
    expect(formatText).toContain('noreply');
  });

  /**
   * Test Case 4: ADD and REPLACE commands present
   * Input: Verify ADD and REPLACE commands present
   * Expected: Both ADD and REPLACE commands are listed in commands reference
   */
  test('TC4: ADD and REPLACE commands are listed', async ({ page }) => {
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Verify ADD command
    const addCommandRow = page.locator('#commands table tbody tr').filter({
      has: page.locator('code', { hasText: 'ADD' })
    });
    await expect(addCommandRow).toBeVisible();

    // Verify REPLACE command
    const replaceCommandRow = page.locator('#commands table tbody tr').filter({
      has: page.locator('code', { hasText: 'REPLACE' })
    });
    await expect(replaceCommandRow).toBeVisible();
  });

  /**
   * Test Case 5: APPEND and PREPEND commands present
   * Input: Verify APPEND and PREPEND commands present
   * Expected: Both APPEND and PREPEND commands are listed in commands reference
   */
  test('TC5: APPEND and PREPEND commands are listed', async ({ page }) => {
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Verify APPEND command
    const appendCommandRow = page.locator('#commands table tbody tr').filter({
      has: page.locator('code', { hasText: 'APPEND' })
    });
    await expect(appendCommandRow).toBeVisible();

    // Verify PREPEND command
    const prependCommandRow = page.locator('#commands table tbody tr').filter({
      has: page.locator('code', { hasText: 'PREPEND' })
    });
    await expect(prependCommandRow).toBeVisible();
  });

  /**
   * Test Case 6: MirDB-specific INFO command
   * Input: Verify MirDB-specific INFO command
   * Expected: INFO command documented as displaying database status
   */
  test('TC6: INFO command documented as MirDB-specific', async ({ page }) => {
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Verify INFO command
    const infoCommandRow = page.locator('#commands table tbody tr').filter({
      has: page.locator('code', { hasText: 'INFO' })
    });
    await expect(infoCommandRow).toBeVisible();

    // Verify it mentions database status or server information
    const rowText = await infoCommandRow.textContent();
    expect(rowText.toLowerCase()).toMatch(/server|status|database|information/);
  });

  /**
   * Test Case 7: MirDB-specific MAJOR_COMPACTION command
   * Input: Verify MirDB-specific MAJOR_COMPACTION command
   * Expected: MAJOR_COMPACTION command documented for triggering manual compaction
   */
  test('TC7: MAJOR_COMPACTION command documented for manual compaction', async ({ page }) => {
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Verify MAJOR_COMPACTION command
    const compactionCommandRow = page.locator('#commands table tbody tr').filter({
      has: page.locator('code', { hasText: 'MAJOR_COMPACTION' })
    });
    await expect(compactionCommandRow).toBeVisible();

    // Verify it mentions compaction
    const rowText = await compactionCommandRow.textContent();
    expect(rowText.toLowerCase()).toContain('compaction');
  });

  /**
   * Test Case 8: Response codes section exists
   * Input: Verify response codes section exists
   * Expected: Response codes STORED, NOT_STORED, EXISTS, NOT_FOUND, DELETED, ERROR documented
   */
  test('TC8: Response codes section documents all required codes', async ({ page }) => {
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Look for response codes section
    const responseCodesSection = page.locator('[data-testid="response-codes"]');
    await expect(responseCodesSection).toBeVisible();

    // Verify all required response codes are documented
    const sectionText = await responseCodesSection.textContent();

    expect(sectionText).toContain('STORED');
    expect(sectionText).toContain('NOT_STORED');
    expect(sectionText).toContain('EXISTS');
    expect(sectionText).toContain('NOT_FOUND');
    expect(sectionText).toContain('DELETED');
    expect(sectionText).toContain('ERROR');
  });

  /**
   * Additional test: GETS command is listed
   */
  test('GETS command is listed in retrieval commands', async ({ page }) => {
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Verify GETS command
    const getsCommandRow = page.locator('#commands table tbody tr').filter({
      has: page.locator('code', { hasText: 'GETS' })
    });
    await expect(getsCommandRow).toBeVisible();
  });

  /**
   * Additional test: Commands section has proper semantic structure
   */
  test('Commands section has proper semantic structure', async ({ page }) => {
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Verify section has role="region"
    await expect(commandsSection).toHaveAttribute('role', 'region');

    // Verify heading exists
    const heading = commandsSection.locator('h2');
    await expect(heading).toBeVisible();
  });
});
