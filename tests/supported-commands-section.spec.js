// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Test Suite: Supported Commands Section
 * Scenario: Verify the supported Memcached commands are displayed with explanations
 * REQ-5: Display supported Memcached commands (SET, GET, DELETE, etc.) with brief explanations
 */

test.describe('Supported Commands Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Check for SET command in commands list
   * Input: Check for SET command in commands list
   * Expected: SET command is listed with description 'Store a key-value pair' or similar
   */
  test('TC1: SET command is listed with description', async ({ page }) => {
    // Navigate to the commands table
    const commandsTable = page.locator('.commands-table');
    await expect(commandsTable).toBeVisible();

    // Find the SET command row
    const setRow = page.locator('.commands-table tbody tr', {
      has: page.locator('td', { hasText: /^SET$/i })
    });
    await expect(setRow).toBeVisible();

    // Verify SET has a description about storing key-value pairs
    const setDescription = setRow.locator('td').nth(1);
    const descriptionText = await setDescription.textContent();
    expect(descriptionText).toBeTruthy();

    // Check for keywords related to storing
    const lowerText = descriptionText.toLowerCase();
    const hasStoreKeyword = lowerText.includes('store') || lowerText.includes('set') || lowerText.includes('save');
    const hasKeyValueKeyword = lowerText.includes('key') || lowerText.includes('value') || lowerText.includes('pair');
    expect(hasStoreKeyword || hasKeyValueKeyword).toBeTruthy();
  });

  /**
   * Test Case 2: Check for GET command in commands list
   * Input: Check for GET command in commands list
   * Expected: GET command is listed with description about retrieving keys
   */
  test('TC2: GET command is listed with description about retrieving keys', async ({ page }) => {
    // Navigate to the commands table
    const commandsTable = page.locator('.commands-table');
    await expect(commandsTable).toBeVisible();

    // Find the GET command row
    const getRow = page.locator('.commands-table tbody tr', {
      has: page.locator('td', { hasText: /^GET$/i })
    });
    await expect(getRow).toBeVisible();

    // Verify GET has a description about retrieving keys
    const getDescription = getRow.locator('td').nth(1);
    const descriptionText = await getDescription.textContent();
    expect(descriptionText).toBeTruthy();

    // Check for keywords related to retrieving
    const lowerText = descriptionText.toLowerCase();
    const hasRetrieveKeyword = lowerText.includes('retrieve') || lowerText.includes('get') || lowerText.includes('fetch') || lowerText.includes('read');
    const hasKeyKeyword = lowerText.includes('key');
    expect(hasRetrieveKeyword || hasKeyKeyword).toBeTruthy();
  });

  /**
   * Test Case 3: Check for DELETE command in commands list
   * Input: Check for DELETE command in commands list
   * Expected: DELETE command is listed with description about removing keys
   */
  test('TC3: DELETE command is listed with description about removing keys', async ({ page }) => {
    // Navigate to the commands table
    const commandsTable = page.locator('.commands-table');
    await expect(commandsTable).toBeVisible();

    // Find the DELETE command row
    const deleteRow = page.locator('.commands-table tbody tr', {
      has: page.locator('td', { hasText: /^DELETE$/i })
    });
    await expect(deleteRow).toBeVisible();

    // Verify DELETE has a description about removing keys
    const deleteDescription = deleteRow.locator('td').nth(1);
    const descriptionText = await deleteDescription.textContent();
    expect(descriptionText).toBeTruthy();

    // Check for keywords related to removing/deleting
    const lowerText = descriptionText.toLowerCase();
    const hasRemoveKeyword = lowerText.includes('remove') || lowerText.includes('delete') || lowerText.includes('drop');
    const hasKeyKeyword = lowerText.includes('key');
    expect(hasRemoveKeyword || hasKeyKeyword).toBeTruthy();
  });

  /**
   * Test Case 4: Check for all 8 supported commands
   * Input: Check for all 8 supported commands
   * Expected: Commands section lists SET, GET, DELETE, ADD, REPLACE, APPEND, PREPEND, INFO
   */
  test('TC4: All 8 supported commands are listed', async ({ page }) => {
    // Navigate to the commands table
    const commandsTable = page.locator('.commands-table');
    await expect(commandsTable).toBeVisible();

    // List of all required commands
    const requiredCommands = ['SET', 'GET', 'DELETE', 'ADD', 'REPLACE', 'APPEND', 'PREPEND', 'INFO'];

    // Check each command exists in the table
    for (const command of requiredCommands) {
      const commandRow = page.locator('.commands-table tbody tr', {
        has: page.locator('td', { hasText: new RegExp(`^${command}$`, 'i') })
      });
      await expect(commandRow, `Command ${command} should be visible`).toBeVisible();
    }

    // Verify we have at least 8 rows in the table body
    const tableRows = page.locator('.commands-table tbody tr');
    const rowCount = await tableRows.count();
    expect(rowCount).toBeGreaterThanOrEqual(8);
  });

  /**
   * Test Case 5: Verify commands are in table or structured list format
   * Input: Verify commands are in table or structured list format
   * Expected: Commands are displayed in a table with Command and Description columns, or similarly structured format
   */
  test('TC5: Commands are displayed in a table with Command and Description columns', async ({ page }) => {
    // Verify the commands table exists and is visible
    const commandsTable = page.locator('.commands-table');
    await expect(commandsTable).toBeVisible();

    // Verify table has a header
    const tableHeader = page.locator('.commands-table thead');
    await expect(tableHeader).toBeVisible();

    // Verify header row exists with appropriate columns
    const headerRow = page.locator('.commands-table thead tr');
    await expect(headerRow).toBeVisible();

    // Check for Command column header
    const commandHeader = page.locator('.commands-table thead th', { hasText: /command/i });
    await expect(commandHeader).toBeVisible();

    // Check for Description column header
    const descriptionHeader = page.locator('.commands-table thead th', { hasText: /description/i });
    await expect(descriptionHeader).toBeVisible();

    // Verify table has body with rows
    const tableBody = page.locator('.commands-table tbody');
    await expect(tableBody).toBeVisible();

    // Verify each row has exactly 2 cells (Command and Description)
    const firstRow = page.locator('.commands-table tbody tr').first();
    const cellsInFirstRow = firstRow.locator('td');
    const cellCount = await cellsInFirstRow.count();
    expect(cellCount).toBe(2);
  });

  /**
   * Additional test: Verify ADD command description
   */
  test('ADD command has correct description', async ({ page }) => {
    const commandsTable = page.locator('.commands-table');
    await expect(commandsTable).toBeVisible();

    const addRow = page.locator('.commands-table tbody tr', {
      has: page.locator('td', { hasText: /^ADD$/i })
    });
    await expect(addRow).toBeVisible();

    const addDescription = addRow.locator('td').nth(1);
    const descriptionText = await addDescription.textContent();

    // ADD should mention storing only if key doesn't exist
    const lowerText = descriptionText.toLowerCase();
    expect(lowerText.includes("doesn't exist") || lowerText.includes('not exist') || lowerText.includes('add')).toBeTruthy();
  });

  /**
   * Additional test: Verify REPLACE command description
   */
  test('REPLACE command has correct description', async ({ page }) => {
    const commandsTable = page.locator('.commands-table');
    await expect(commandsTable).toBeVisible();

    const replaceRow = page.locator('.commands-table tbody tr', {
      has: page.locator('td', { hasText: /^REPLACE$/i })
    });
    await expect(replaceRow).toBeVisible();

    const replaceDescription = replaceRow.locator('td').nth(1);
    const descriptionText = await replaceDescription.textContent();

    // REPLACE should mention storing only if key exists
    const lowerText = descriptionText.toLowerCase();
    expect(lowerText.includes('exists') || lowerText.includes('replace')).toBeTruthy();
  });

  /**
   * Additional test: Verify APPEND command description
   */
  test('APPEND command has correct description', async ({ page }) => {
    const commandsTable = page.locator('.commands-table');
    await expect(commandsTable).toBeVisible();

    const appendRow = page.locator('.commands-table tbody tr', {
      has: page.locator('td', { hasText: /^APPEND$/i })
    });
    await expect(appendRow).toBeVisible();

    const appendDescription = appendRow.locator('td').nth(1);
    const descriptionText = await appendDescription.textContent();

    // APPEND should mention appending data
    const lowerText = descriptionText.toLowerCase();
    expect(lowerText.includes('append')).toBeTruthy();
  });

  /**
   * Additional test: Verify PREPEND command description
   */
  test('PREPEND command has correct description', async ({ page }) => {
    const commandsTable = page.locator('.commands-table');
    await expect(commandsTable).toBeVisible();

    const prependRow = page.locator('.commands-table tbody tr', {
      has: page.locator('td', { hasText: /^PREPEND$/i })
    });
    await expect(prependRow).toBeVisible();

    const prependDescription = prependRow.locator('td').nth(1);
    const descriptionText = await prependDescription.textContent();

    // PREPEND should mention prepending data
    const lowerText = descriptionText.toLowerCase();
    expect(lowerText.includes('prepend')).toBeTruthy();
  });

  /**
   * Additional test: Verify INFO command description (MirDB-specific)
   */
  test('INFO command has correct description and is marked as MirDB-specific', async ({ page }) => {
    const commandsTable = page.locator('.commands-table');
    await expect(commandsTable).toBeVisible();

    const infoRow = page.locator('.commands-table tbody tr', {
      has: page.locator('td', { hasText: /^INFO$/i })
    });
    await expect(infoRow).toBeVisible();

    const infoDescription = infoRow.locator('td').nth(1);
    const descriptionText = await infoDescription.textContent();

    // INFO should mention database status and MirDB-specific
    const lowerText = descriptionText.toLowerCase();
    const hasDatabaseKeyword = lowerText.includes('database') || lowerText.includes('status') || lowerText.includes('info');
    expect(hasDatabaseKeyword).toBeTruthy();
  });

  /**
   * Additional test: Commands section has proper heading
   */
  test('Commands section has proper heading', async ({ page }) => {
    // Find the Supported Commands heading
    const commandsHeading = page.locator('h3', { hasText: /supported commands/i });
    await expect(commandsHeading).toBeVisible();
  });

  /**
   * Additional test: Commands table is within documentation section
   */
  test('Commands table is within documentation section', async ({ page }) => {
    // Verify commands table is within the documentation section
    const docsSection = page.locator('#documentation');
    await expect(docsSection).toBeVisible();

    const commandsTableInDocs = docsSection.locator('.commands-table');
    await expect(commandsTableInDocs).toBeVisible();
  });
});
