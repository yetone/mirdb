// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

// Base URL for the static HTML file
const BASE_URL = 'file://' + path.resolve(__dirname, '../dist/index.html');

test.describe('Protocol Reference Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(BASE_URL);
  });

  test('Test Case 1: Supported commands table is visible with all required commands', async ({ page }) => {
    // Navigate to protocol section
    const protocolSection = page.locator('[data-testid="protocol-section"]');
    await expect(protocolSection).toBeVisible();

    // Check that the commands table is visible
    const commandsTable = page.locator('[data-testid="commands-table"]');
    await expect(commandsTable).toBeVisible();

    // Verify all required commands are listed: SET, GET, ADD, REPLACE, DELETE, APPEND, PREPEND, GETS
    const requiredCommands = ['SET', 'GET', 'ADD', 'REPLACE', 'DELETE', 'APPEND', 'PREPEND', 'GETS'];

    for (const command of requiredCommands) {
      const commandRow = page.locator(`[data-testid="command-${command.toLowerCase()}"]`);
      await expect(commandRow).toBeVisible();

      // Verify the command text is present
      const commandText = await commandRow.textContent();
      expect(commandText).toContain(command);
    }
  });

  test('Test Case 2: MirDB-specific commands INFO and MAJOR_COMPACTION are listed separately', async ({ page }) => {
    // Check that the MirDB-specific commands section is visible
    const mirdbCommands = page.locator('[data-testid="mirdb-commands"]');
    await expect(mirdbCommands).toBeVisible();

    // Verify INFO command is listed with description
    const infoCommand = page.locator('[data-testid="command-info"]');
    await expect(infoCommand).toBeVisible();
    const infoText = await infoCommand.textContent();
    expect(infoText).toContain('INFO');
    expect(infoText.length).toBeGreaterThan(10); // Has a description

    // Verify MAJOR_COMPACTION command is listed with description
    const majorCompactionCommand = page.locator('[data-testid="command-major-compaction"]');
    await expect(majorCompactionCommand).toBeVisible();
    const majorCompactionText = await majorCompactionCommand.textContent();
    expect(majorCompactionText).toContain('MAJOR_COMPACTION');
    expect(majorCompactionText.length).toBeGreaterThan(10); // Has a description

    // Verify these are highlighted as MirDB-specific
    const mirdbSection = await mirdbCommands.textContent();
    expect(mirdbSection.toLowerCase()).toContain('mirdb');
  });

  test('Test Case 3: Response codes are documented', async ({ page }) => {
    // Check that the response codes section is visible
    const responseCodes = page.locator('[data-testid="response-codes"]');
    await expect(responseCodes).toBeVisible();

    // Verify all required response codes are listed: STORED, NOT_STORED, EXISTS, NOT_FOUND, DELETED, ERROR
    const requiredCodes = ['STORED', 'NOT_STORED', 'EXISTS', 'NOT_FOUND', 'DELETED', 'ERROR'];

    for (const code of requiredCodes) {
      const codeRow = page.locator(`[data-testid="response-${code.toLowerCase().replace('_', '-')}"]`);
      await expect(codeRow).toBeVisible();

      // Verify the code text is present
      const codeText = await codeRow.textContent();
      expect(codeText).toContain(code);
    }
  });

  test('Protocol section has proper heading', async ({ page }) => {
    const protocolSection = page.locator('[data-testid="protocol-section"]');
    await expect(protocolSection).toBeVisible();

    // Verify the section has a proper heading
    const heading = protocolSection.locator('h2');
    await expect(heading).toBeVisible();
    const headingText = await heading.textContent();
    expect(headingText.toLowerCase()).toContain('protocol');
  });

  test('Commands table has proper structure with headers', async ({ page }) => {
    const commandsTable = page.locator('[data-testid="commands-table"]');
    await expect(commandsTable).toBeVisible();

    // Verify table has headers
    const tableHeaders = commandsTable.locator('thead th');
    const headerCount = await tableHeaders.count();
    expect(headerCount).toBeGreaterThanOrEqual(2);

    // Verify Command and Description columns exist
    const headerText = await commandsTable.locator('thead').textContent();
    expect(headerText).toContain('Command');
    expect(headerText).toContain('Description');
  });

  test('Response codes table has proper structure with headers', async ({ page }) => {
    const responseCodes = page.locator('[data-testid="response-codes"]');
    await expect(responseCodes).toBeVisible();

    // Verify table has headers
    const tableHeaders = responseCodes.locator('thead th');
    const headerCount = await tableHeaders.count();
    expect(headerCount).toBeGreaterThanOrEqual(2);

    // Verify Code and Meaning columns exist
    const headerText = await responseCodes.locator('thead').textContent();
    expect(headerText).toContain('Code');
    expect(headerText).toContain('Meaning');
  });
});
