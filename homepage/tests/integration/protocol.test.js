/**
 * Protocol Section Integration Tests
 * Owner: Scenario 6 - Protocol Reference Section
 *
 * Test cases:
 * - Commands table is rendered
 * - Standard memcached commands listed
 * - MirDB-specific commands highlighted
 * - Each command has description
 */

import { test, expect } from '@playwright/test';

test.describe('Protocol Reference Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Commands table or list is rendered', async ({ page }) => {
    // Check protocol section exists
    const protocolSection = page.locator('#protocol');
    await expect(protocolSection).toBeVisible();

    // Check that the section title is correct
    const sectionTitle = page.locator('#protocol-title');
    await expect(sectionTitle).toBeVisible();
    await expect(sectionTitle).toHaveText('Protocol Reference');

    // Check that the commands table is rendered
    const protocolTable = page.locator('.protocol-table');
    await expect(protocolTable).toBeVisible();

    // Check table has proper structure
    const tableHead = protocolTable.locator('thead');
    await expect(tableHead).toBeVisible();

    const tableBody = protocolTable.locator('tbody');
    await expect(tableBody).toBeVisible();

    // Check table headers
    const headers = protocolTable.locator('th');
    await expect(headers).toHaveCount(3);
    await expect(headers.nth(0)).toContainText('Command');
    await expect(headers.nth(1)).toContainText('Description');
    await expect(headers.nth(2)).toContainText('Type');
  });

  test('TC2: Standard memcached commands (set, get, delete, add, replace, append, prepend, gets) are listed', async ({ page }) => {
    const tableBody = page.locator('.protocol-table tbody');

    // Check all standard memcached commands are present
    const standardCommands = ['set', 'get', 'delete', 'add', 'replace', 'append', 'prepend', 'gets'];

    for (const command of standardCommands) {
      const row = tableBody.locator(`tr[data-command="${command}"]`);
      await expect(row).toBeVisible();

      // Check command name is displayed
      const commandName = row.locator('.command-name');
      await expect(commandName).toContainText(command);

      // Check command is marked as standard
      const commandType = row.locator('.command-type-standard');
      await expect(commandType).toBeVisible();
      await expect(commandType).toContainText('Standard');
    }
  });

  test('TC3: MirDB-specific commands (info, major_compaction) are present and highlighted', async ({ page }) => {
    const tableBody = page.locator('.protocol-table tbody');

    // Check MirDB-specific commands
    const mirdbCommands = ['info', 'major_compaction'];

    for (const command of mirdbCommands) {
      const row = tableBody.locator(`tr[data-command="${command}"]`);
      await expect(row).toBeVisible();

      // Check the row has mirdb-specific class for highlighting
      await expect(row).toHaveClass(/mirdb-specific/);

      // Check command name has special styling
      const commandName = row.locator('.command-name');
      await expect(commandName).toContainText(command);
      await expect(commandName).toHaveClass(/mirdb-command/);

      // Check command is marked as MirDB type
      const commandType = row.locator('.command-type-mirdb');
      await expect(commandType).toBeVisible();
      await expect(commandType).toContainText('MirDB');
    }
  });

  test('TC4: Each command has a description of its purpose', async ({ page }) => {
    const tableBody = page.locator('.protocol-table tbody');
    const rows = tableBody.locator('tr');

    // Get total number of commands (should be 10: 8 standard + 2 MirDB)
    await expect(rows).toHaveCount(10);

    // Check each command has a non-empty description
    const rowCount = await rows.count();
    for (let i = 0; i < rowCount; i++) {
      const row = rows.nth(i);

      // Description is in the second column
      const descriptionCell = row.locator('td').nth(1);
      await expect(descriptionCell).toBeVisible();

      // Check description is not empty
      const description = await descriptionCell.textContent();
      expect(description.trim().length).toBeGreaterThan(10);
    }
  });

  test('Protocol section has proper semantic structure', async ({ page }) => {
    // Check section has aria-labelledby
    const protocolSection = page.locator('#protocol');
    await expect(protocolSection).toHaveAttribute('aria-labelledby', 'protocol-title');

    // Check table has proper accessibility attributes
    const protocolTable = page.locator('.protocol-table');
    await expect(protocolTable).toHaveAttribute('aria-label', 'Supported memcached commands');

    // Check table headers use scope attribute
    const headers = protocolTable.locator('th');
    const headerCount = await headers.count();
    for (let i = 0; i < headerCount; i++) {
      await expect(headers.nth(i)).toHaveAttribute('scope', 'col');
    }
  });

  test('Protocol section displays note about MirDB-specific commands', async ({ page }) => {
    // Check that there's a note explaining MirDB-specific commands
    const protocolNote = page.locator('.protocol-note');
    await expect(protocolNote).toBeVisible();

    // Note should mention MirDB-specific commands
    await expect(protocolNote).toContainText('MirDB');
    await expect(protocolNote).toContainText(/not part of.*standard|extension/i);
  });

  test('Commands have specific descriptions', async ({ page }) => {
    const tableBody = page.locator('.protocol-table tbody');

    // Check specific command descriptions
    const commandDescriptions = {
      'set': /store|key-value|overwrite/i,
      'get': /retrieve|keys/i,
      'delete': /remove/i,
      'add': /store.*not.*exist/i,
      'replace': /store.*exist/i,
      'append': /append/i,
      'prepend': /prepend/i,
      'gets': /CAS|token/i,
      'info': /status|statistics/i,
      'major_compaction': /compaction|SSTable/i,
    };

    for (const [command, descPattern] of Object.entries(commandDescriptions)) {
      const row = tableBody.locator(`tr[data-command="${command}"]`);
      const descriptionCell = row.locator('td').nth(1);
      await expect(descriptionCell).toContainText(descPattern);
    }
  });

  test('Table is responsive with horizontal scroll wrapper', async ({ page }) => {
    // Check that table wrapper exists for responsive scrolling
    const tableWrapper = page.locator('.protocol-table-wrapper');
    await expect(tableWrapper).toBeVisible();

    // Check wrapper has overflow-x handling via CSS
    const overflowX = await tableWrapper.evaluate((el) => {
      return window.getComputedStyle(el).overflowX;
    });
    expect(overflowX).toBe('auto');
  });

  test('Section subtitle describes protocol compatibility', async ({ page }) => {
    const subtitle = page.locator('#protocol .section-subtitle');
    await expect(subtitle).toBeVisible();
    await expect(subtitle).toContainText(/memcached.*protocol|protocol.*compatibility|MirDB-specific/i);
  });
});
