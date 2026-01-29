/**
 * E2E tests for SupportedCommands responsive behavior.
 * Owner: Scenario 7 - Supported Commands Table
 *
 * Test Case 6: Verify commands table is scrollable or reformats for small viewports
 */

import { test, expect } from '@playwright/test';

test.describe('Commands Table Responsive Layout', () => {
  test('Test Case 6: Table is scrollable on mobile viewport (375px)', async ({ page }) => {
    // Set viewport to 375px width (iPhone SE / small mobile)
    await page.setViewportSize({ width: 375, height: 667 });

    // Navigate to the homepage
    await page.goto('/');

    // Wait for the commands section to be visible
    const commandsSection = page.locator('[data-testid="commands-section"]');
    await expect(commandsSection).toBeVisible();

    // Verify the table container has overflow-x-auto for horizontal scrolling
    const tableContainer = page.locator('[data-testid="commands-table-container"]');
    await expect(tableContainer).toBeVisible();

    // Check that the container allows horizontal scrolling
    const containerClasses = await tableContainer.getAttribute('class');
    expect(containerClasses).toContain('overflow-x-auto');

    // Verify the table exists and has content
    const table = page.locator('[data-testid="commands-table"]');
    await expect(table).toBeVisible();

    // Verify all 8 commands are present
    const expectedCommands = ['get', 'gets', 'set', 'add', 'replace', 'append', 'prepend', 'delete'];
    for (const cmdName of expectedCommands) {
      await expect(page.locator(`[data-testid="command-row-${cmdName}"]`)).toBeVisible();
    }

    // Verify the table is scrollable by checking its width vs container width
    const tableBox = await table.boundingBox();
    const containerBox = await tableContainer.boundingBox();

    expect(tableBox).not.toBeNull();
    expect(containerBox).not.toBeNull();

    if (tableBox && containerBox) {
      // Table should have minimum width (500px) which is wider than container on mobile
      expect(tableBox.width).toBeGreaterThanOrEqual(500);
    }
  });

  test('Table displays normally on desktop viewport', async ({ page }) => {
    // Set viewport to desktop size
    await page.setViewportSize({ width: 1280, height: 800 });

    // Navigate to the homepage
    await page.goto('/');

    // Wait for the commands section to be visible
    const commandsSection = page.locator('[data-testid="commands-section"]');
    await expect(commandsSection).toBeVisible();

    // Verify the table is visible
    const table = page.locator('[data-testid="commands-table"]');
    await expect(table).toBeVisible();

    // Verify all 8 commands are present
    const commandRows = page.locator('[data-testid^="command-row-"]');
    await expect(commandRows).toHaveCount(8);

    // Verify table headers (scoped to commands table to avoid conflicts with other tables on the page)
    const commandsTable = page.locator('[data-testid="commands-table"]');
    await expect(commandsTable.locator('th', { hasText: 'Command' })).toBeVisible();
    await expect(commandsTable.locator('th', { hasText: 'Description' })).toBeVisible();
    await expect(commandsTable.locator('th', { hasText: 'Syntax' })).toBeVisible();
  });

  test('All 8 command entries have correct content', async ({ page }) => {
    await page.goto('/');

    // Wait for the commands section to be visible
    const commandsSection = page.locator('[data-testid="commands-section"]');
    await expect(commandsSection).toBeVisible();

    // Check section heading
    await expect(page.locator('h2', { hasText: 'Supported Commands' })).toBeVisible();

    // Verify each command has name, description, and syntax
    const expectedCommands = [
      { name: 'get', descContains: 'retrieve', syntaxContains: 'get <key>' },
      { name: 'gets', descContains: 'cas', syntaxContains: 'gets <key>' },
      { name: 'set', descContains: 'store', syntaxContains: 'set <key>' },
      { name: 'add', descContains: 'does not', syntaxContains: 'add <key>' },
      { name: 'replace', descContains: 'exists', syntaxContains: 'replace <key>' },
      { name: 'append', descContains: 'end', syntaxContains: 'append <key>' },
      { name: 'prepend', descContains: 'beginning', syntaxContains: 'prepend <key>' },
      { name: 'delete', descContains: 'remove', syntaxContains: 'delete <key>' },
    ];

    for (const cmd of expectedCommands) {
      // Check command name
      const nameCell = page.locator(`[data-testid="command-name-${cmd.name}"]`);
      await expect(nameCell).toHaveText(cmd.name);

      // Check description contains expected text (case-insensitive via regex)
      const descCell = page.locator(`[data-testid="command-description-${cmd.name}"]`);
      const descText = await descCell.textContent();
      expect(descText?.toLowerCase()).toContain(cmd.descContains.toLowerCase());

      // Check syntax contains expected text
      const syntaxCell = page.locator(`[data-testid="command-syntax-${cmd.name}"]`);
      await expect(syntaxCell).toContainText(cmd.syntaxContains);
    }
  });

  test('Table maintains readable layout at tablet viewport', async ({ page }) => {
    // Set viewport to tablet size
    await page.setViewportSize({ width: 768, height: 1024 });

    // Navigate to the homepage
    await page.goto('/');

    // Wait for the commands section to be visible
    const commandsSection = page.locator('[data-testid="commands-section"]');
    await expect(commandsSection).toBeVisible();

    // Verify the table is visible and properly sized
    const table = page.locator('[data-testid="commands-table"]');
    await expect(table).toBeVisible();

    // Check that all columns are visible without needing to scroll
    const tableBox = await table.boundingBox();
    const containerBox = await page.locator('[data-testid="commands-table-container"]').boundingBox();

    expect(tableBox).not.toBeNull();
    expect(containerBox).not.toBeNull();

    // At tablet width, table should fit within container or be slightly scrollable
    if (tableBox && containerBox) {
      // Table should be at least 500px wide (our minimum)
      expect(tableBox.width).toBeGreaterThanOrEqual(500);
    }

    // Verify all 8 commands are present
    const commandRows = page.locator('[data-testid^="command-row-"]');
    await expect(commandRows).toHaveCount(8);
  });
});
