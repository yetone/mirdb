// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Supported Commands Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Section displays all 9 commands: SET, GET, DELETE, ADD, REPLACE, APPEND, PREPEND, INFO, MAJOR_COMPACTION', async ({ page }) => {
    // Navigate to Commands section
    const commandsSection = page.locator('#commands, [data-testid="commands-section"]');
    await expect(commandsSection).toBeVisible();

    // Verify all 9 commands are present
    const expectedCommands = [
      'SET',
      'GET',
      'DELETE',
      'ADD',
      'REPLACE',
      'APPEND',
      'PREPEND',
      'INFO',
      'MAJOR_COMPACTION'
    ];

    for (const command of expectedCommands) {
      const commandElement = commandsSection.locator(`[data-testid="command-${command}"], .command-item:has(code:text("${command}"))`);
      await expect(commandElement).toBeVisible();
    }

    // Verify there are exactly 9 command items
    const commandItems = commandsSection.locator('.command-item, [data-testid^="command-"]');
    await expect(commandItems).toHaveCount(9);
  });

  test('TC2: SET command shows description: Store a key-value pair', async ({ page }) => {
    // Navigate to Commands section
    const commandsSection = page.locator('#commands, [data-testid="commands-section"]');
    await expect(commandsSection).toBeVisible();

    // Check SET command description
    const setCommand = commandsSection.locator('[data-testid="command-SET"], .command-item:has(code:text("SET"))');
    await expect(setCommand).toBeVisible();

    // Verify description text
    const description = setCommand.locator('span, .command-description');
    await expect(description).toContainText('Store a key-value pair');
  });

  test('TC3: GET command shows description: Retrieve value(s) by key', async ({ page }) => {
    // Navigate to Commands section
    const commandsSection = page.locator('#commands, [data-testid="commands-section"]');
    await expect(commandsSection).toBeVisible();

    // Check GET command description
    const getCommand = commandsSection.locator('[data-testid="command-GET"], .command-item:has(code:text("GET"))');
    await expect(getCommand).toBeVisible();

    // Verify description text
    const description = getCommand.locator('span, .command-description');
    await expect(description).toContainText('Retrieve value(s) by key');
  });

  test('TC4: DELETE command shows description: Remove a key', async ({ page }) => {
    // Navigate to Commands section
    const commandsSection = page.locator('#commands, [data-testid="commands-section"]');
    await expect(commandsSection).toBeVisible();

    // Check DELETE command description
    const deleteCommand = commandsSection.locator('[data-testid="command-DELETE"], .command-item:has(code:text("DELETE"))');
    await expect(deleteCommand).toBeVisible();

    // Verify description text
    const description = deleteCommand.locator('span, .command-description');
    await expect(description).toContainText('Remove a key');
  });

  test('TC5: INFO command shows description: Display database status', async ({ page }) => {
    // Navigate to Commands section
    const commandsSection = page.locator('#commands, [data-testid="commands-section"]');
    await expect(commandsSection).toBeVisible();

    // Check INFO command description
    const infoCommand = commandsSection.locator('[data-testid="command-INFO"], .command-item:has(code:text("INFO"))');
    await expect(infoCommand).toBeVisible();

    // Verify description text
    const description = infoCommand.locator('span, .command-description');
    await expect(description).toContainText('Display database status');
  });
});
