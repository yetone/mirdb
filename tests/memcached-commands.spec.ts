import { test, expect } from '@playwright/test';

/**
 * E2E Tests for Supported Memcached Commands Display
 *
 * This test suite verifies that the homepage displays all seven
 * required memcached commands: SET, GET, DELETE, ADD, REPLACE, APPEND, PREPEND
 *
 * Requirement: REQ-5 - Show supported memcached commands
 */

test.describe('Supported Memcached Commands Display', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to the homepage
    await page.goto('/');
    // Wait for the commands section to be visible
    await page.waitForSelector('#commands');
  });

  test('should display the commands section with proper heading', async ({ page }) => {
    // Verify the commands section exists
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Verify the section heading
    const heading = commandsSection.locator('h2');
    await expect(heading).toHaveText('Supported Memcached Commands');
  });

  test('Test Case 1: SET command is listed in supported commands', async ({ page }) => {
    // Locate the command card with SET command
    const setCommand = page.locator('.command-card[data-command="SET"]');
    await expect(setCommand).toBeVisible();

    // Verify the command name is displayed
    const commandName = setCommand.locator('.command-name');
    await expect(commandName).toHaveText('SET');

    // Verify description exists
    const description = setCommand.locator('.command-description');
    await expect(description).toBeVisible();
    await expect(description).toContainText('Store a key-value pair');
  });

  test('Test Case 2: GET command is listed in supported commands', async ({ page }) => {
    // Locate the command card with GET command
    const getCommand = page.locator('.command-card[data-command="GET"]');
    await expect(getCommand).toBeVisible();

    // Verify the command name is displayed
    const commandName = getCommand.locator('.command-name');
    await expect(commandName).toHaveText('GET');

    // Verify description exists
    const description = getCommand.locator('.command-description');
    await expect(description).toBeVisible();
    await expect(description).toContainText('Retrieve the value');
  });

  test('Test Case 3: DELETE command is listed in supported commands', async ({ page }) => {
    // Locate the command card with DELETE command
    const deleteCommand = page.locator('.command-card[data-command="DELETE"]');
    await expect(deleteCommand).toBeVisible();

    // Verify the command name is displayed
    const commandName = deleteCommand.locator('.command-name');
    await expect(commandName).toHaveText('DELETE');

    // Verify description exists
    const description = deleteCommand.locator('.command-description');
    await expect(description).toBeVisible();
    await expect(description).toContainText('Remove a key');
  });

  test('Test Case 4: ADD command is listed in supported commands', async ({ page }) => {
    // Locate the command card with ADD command
    const addCommand = page.locator('.command-card[data-command="ADD"]');
    await expect(addCommand).toBeVisible();

    // Verify the command name is displayed
    const commandName = addCommand.locator('.command-name');
    await expect(commandName).toHaveText('ADD');

    // Verify description exists
    const description = addCommand.locator('.command-description');
    await expect(description).toBeVisible();
    await expect(description).toContainText('Store only if the key does not exist');
  });

  test('Test Case 5: REPLACE command is listed in supported commands', async ({ page }) => {
    // Locate the command card with REPLACE command
    const replaceCommand = page.locator('.command-card[data-command="REPLACE"]');
    await expect(replaceCommand).toBeVisible();

    // Verify the command name is displayed
    const commandName = replaceCommand.locator('.command-name');
    await expect(commandName).toHaveText('REPLACE');

    // Verify description exists
    const description = replaceCommand.locator('.command-description');
    await expect(description).toBeVisible();
    await expect(description).toContainText('Store only if the key already exists');
  });

  test('Test Case 6: APPEND command is listed in supported commands', async ({ page }) => {
    // Locate the command card with APPEND command
    const appendCommand = page.locator('.command-card[data-command="APPEND"]');
    await expect(appendCommand).toBeVisible();

    // Verify the command name is displayed
    const commandName = appendCommand.locator('.command-name');
    await expect(commandName).toHaveText('APPEND');

    // Verify description exists
    const description = appendCommand.locator('.command-description');
    await expect(description).toBeVisible();
    await expect(description).toContainText('Append data to an existing value');
  });

  test('Test Case 7: PREPEND command is listed in supported commands', async ({ page }) => {
    // Locate the command card with PREPEND command
    const prependCommand = page.locator('.command-card[data-command="PREPEND"]');
    await expect(prependCommand).toBeVisible();

    // Verify the command name is displayed
    const commandName = prependCommand.locator('.command-name');
    await expect(commandName).toHaveText('PREPEND');

    // Verify description exists
    const description = prependCommand.locator('.command-description');
    await expect(description).toBeVisible();
    await expect(description).toContainText('Prepend data to an existing value');
  });

  test('should display all seven commands in the grid', async ({ page }) => {
    // Verify all 7 command cards are present
    const commandCards = page.locator('.command-card');
    await expect(commandCards).toHaveCount(7);

    // Verify each required command is present
    const requiredCommands = ['SET', 'GET', 'DELETE', 'ADD', 'REPLACE', 'APPEND', 'PREPEND'];

    for (const command of requiredCommands) {
      const commandCard = page.locator(`.command-card[data-command="${command}"]`);
      await expect(commandCard).toBeVisible();
    }
  });

  test('commands should be clearly formatted and readable', async ({ page }) => {
    // Check that command names use code formatting
    const commandNames = page.locator('.command-name');
    const count = await commandNames.count();

    expect(count).toBe(7);

    // Verify each command name is properly styled as code element
    for (let i = 0; i < count; i++) {
      const commandName = commandNames.nth(i);
      await expect(commandName).toBeVisible();
      // Check it's a code element
      const tagName = await commandName.evaluate(el => el.tagName.toLowerCase());
      expect(tagName).toBe('code');
    }
  });
});
