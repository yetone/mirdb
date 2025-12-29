// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Commands Reference Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Commands section exists with list or table of supported operations', async ({ page }) => {
    // Verify commands section exists
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Verify section has the "Supported Commands" heading
    const sectionTitle = commandsSection.locator('.section-title');
    await expect(sectionTitle).toHaveText('Supported Commands');

    // Verify commands are displayed (either as cards or table rows)
    const commandCards = commandsSection.locator('.command-card');
    const commandCount = await commandCards.count();
    expect(commandCount).toBeGreaterThanOrEqual(7); // SET, GET, DELETE, ADD, REPLACE, APPEND, PREPEND
  });

  test('TC2: SET command is documented with description', async ({ page }) => {
    const commandsSection = page.locator('#commands');
    const setCommand = commandsSection.locator('[data-command="set"]');

    // Verify SET command card is visible
    await expect(setCommand).toBeVisible();

    // Verify command name
    const commandName = setCommand.locator('.command-name');
    await expect(commandName).toHaveText('SET');

    // Verify description exists and mentions storing/setting data
    const description = setCommand.locator('.command-description');
    await expect(description).toBeVisible();
    const descText = await description.textContent();
    expect(descText.toLowerCase()).toMatch(/store|set|save/);
  });

  test('TC3: GET command is documented with description', async ({ page }) => {
    const commandsSection = page.locator('#commands');
    const getCommand = commandsSection.locator('[data-command="get"]');

    // Verify GET command card is visible
    await expect(getCommand).toBeVisible();

    // Verify command name
    const commandName = getCommand.locator('.command-name');
    await expect(commandName).toHaveText('GET');

    // Verify description exists and mentions retrieving/getting data
    const description = getCommand.locator('.command-description');
    await expect(description).toBeVisible();
    const descText = await description.textContent();
    expect(descText.toLowerCase()).toMatch(/retrieve|get|fetch|read/);
  });

  test('TC4: DELETE command is documented with description', async ({ page }) => {
    const commandsSection = page.locator('#commands');
    const deleteCommand = commandsSection.locator('[data-command="delete"]');

    // Verify DELETE command card is visible
    await expect(deleteCommand).toBeVisible();

    // Verify command name
    const commandName = deleteCommand.locator('.command-name');
    await expect(commandName).toHaveText('DELETE');

    // Verify description exists and mentions removing/deleting data
    const description = deleteCommand.locator('.command-description');
    await expect(description).toBeVisible();
    const descText = await description.textContent();
    expect(descText.toLowerCase()).toMatch(/remove|delete|erase/);
  });

  test('TC5: Additional commands (ADD, REPLACE, APPEND, PREPEND) are documented', async ({ page }) => {
    const commandsSection = page.locator('#commands');

    // Verify ADD command
    const addCommand = commandsSection.locator('[data-command="add"]');
    await expect(addCommand).toBeVisible();
    const addName = addCommand.locator('.command-name');
    await expect(addName).toHaveText('ADD');
    const addDesc = addCommand.locator('.command-description');
    await expect(addDesc).toBeVisible();

    // Verify REPLACE command
    const replaceCommand = commandsSection.locator('[data-command="replace"]');
    await expect(replaceCommand).toBeVisible();
    const replaceName = replaceCommand.locator('.command-name');
    await expect(replaceName).toHaveText('REPLACE');
    const replaceDesc = replaceCommand.locator('.command-description');
    await expect(replaceDesc).toBeVisible();

    // Verify APPEND command
    const appendCommand = commandsSection.locator('[data-command="append"]');
    await expect(appendCommand).toBeVisible();
    const appendName = appendCommand.locator('.command-name');
    await expect(appendName).toHaveText('APPEND');
    const appendDesc = appendCommand.locator('.command-description');
    await expect(appendDesc).toBeVisible();

    // Verify PREPEND command
    const prependCommand = commandsSection.locator('[data-command="prepend"]');
    await expect(prependCommand).toBeVisible();
    const prependName = prependCommand.locator('.command-name');
    await expect(prependName).toHaveText('PREPEND');
    const prependDesc = prependCommand.locator('.command-description');
    await expect(prependDesc).toBeVisible();
  });
});
