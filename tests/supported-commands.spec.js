const { test, expect } = require('@playwright/test');

test.describe('Supported Commands Display (REQ-5)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC6: Commands section exists on the page', async ({ page }) => {
    // Navigate to commands section
    const commandsSection = page.locator('#commands');
    await commandsSection.scrollIntoViewIfNeeded();

    // Verify the commands section is visible
    await expect(commandsSection).toBeVisible();

    // Verify section has a title
    const sectionTitle = commandsSection.locator('.section-title');
    await expect(sectionTitle).toBeVisible();
    await expect(sectionTitle).toContainText('Supported Commands');

    // Verify section has a subtitle
    const sectionSubtitle = commandsSection.locator('.section-subtitle');
    await expect(sectionSubtitle).toBeVisible();
    await expect(sectionSubtitle).toContainText('memcached');

    // Verify the commands grid exists
    const commandsGrid = commandsSection.locator('.commands-grid');
    await expect(commandsGrid).toBeVisible();
  });

  test('TC1: SET command is listed in supported commands', async ({ page }) => {
    // Navigate to commands section
    const commandsSection = page.locator('#commands');
    await commandsSection.scrollIntoViewIfNeeded();

    // Find the SET command item
    const setCommand = commandsSection.locator('.command-item', { hasText: 'SET' });
    await expect(setCommand).toBeVisible();

    // Verify it has the command name
    const commandName = setCommand.locator('.command-name');
    await expect(commandName).toHaveText('SET');

    // Verify it has a description
    const commandDesc = setCommand.locator('.command-desc');
    await expect(commandDesc).toBeVisible();
    await expect(commandDesc).toContainText(/store/i);
  });

  test('TC2: GET command is listed in supported commands', async ({ page }) => {
    // Navigate to commands section
    const commandsSection = page.locator('#commands');
    await commandsSection.scrollIntoViewIfNeeded();

    // Find the GET command item
    const getCommand = commandsSection.locator('.command-item', { hasText: 'GET' }).first();
    await expect(getCommand).toBeVisible();

    // Verify it has the command name
    const commandName = getCommand.locator('.command-name');
    await expect(commandName).toHaveText('GET');

    // Verify it has a description
    const commandDesc = getCommand.locator('.command-desc');
    await expect(commandDesc).toBeVisible();
    await expect(commandDesc).toContainText(/retrieve/i);
  });

  test('TC3: DELETE command is listed in supported commands', async ({ page }) => {
    // Navigate to commands section
    const commandsSection = page.locator('#commands');
    await commandsSection.scrollIntoViewIfNeeded();

    // Find the DELETE command item
    const deleteCommand = commandsSection.locator('.command-item', { hasText: 'DELETE' });
    await expect(deleteCommand).toBeVisible();

    // Verify it has the command name
    const commandName = deleteCommand.locator('.command-name');
    await expect(commandName).toHaveText('DELETE');

    // Verify it has a description
    const commandDesc = deleteCommand.locator('.command-desc');
    await expect(commandDesc).toBeVisible();
    await expect(commandDesc).toContainText(/remove/i);
  });

  test('TC4: ADD command is listed in supported commands', async ({ page }) => {
    // Navigate to commands section
    const commandsSection = page.locator('#commands');
    await commandsSection.scrollIntoViewIfNeeded();

    // Find the ADD command item
    const addCommand = commandsSection.locator('.command-item', { hasText: 'ADD' });
    await expect(addCommand).toBeVisible();

    // Verify it has the command name
    const commandName = addCommand.locator('.command-name');
    await expect(commandName).toHaveText('ADD');

    // Verify it has a description
    const commandDesc = addCommand.locator('.command-desc');
    await expect(commandDesc).toBeVisible();
    await expect(commandDesc).toContainText(/not exists/i);
  });

  test('TC5: REPLACE command is listed in supported commands', async ({ page }) => {
    // Navigate to commands section
    const commandsSection = page.locator('#commands');
    await commandsSection.scrollIntoViewIfNeeded();

    // Find the REPLACE command item
    const replaceCommand = commandsSection.locator('.command-item', { hasText: 'REPLACE' });
    await expect(replaceCommand).toBeVisible();

    // Verify it has the command name
    const commandName = replaceCommand.locator('.command-name');
    await expect(commandName).toHaveText('REPLACE');

    // Verify it has a description
    const commandDesc = replaceCommand.locator('.command-desc');
    await expect(commandDesc).toBeVisible();
    await expect(commandDesc).toContainText(/existing/i);
  });

  test('All storage commands (SET, ADD, REPLACE, APPEND, PREPEND) are listed', async ({ page }) => {
    // Navigate to commands section
    const commandsSection = page.locator('#commands');
    await commandsSection.scrollIntoViewIfNeeded();

    // Check all storage commands
    const storageCommands = ['SET', 'ADD', 'REPLACE', 'APPEND', 'PREPEND'];

    for (const cmd of storageCommands) {
      const commandItem = commandsSection.locator('.command-name', { hasText: new RegExp(`^${cmd}$`) });
      await expect(commandItem, `${cmd} command should be visible`).toBeVisible();
    }
  });

  test('All retrieval commands (GET, GETS) are listed', async ({ page }) => {
    // Navigate to commands section
    const commandsSection = page.locator('#commands');
    await commandsSection.scrollIntoViewIfNeeded();

    // Check all retrieval commands
    const retrievalCommands = ['GET', 'GETS'];

    for (const cmd of retrievalCommands) {
      const commandItem = commandsSection.locator('.command-name', { hasText: new RegExp(`^${cmd}$`) });
      await expect(commandItem, `${cmd} command should be visible`).toBeVisible();
    }
  });

  test('DELETE command is listed as admin command', async ({ page }) => {
    // Navigate to commands section
    const commandsSection = page.locator('#commands');
    await commandsSection.scrollIntoViewIfNeeded();

    // Verify DELETE command exists
    const deleteCommand = commandsSection.locator('.command-name', { hasText: /^DELETE$/ });
    await expect(deleteCommand).toBeVisible();
  });

  test('Commands section is navigable from header', async ({ page }) => {
    // Find the commands link in navigation
    const commandsLink = page.locator('nav a[href="#commands"]');
    await expect(commandsLink).toBeVisible();

    // Click the commands link
    await commandsLink.click();

    // Verify commands section is in view
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeInViewport();
  });

  test('Commands grid displays at least 8 commands', async ({ page }) => {
    // Navigate to commands section
    const commandsSection = page.locator('#commands');
    await commandsSection.scrollIntoViewIfNeeded();

    // Count the command items
    const commandItems = commandsSection.locator('.command-item');
    const count = await commandItems.count();

    // Should have at least 8 commands: SET, GET, GETS, DELETE, ADD, REPLACE, APPEND, PREPEND
    expect(count).toBeGreaterThanOrEqual(8);
  });
});
