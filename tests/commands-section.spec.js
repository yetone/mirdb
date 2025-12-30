// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = 'file://' + path.resolve(__dirname, '..', 'index.html');

test.describe('Supported Commands Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(indexPath);
  });

  /**
   * Test Case 1: Locate commands section on homepage
   * Expected: Commands section exists with organized display of supported commands
   */
  test('TC1: commands section exists with organized display', async ({ page }) => {
    // Navigate to commands section
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Verify section has a title
    const sectionTitle = commandsSection.locator('.section-title');
    await expect(sectionTitle).toBeVisible();

    // Verify commands are displayed in organized format (table or grid)
    const commandsDisplay = commandsSection.locator('.commands-grid, .commands-table, table');
    await expect(commandsDisplay).toBeVisible();

    // Verify there are command items displayed
    const commandItems = commandsSection.locator('.command-item, .command-row, tr, .command-card');
    const count = await commandItems.count();
    expect(count).toBeGreaterThan(0);
  });

  /**
   * Test Case 2: Check for GET command in list
   * Expected: GET command is listed in supported commands
   */
  test('TC2: GET command is listed in supported commands', async ({ page }) => {
    // Navigate to commands section
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Find GET command in the section
    const sectionText = await commandsSection.textContent();
    expect(sectionText).toContain('GET');

    // Verify GET is displayed as a command (not just random text)
    const getCommand = commandsSection.locator(':text("GET")');
    await expect(getCommand.first()).toBeVisible();
  });

  /**
   * Test Case 3: Check for SET command in list
   * Expected: SET command is listed in supported commands
   */
  test('TC3: SET command is listed in supported commands', async ({ page }) => {
    // Navigate to commands section
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Find SET command in the section
    const sectionText = await commandsSection.textContent();
    expect(sectionText).toContain('SET');

    // Verify SET is displayed as a command
    const setCommand = commandsSection.locator(':text("SET")');
    await expect(setCommand.first()).toBeVisible();
  });

  /**
   * Test Case 4: Check for DELETE command in list
   * Expected: DELETE command is listed in supported commands
   */
  test('TC4: DELETE command is listed in supported commands', async ({ page }) => {
    // Navigate to commands section
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Find DELETE command in the section
    const sectionText = await commandsSection.textContent();
    expect(sectionText).toContain('DELETE');

    // Verify DELETE is displayed as a command
    const deleteCommand = commandsSection.locator(':text("DELETE")');
    await expect(deleteCommand.first()).toBeVisible();
  });

  /**
   * Test Case 5: Check for additional storage commands
   * Expected: Additional commands like ADD, REPLACE, APPEND, or PREPEND are listed
   */
  test('TC5: additional storage commands are listed', async ({ page }) => {
    // Navigate to commands section
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Get section text content
    const sectionText = await commandsSection.textContent();

    // Check for at least one of the additional storage commands
    const hasAdd = sectionText.includes('ADD');
    const hasReplace = sectionText.includes('REPLACE');
    const hasAppend = sectionText.includes('APPEND');
    const hasPrepend = sectionText.includes('PREPEND');

    // At least one additional command should be present
    const hasAdditionalCommand = hasAdd || hasReplace || hasAppend || hasPrepend;
    expect(hasAdditionalCommand).toBe(true);

    // Verify commands are visible (check at least one is visible)
    if (hasAdd) {
      const addCommand = commandsSection.locator(':text("ADD")');
      await expect(addCommand.first()).toBeVisible();
    } else if (hasReplace) {
      const replaceCommand = commandsSection.locator(':text("REPLACE")');
      await expect(replaceCommand.first()).toBeVisible();
    } else if (hasAppend) {
      const appendCommand = commandsSection.locator(':text("APPEND")');
      await expect(appendCommand.first()).toBeVisible();
    } else if (hasPrepend) {
      const prependCommand = commandsSection.locator(':text("PREPEND")');
      await expect(prependCommand.first()).toBeVisible();
    }
  });
});
