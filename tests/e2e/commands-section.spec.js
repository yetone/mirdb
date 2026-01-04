// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Supported Commands Display (REQ-5)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('should display storage commands (SET, ADD, REPLACE, APPEND, PREPEND)', async ({ page }) => {
    // Navigate to commands section
    const commandsSection = page.locator('.commands');
    await commandsSection.scrollIntoViewIfNeeded();

    // Check that the section is visible
    await expect(commandsSection).toBeVisible();

    // Find the Storage command group
    const storageGroup = commandsSection.locator('.command-group').filter({ hasText: 'Storage' });
    await expect(storageGroup).toBeVisible();

    // Verify storage commands are listed
    const storageCommands = storageGroup.locator('code');
    await expect(storageCommands).toContainText('SET');
    await expect(storageCommands).toContainText('ADD');
    await expect(storageCommands).toContainText('REPLACE');
    await expect(storageCommands).toContainText('APPEND');
    await expect(storageCommands).toContainText('PREPEND');
  });

  test('should display retrieval commands (GET, GETS)', async ({ page }) => {
    // Navigate to commands section
    const commandsSection = page.locator('.commands');
    await commandsSection.scrollIntoViewIfNeeded();

    // Check that the section is visible
    await expect(commandsSection).toBeVisible();

    // Find the Retrieval command group
    const retrievalGroup = commandsSection.locator('.command-group').filter({ hasText: 'Retrieval' });
    await expect(retrievalGroup).toBeVisible();

    // Verify retrieval commands are listed
    const retrievalCommands = retrievalGroup.locator('code');
    await expect(retrievalCommands).toContainText('GET');
    await expect(retrievalCommands).toContainText('GETS');
  });

  test('should display deletion command (DELETE)', async ({ page }) => {
    // Navigate to commands section
    const commandsSection = page.locator('.commands');
    await commandsSection.scrollIntoViewIfNeeded();

    // Check that the section is visible
    await expect(commandsSection).toBeVisible();

    // Find the Deletion command group
    const deletionGroup = commandsSection.locator('.command-group').filter({ hasText: 'Deletion' });
    await expect(deletionGroup).toBeVisible();

    // Verify deletion command is listed
    const deletionCommands = deletionGroup.locator('code');
    await expect(deletionCommands).toContainText('DELETE');
  });

  test('should display MirDB-specific commands (INFO, MAJOR_COMPACTION)', async ({ page }) => {
    // Navigate to commands section
    const commandsSection = page.locator('.commands');
    await commandsSection.scrollIntoViewIfNeeded();

    // Check that the section is visible
    await expect(commandsSection).toBeVisible();

    // Find the MirDB-Specific command group
    const mirdbGroup = commandsSection.locator('.command-group').filter({ hasText: 'MirDB-Specific' });
    await expect(mirdbGroup).toBeVisible();

    // Verify MirDB-specific commands are listed
    const mirdbCommands = mirdbGroup.locator('code');
    await expect(mirdbCommands).toContainText('INFO');
    await expect(mirdbCommands).toContainText('MAJOR_COMPACTION');
  });

  test('should have a section header for Supported Commands', async ({ page }) => {
    // Navigate to commands section
    const commandsSection = page.locator('.commands');
    await commandsSection.scrollIntoViewIfNeeded();

    // Check for section header
    const header = commandsSection.locator('h2');
    await expect(header).toBeVisible();
    await expect(header).toHaveText('Supported Commands');
  });

  test('should display all four command groups', async ({ page }) => {
    // Navigate to commands section
    const commandsSection = page.locator('.commands');
    await commandsSection.scrollIntoViewIfNeeded();

    // Verify all command groups are present
    const commandGroups = commandsSection.locator('.command-group');
    await expect(commandGroups).toHaveCount(4);
  });
});
