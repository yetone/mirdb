// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const pageUrl = 'file://' + path.join(__dirname, '..', 'index.html');

test.describe('Supported Commands Section (REQ-5)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(pageUrl);
  });

  // Test Case 1: Check for supported commands section
  test('TC1: Supported commands section is displayed', async ({ page }) => {
    // Navigate to commands section
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Verify section heading
    const heading = commandsSection.locator('h2');
    await expect(heading).toHaveText('Supported Commands');

    // Verify the commands grid is present
    const commandsGrid = commandsSection.locator('.commands-grid');
    await expect(commandsGrid).toBeVisible();

    // Verify there are command categories
    const commandCategories = commandsSection.locator('.command-category');
    const categoryCount = await commandCategories.count();
    expect(categoryCount).toBeGreaterThanOrEqual(3);
  });

  // Test Case 2: Verify storage commands listed
  test('TC2: Storage commands (SET, ADD, REPLACE, APPEND, PREPEND) are listed', async ({ page }) => {
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Find the Storage category
    const storageCategory = commandsSection.locator('.command-category').filter({
      has: page.locator('h3', { hasText: 'Storage' })
    });
    await expect(storageCategory).toBeVisible();

    // Get the list of storage commands
    const storageList = storageCategory.locator('ul');
    await expect(storageList).toBeVisible();

    // Verify each storage command is listed
    const storageCommands = ['SET', 'ADD', 'REPLACE', 'APPEND', 'PREPEND'];
    for (const command of storageCommands) {
      const commandItem = storageList.locator('li', { hasText: command });
      await expect(commandItem).toBeVisible();
    }
  });

  // Test Case 3: Verify retrieval commands listed
  test('TC3: Retrieval commands (GET, GETS) are listed', async ({ page }) => {
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Find the Retrieval category
    const retrievalCategory = commandsSection.locator('.command-category').filter({
      has: page.locator('h3', { hasText: 'Retrieval' })
    });
    await expect(retrievalCategory).toBeVisible();

    // Get the list of retrieval commands
    const retrievalList = retrievalCategory.locator('ul');
    await expect(retrievalList).toBeVisible();

    // Verify each retrieval command is listed using exact text match
    const retrievalCommands = ['GET', 'GETS'];
    for (const command of retrievalCommands) {
      const commandItem = retrievalList.getByText(command, { exact: true });
      await expect(commandItem).toBeVisible();
    }
  });

  // Test Case 4: Verify MirDB-specific commands listed
  test('TC4: MirDB-specific commands (INFO, MAJOR_COMPACTION) are listed', async ({ page }) => {
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Find the MirDB Specific category
    const mirdbCategory = commandsSection.locator('.command-category').filter({
      has: page.locator('h3', { hasText: /MirDB/i })
    });
    await expect(mirdbCategory).toBeVisible();

    // Get the list of MirDB-specific commands
    const mirdbList = mirdbCategory.locator('ul');
    await expect(mirdbList).toBeVisible();

    // Verify each MirDB-specific command is listed
    const mirdbCommands = ['INFO', 'MAJOR_COMPACTION'];
    for (const command of mirdbCommands) {
      const commandItem = mirdbList.locator('li', { hasText: command });
      await expect(commandItem).toBeVisible();
    }
  });

  // Additional test: Verify deletion commands are also listed (bonus coverage)
  test('Deletion commands (DELETE) are listed', async ({ page }) => {
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Find the Deletion category
    const deletionCategory = commandsSection.locator('.command-category').filter({
      has: page.locator('h3', { hasText: 'Deletion' })
    });
    await expect(deletionCategory).toBeVisible();

    // Verify DELETE command is listed
    const deletionList = deletionCategory.locator('ul');
    const deleteCommand = deletionList.locator('li', { hasText: 'DELETE' });
    await expect(deleteCommand).toBeVisible();
  });

  // Test navigation to commands section from internal link
  test('Commands section is accessible via navigation', async ({ page }) => {
    // Scroll to top first
    await page.evaluate(() => window.scrollTo(0, 0));

    // The commands section should be reachable by scrolling
    const commandsSection = page.locator('#commands');

    // Scroll to the commands section
    await commandsSection.scrollIntoViewIfNeeded();

    // Verify it's visible
    await expect(commandsSection).toBeVisible();
    await expect(commandsSection).toBeInViewport();
  });
});
