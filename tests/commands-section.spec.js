// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Supported Commands Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Commands section lists storage commands - SET, ADD, REPLACE, APPEND, PREPEND', async ({ page }) => {
    // Navigate to commands section
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Find the storage commands category
    const storageCategory = commandsSection.locator('.command-category[data-category="storage"]');
    await expect(storageCategory).toBeVisible();

    // Verify title
    const title = storageCategory.locator('h3');
    await expect(title).toContainText('Storage Commands');

    // Get all command list items
    const commandList = storageCategory.locator('.command-list');
    const commandItems = commandList.locator('li');

    // Verify all storage commands are listed
    const storageCommands = ['SET', 'ADD', 'REPLACE', 'APPEND', 'PREPEND'];
    for (const cmd of storageCommands) {
      const cmdElement = commandList.locator(`code:has-text("${cmd}")`);
      await expect(cmdElement).toBeVisible();
    }

    // Verify we have exactly 5 storage commands
    await expect(commandItems).toHaveCount(5);
  });

  test('TC2: Commands section lists retrieval commands - GET and GETS', async ({ page }) => {
    // Navigate to commands section
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Find the retrieval commands category
    const retrievalCategory = commandsSection.locator('.command-category[data-category="retrieval"]');
    await expect(retrievalCategory).toBeVisible();

    // Verify title
    const title = retrievalCategory.locator('h3');
    await expect(title).toContainText('Retrieval Commands');

    // Get command list
    const commandList = retrievalCategory.locator('.command-list');
    const commandItems = commandList.locator('li');

    // Verify GET command is listed (using exact text match)
    const getCmd = commandList.getByRole('listitem').filter({ hasText: 'GET' }).first();
    await expect(getCmd).toBeVisible();
    await expect(getCmd.locator('code')).toHaveText('GET');

    // Verify GETS command is listed
    const getsCmd = commandList.getByRole('listitem').filter({ hasText: 'GETS' });
    await expect(getsCmd).toBeVisible();
    await expect(getsCmd.locator('code')).toHaveText('GETS');

    // Verify we have exactly 2 retrieval commands
    await expect(commandItems).toHaveCount(2);
  });

  test('TC3: Commands section lists DELETE command', async ({ page }) => {
    // Navigate to commands section
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Find the deletion commands category
    const deletionCategory = commandsSection.locator('.command-category[data-category="deletion"]');
    await expect(deletionCategory).toBeVisible();

    // Verify title
    const title = deletionCategory.locator('h3');
    await expect(title).toContainText('Deletion Commands');

    // Get command list
    const commandList = deletionCategory.locator('.command-list');
    const commandItems = commandList.locator('li');

    // Verify DELETE command is listed
    const deleteCmd = commandList.locator('code:has-text("DELETE")');
    await expect(deleteCmd).toBeVisible();

    // Verify we have exactly 1 deletion command
    await expect(commandItems).toHaveCount(1);
  });

  test('TC4: Commands section lists MirDB extensions - INFO and MAJOR_COMPACTION', async ({ page }) => {
    // Navigate to commands section
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Find the MirDB extensions category
    const extensionsCategory = commandsSection.locator('.command-category[data-category="mirdb-extensions"]');
    await expect(extensionsCategory).toBeVisible();

    // Verify title indicates these are MirDB-specific
    const title = extensionsCategory.locator('h3');
    await expect(title).toContainText('MirDB');

    // Get command list
    const commandList = extensionsCategory.locator('.command-list');
    const commandItems = commandList.locator('li');

    // Verify INFO and MAJOR_COMPACTION commands are listed
    const mirdbCommands = ['INFO', 'MAJOR_COMPACTION'];
    for (const cmd of mirdbCommands) {
      const cmdElement = commandList.locator(`code:has-text("${cmd}")`);
      await expect(cmdElement).toBeVisible();
    }

    // Verify we have exactly 2 MirDB extension commands
    await expect(commandItems).toHaveCount(2);
  });

  test('Commands section has navigation link in header', async ({ page }) => {
    // Check that navigation link to commands section exists
    const navLink = page.locator('nav .nav-links a[href="#commands"]');
    await expect(navLink).toBeVisible();
    await expect(navLink).toHaveText('Commands');

    // Click the link and verify navigation
    await navLink.click();

    // Verify the commands section is in view
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeInViewport();
  });

  test('Commands section has proper structure with title and intro', async ({ page }) => {
    const commandsSection = page.locator('#commands');

    // Check main heading
    const heading = commandsSection.locator('h2');
    await expect(heading).toContainText('Supported Commands');

    // Check intro paragraph about memcached protocol
    const intro = commandsSection.locator('.commands-intro');
    await expect(intro).toBeVisible();
    const introText = await intro.textContent();
    expect(introText).toContain('memcached');

    // Check that there are 4 command categories
    const categories = commandsSection.locator('.command-category');
    await expect(categories).toHaveCount(4);
  });

  test('Commands grid has two-column layout on desktop', async ({ page }) => {
    // Set desktop viewport
    await page.setViewportSize({ width: 1200, height: 800 });

    const commandsGrid = page.locator('.commands-grid');

    // Check that the grid is using 2 columns
    const gridStyle = await commandsGrid.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        display: style.display,
        gridTemplateColumns: style.gridTemplateColumns,
      };
    });

    expect(gridStyle.display).toBe('grid');
    // gridTemplateColumns should have 2 values (2 columns)
    const columnCount = gridStyle.gridTemplateColumns.split(' ').length;
    expect(columnCount).toBe(2);
  });
});
