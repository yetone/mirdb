import { test, expect } from '@playwright/test';

test.describe('Commands Reference Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Storage commands are listed (SET, ADD, REPLACE, APPEND, PREPEND)', async ({ page }) => {
    // Navigate to the commands section
    const commandsSection = page.locator('[data-testid="commands-section"]');
    await commandsSection.scrollIntoViewIfNeeded();
    await expect(commandsSection).toBeVisible();

    // Check for Storage commands category
    const storageCommands = page.locator('[data-testid="storage-commands"]');
    await expect(storageCommands).toBeVisible();

    // Verify category heading
    const storageHeading = storageCommands.locator('h3');
    await expect(storageHeading).toContainText('Storage Commands');

    // Get all command codes in the storage section
    const commandCodes = storageCommands.locator('code');
    const commandCount = await commandCodes.count();
    expect(commandCount).toBe(5);

    // Verify each required storage command is present
    const storageContent = await storageCommands.textContent();
    expect(storageContent).toContain('SET');
    expect(storageContent).toContain('ADD');
    expect(storageContent).toContain('REPLACE');
    expect(storageContent).toContain('APPEND');
    expect(storageContent).toContain('PREPEND');

    // Verify each command has a description
    const commandItems = storageCommands.locator('li');
    const itemCount = await commandItems.count();
    expect(itemCount).toBe(5);

    // Each list item should have both a code element and descriptive text
    for (let i = 0; i < itemCount; i++) {
      const item = commandItems.nth(i);
      const code = item.locator('code');
      await expect(code).toBeVisible();
      const itemText = await item.textContent();
      // Each item should have description text (contains " - ")
      expect(itemText).toMatch(/.+ - .+/);
    }
  });

  test('TC2: Retrieval commands are listed (GET, GETS)', async ({ page }) => {
    // Navigate to the commands section
    const commandsSection = page.locator('[data-testid="commands-section"]');
    await commandsSection.scrollIntoViewIfNeeded();
    await expect(commandsSection).toBeVisible();

    // Check for Retrieval commands category
    const retrievalCommands = page.locator('[data-testid="retrieval-commands"]');
    await expect(retrievalCommands).toBeVisible();

    // Verify category heading
    const retrievalHeading = retrievalCommands.locator('h3');
    await expect(retrievalHeading).toContainText('Retrieval Commands');

    // Get all command codes in the retrieval section
    const commandCodes = retrievalCommands.locator('code');
    const commandCount = await commandCodes.count();
    expect(commandCount).toBe(2);

    // Verify each required retrieval command is present
    const retrievalContent = await retrievalCommands.textContent();
    expect(retrievalContent).toContain('GET');
    expect(retrievalContent).toContain('GETS');

    // Verify each command has a description
    const commandItems = retrievalCommands.locator('li');
    const itemCount = await commandItems.count();
    expect(itemCount).toBe(2);

    for (let i = 0; i < itemCount; i++) {
      const item = commandItems.nth(i);
      const code = item.locator('code');
      await expect(code).toBeVisible();
      const itemText = await item.textContent();
      expect(itemText).toMatch(/.+ - .+/);
    }
  });

  test('TC3: Deletion command is listed (DELETE)', async ({ page }) => {
    // Navigate to the commands section
    const commandsSection = page.locator('[data-testid="commands-section"]');
    await commandsSection.scrollIntoViewIfNeeded();
    await expect(commandsSection).toBeVisible();

    // Check for Deletion commands category
    const deletionCommands = page.locator('[data-testid="deletion-commands"]');
    await expect(deletionCommands).toBeVisible();

    // Verify category heading
    const deletionHeading = deletionCommands.locator('h3');
    await expect(deletionHeading).toContainText('Deletion Commands');

    // Get all command codes in the deletion section
    const commandCodes = deletionCommands.locator('code');
    const commandCount = await commandCodes.count();
    expect(commandCount).toBe(1);

    // Verify DELETE command is present
    const deletionContent = await deletionCommands.textContent();
    expect(deletionContent).toContain('DELETE');

    // Verify the command has a description
    const commandItem = deletionCommands.locator('li');
    await expect(commandItem).toBeVisible();
    const itemText = await commandItem.textContent();
    expect(itemText).toMatch(/.+ - .+/);
  });

  test('TC4: Admin commands are listed (INFO, MAJOR_COMPACTION)', async ({ page }) => {
    // Navigate to the commands section
    const commandsSection = page.locator('[data-testid="commands-section"]');
    await commandsSection.scrollIntoViewIfNeeded();
    await expect(commandsSection).toBeVisible();

    // Check for Admin commands category
    const adminCommands = page.locator('[data-testid="admin-commands"]');
    await expect(adminCommands).toBeVisible();

    // Verify category heading
    const adminHeading = adminCommands.locator('h3');
    await expect(adminHeading).toContainText('Admin Commands');

    // Get all command codes in the admin section
    const commandCodes = adminCommands.locator('code');
    const commandCount = await commandCodes.count();
    expect(commandCount).toBe(2);

    // Verify each required admin command is present
    const adminContent = await adminCommands.textContent();
    expect(adminContent).toContain('INFO');
    expect(adminContent).toContain('MAJOR_COMPACTION');

    // Verify each command has a description
    const commandItems = adminCommands.locator('li');
    const itemCount = await commandItems.count();
    expect(itemCount).toBe(2);

    for (let i = 0; i < itemCount; i++) {
      const item = commandItems.nth(i);
      const code = item.locator('code');
      await expect(code).toBeVisible();
      const itemText = await item.textContent();
      expect(itemText).toMatch(/.+ - .+/);
    }
  });

  test('TC5: Each command has a brief description of its behavior', async ({ page }) => {
    // Navigate to the commands section
    const commandsSection = page.locator('[data-testid="commands-section"]');
    await commandsSection.scrollIntoViewIfNeeded();
    await expect(commandsSection).toBeVisible();

    // Verify section heading
    const sectionHeading = commandsSection.locator('h2');
    await expect(sectionHeading).toContainText('Supported Commands');

    // Check all four command categories are present
    const categories = [
      'storage-commands',
      'retrieval-commands',
      'deletion-commands',
      'admin-commands'
    ];

    for (const category of categories) {
      const categoryElement = page.locator(`[data-testid="${category}"]`);
      await expect(categoryElement).toBeVisible();
    }

    // Verify all command items in all categories have descriptions
    const allCommandItems = commandsSection.locator('.command-list li');
    const totalItems = await allCommandItems.count();

    // Total should be: 5 (storage) + 2 (retrieval) + 1 (deletion) + 2 (admin) = 10
    expect(totalItems).toBe(10);

    // Each item should have a code element and a description (format: "COMMAND - description")
    for (let i = 0; i < totalItems; i++) {
      const item = allCommandItems.nth(i);

      // Should have a code element for the command name
      const code = item.locator('code');
      await expect(code).toBeVisible();

      // The item text should follow the format "COMMAND - description"
      const itemText = await item.textContent();
      expect(itemText).toBeTruthy();
      expect(itemText).toMatch(/^[A-Z_]+ - .+$/);

      // Description part should have meaningful content (more than just a few characters)
      const parts = itemText!.split(' - ');
      expect(parts.length).toBe(2);
      expect(parts[1].length).toBeGreaterThan(5);
    }
  });

  test('Navigation to Commands section works', async ({ page }) => {
    // Click on the Commands link in navigation
    await page.click('a[href="#commands"]');

    // Verify the section is in view
    const commandsSection = page.locator('[data-testid="commands-section"]');
    await expect(commandsSection).toBeInViewport();
  });
});
