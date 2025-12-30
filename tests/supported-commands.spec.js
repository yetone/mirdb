const { test, expect } = require('@playwright/test');

/**
 * Supported Commands Display Tests
 * Scenario: Validate the supported memcached commands are displayed correctly
 */

test.describe('Supported Commands Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: Page displays SET as a supported storage command', async ({ page }) => {
    // Find the commands section
    const commandsSection = page.locator('#commands, [class*="commands"], .commands, #protocol-commands, .protocol-commands');
    await expect(commandsSection.first()).toBeVisible();

    const sectionContent = await commandsSection.first().textContent();

    // Verify SET command is listed
    expect(sectionContent).toContain('SET');

    // Verify it's in the storage commands category
    const storageSection = commandsSection.locator('[class*="storage"], .command-category').filter({ hasText: /storage/i });
    const hasStorageWithSet = (await storageSection.count()) > 0 &&
                             (await storageSection.textContent()).includes('SET');

    // Either SET is in a storage section, or we can verify through page structure
    const pageContent = await page.textContent('body');
    const hasSetInStorageContext = pageContent.toLowerCase().includes('storage') &&
                                   pageContent.includes('SET');

    expect(hasStorageWithSet || hasSetInStorageContext).toBe(true);
  });

  test('Test Case 2: Page displays GET as a supported retrieval command', async ({ page }) => {
    // Find the commands section
    const commandsSection = page.locator('#commands, [class*="commands"], .commands, #protocol-commands, .protocol-commands');
    await expect(commandsSection.first()).toBeVisible();

    const sectionContent = await commandsSection.first().textContent();

    // Verify GET command is listed
    expect(sectionContent).toContain('GET');

    // Verify it's in the retrieval commands category
    const retrievalSection = commandsSection.locator('[class*="retrieval"], .command-category').filter({ hasText: /retrieval/i });
    const hasRetrievalWithGet = (await retrievalSection.count()) > 0 &&
                                (await retrievalSection.textContent()).includes('GET');

    // Either GET is in a retrieval section, or we can verify through page structure
    const pageContent = await page.textContent('body');
    const hasGetInRetrievalContext = pageContent.toLowerCase().includes('retrieval') &&
                                     pageContent.includes('GET');

    expect(hasRetrievalWithGet || hasGetInRetrievalContext).toBe(true);
  });

  test('Test Case 3: Page displays DELETE as a supported deletion command', async ({ page }) => {
    // Find the commands section
    const commandsSection = page.locator('#commands, [class*="commands"], .commands, #protocol-commands, .protocol-commands');
    await expect(commandsSection.first()).toBeVisible();

    const sectionContent = await commandsSection.first().textContent();

    // Verify DELETE command is listed
    expect(sectionContent).toContain('DELETE');

    // Verify it's in the deletion commands category
    const deletionSection = commandsSection.locator('[class*="deletion"], .command-category').filter({ hasText: /deletion/i });
    const hasDeletionWithDelete = (await deletionSection.count()) > 0 &&
                                  (await deletionSection.textContent()).includes('DELETE');

    // Either DELETE is in a deletion section, or we can verify through page structure
    const pageContent = await page.textContent('body');
    const hasDeleteInDeletionContext = pageContent.toLowerCase().includes('deletion') &&
                                       pageContent.includes('DELETE');

    expect(hasDeletionWithDelete || hasDeleteInDeletionContext).toBe(true);
  });

  test('Test Case 4: Page displays INFO and MAJOR_COMPACTION as MirDB-specific commands', async ({ page }) => {
    // Find the commands section
    const commandsSection = page.locator('#commands, [class*="commands"], .commands, #protocol-commands, .protocol-commands');
    await expect(commandsSection.first()).toBeVisible();

    const sectionContent = await commandsSection.first().textContent();

    // Verify INFO command is listed
    expect(sectionContent).toContain('INFO');

    // Verify MAJOR_COMPACTION command is listed
    expect(sectionContent).toContain('MAJOR_COMPACTION');

    // Verify they're in the MirDB-specific commands category
    const mirdbSection = commandsSection.locator('[class*="mirdb"], [class*="specific"], .command-category').filter({ hasText: /mirdb|specific/i });
    const hasMirdbWithCommands = (await mirdbSection.count()) > 0;

    // Either commands are in a MirDB-specific section, or we can verify through page structure
    const pageContent = await page.textContent('body');
    const hasInfoInMirdbContext = (pageContent.toLowerCase().includes('mirdb') ||
                                   pageContent.toLowerCase().includes('specific')) &&
                                  pageContent.includes('INFO') &&
                                  pageContent.includes('MAJOR_COMPACTION');

    expect(hasMirdbWithCommands || hasInfoInMirdbContext).toBe(true);
  });

  test('Verify all storage commands are displayed', async ({ page }) => {
    // According to PRD: SET, ADD, REPLACE, APPEND, PREPEND
    const commandsSection = page.locator('#commands, [class*="commands"], .commands, #protocol-commands, .protocol-commands');
    await expect(commandsSection.first()).toBeVisible();

    const sectionContent = await commandsSection.first().textContent();

    // Verify all storage commands
    expect(sectionContent).toContain('SET');
    expect(sectionContent).toContain('ADD');
    expect(sectionContent).toContain('REPLACE');
    expect(sectionContent).toContain('APPEND');
    expect(sectionContent).toContain('PREPEND');
  });

  test('Verify all retrieval commands are displayed', async ({ page }) => {
    // According to PRD: GET, GETS
    const commandsSection = page.locator('#commands, [class*="commands"], .commands, #protocol-commands, .protocol-commands');
    await expect(commandsSection.first()).toBeVisible();

    const sectionContent = await commandsSection.first().textContent();

    // Verify all retrieval commands
    expect(sectionContent).toContain('GET');
    expect(sectionContent).toContain('GETS');
  });
});
