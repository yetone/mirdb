/**
 * Supported Commands E2E Tests
 * Owner: Scenario 5 - Supported Commands Section
 *
 * Tests:
 * - Commands list present
 * - SET command documented
 * - GET command documented
 * - DELETE command documented
 * - Monospace code formatting
 */

const { test, expect } = require('@playwright/test');

test.describe('Supported Commands Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case 1: Query commands section for command list
  test('commands section contains list of supported memcached commands', async ({ page }) => {
    // Navigate to commands section
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Verify section heading
    await expect(commandsSection.locator('h2')).toHaveText('Supported Commands');

    // Verify command categories are present
    const commandCategories = commandsSection.locator('.command-category');
    await expect(commandCategories).toHaveCount(4); // Storage, Retrieval, Deletion, MirDB-Specific

    // Verify specific commands are listed
    const setCommand = commandsSection.locator('[data-command="set"]');
    await expect(setCommand).toBeVisible();

    const getCommand = commandsSection.locator('[data-command="get"]');
    await expect(getCommand).toBeVisible();

    const deleteCommand = commandsSection.locator('[data-command="delete"]');
    await expect(deleteCommand).toBeVisible();
  });

  // Test Case 2: Check for SET command documentation
  test('SET command listed with syntax: set <key> <flags> <exptime> <bytes>', async ({ page }) => {
    const commandsSection = page.locator('#commands');
    const setCommand = commandsSection.locator('[data-command="set"]');

    // Verify SET heading
    await expect(setCommand.locator('h4')).toHaveText('SET');

    // Verify SET description is present
    await expect(setCommand.locator('p').first()).toContainText('Store a key-value pair');

    // Verify syntax contains the required elements
    const syntaxBlock = setCommand.locator('pre code').first();
    const syntaxText = await syntaxBlock.textContent();
    expect(syntaxText).toContain('set');
    expect(syntaxText).toContain('<key>');
    expect(syntaxText).toContain('<flags>');
    expect(syntaxText).toContain('<exptime>');
    expect(syntaxText).toContain('<bytes>');
  });

  // Test Case 3: Check for GET command documentation
  test('GET command listed with syntax: get <key>', async ({ page }) => {
    const commandsSection = page.locator('#commands');
    const getCommand = commandsSection.locator('[data-command="get"]');

    // Verify GET heading
    await expect(getCommand.locator('h4')).toHaveText('GET');

    // Verify GET description is present
    await expect(getCommand.locator('p').first()).toContainText('Retrieve');

    // Verify syntax contains the required elements
    const syntaxBlock = getCommand.locator('pre code').first();
    const syntaxText = await syntaxBlock.textContent();
    expect(syntaxText).toContain('get');
    expect(syntaxText).toContain('<key>');
  });

  // Test Case 4: Check for DELETE command documentation
  test('DELETE command listed with appropriate syntax', async ({ page }) => {
    const commandsSection = page.locator('#commands');
    const deleteCommand = commandsSection.locator('[data-command="delete"]');

    // Verify DELETE heading
    await expect(deleteCommand.locator('h4')).toHaveText('DELETE');

    // Verify DELETE description is present
    await expect(deleteCommand.locator('p').first()).toContainText('Remove a key');

    // Verify syntax contains the required elements
    const syntaxBlock = deleteCommand.locator('pre code').first();
    const syntaxText = await syntaxBlock.textContent();
    expect(syntaxText).toContain('delete');
    expect(syntaxText).toContain('<key>');
  });

  // Test Case 5: Verify command examples use proper code formatting (E2E aspect)
  test('commands are displayed with pre/code elements', async ({ page }) => {
    const commandsSection = page.locator('#commands');

    // Check that pre elements exist in commands section
    const preElements = commandsSection.locator('pre');
    const preCount = await preElements.count();
    expect(preCount).toBeGreaterThan(0);

    // Check that code elements exist within pre elements
    const codeElements = commandsSection.locator('pre code');
    const codeCount = await codeElements.count();
    expect(codeCount).toBeGreaterThan(0);

    // Verify code elements are visible
    await expect(codeElements.first()).toBeVisible();
  });

  // Additional test: Verify storage commands category
  test('storage commands category contains expected commands', async ({ page }) => {
    const storageCategory = page.locator('[data-category="storage"]');
    await expect(storageCategory).toBeVisible();
    await expect(storageCategory.locator('h3')).toHaveText('Storage Commands');

    // Verify all storage commands are present
    await expect(storageCategory.locator('[data-command="set"]')).toBeVisible();
    await expect(storageCategory.locator('[data-command="add"]')).toBeVisible();
    await expect(storageCategory.locator('[data-command="replace"]')).toBeVisible();
    await expect(storageCategory.locator('[data-command="append"]')).toBeVisible();
    await expect(storageCategory.locator('[data-command="prepend"]')).toBeVisible();
  });

  // Additional test: Verify retrieval commands category
  test('retrieval commands category contains expected commands', async ({ page }) => {
    const retrievalCategory = page.locator('[data-category="retrieval"]');
    await expect(retrievalCategory).toBeVisible();
    await expect(retrievalCategory.locator('h3')).toHaveText('Retrieval Commands');

    // Verify retrieval commands are present
    await expect(retrievalCategory.locator('[data-command="get"]')).toBeVisible();
    await expect(retrievalCategory.locator('[data-command="gets"]')).toBeVisible();
  });

  // Additional test: Verify deletion commands category
  test('deletion commands category contains expected commands', async ({ page }) => {
    const deletionCategory = page.locator('[data-category="deletion"]');
    await expect(deletionCategory).toBeVisible();
    await expect(deletionCategory.locator('h3')).toHaveText('Deletion Commands');

    // Verify delete command is present
    await expect(deletionCategory.locator('[data-command="delete"]')).toBeVisible();
  });

  // Additional test: Verify response codes section
  test('response codes section is present and visible', async ({ page }) => {
    const responseCodes = page.locator('.response-codes');
    await expect(responseCodes).toBeVisible();
    await expect(responseCodes.locator('h3')).toHaveText('Response Codes');

    // Verify some response codes are listed
    const responseItems = responseCodes.locator('.response-item');
    const count = await responseItems.count();
    expect(count).toBeGreaterThan(0);

    // Check for specific response codes
    await expect(responseCodes).toContainText('STORED');
    await expect(responseCodes).toContainText('DELETED');
    await expect(responseCodes).toContainText('NOT_FOUND');
  });

  // Additional test: Verify MirDB-specific commands
  test('MirDB-specific commands section is present', async ({ page }) => {
    const mirdbCategory = page.locator('[data-category="mirdb"]');
    await expect(mirdbCategory).toBeVisible();
    await expect(mirdbCategory.locator('h3')).toHaveText('MirDB-Specific Commands');

    // Verify MirDB-specific commands are present
    await expect(mirdbCategory.locator('[data-command="info"]')).toBeVisible();
    await expect(mirdbCategory.locator('[data-command="major_compaction"]')).toBeVisible();
  });

  // Test: Navigation to commands section works
  test('can navigate to commands section via anchor link', async ({ page }) => {
    // Click the commands link in navigation
    await page.locator('.nav-links a[href="#commands"]').click();

    // Verify commands section is in view
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeInViewport();
  });
});
