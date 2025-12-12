import { test, expect } from '@playwright/test';

/**
 * E2E Tests for Supported Commands Documentation
 * Scenario: Verify that all supported memcached commands are documented with usage examples
 */

test.describe('Supported Commands Documentation', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to homepage before each test
    await page.goto('/');
  });

  /**
   * Test Case 1: Check for SET command documentation
   * Input: Check for SET command documentation
   * Expected: SET command is documented with syntax and usage example
   */
  test('should display SET command with syntax and usage example', async ({ page }) => {
    // Navigate to commands section
    const commandsSection = page.locator('[data-testid="commands-section"]');
    await expect(commandsSection).toBeVisible();

    // Verify SET command block exists
    const setCommand = page.locator('[data-testid="command-set"]');
    await expect(setCommand).toBeVisible();

    // Verify SET command has heading
    const setHeading = setCommand.locator('h4');
    await expect(setHeading).toContainText('SET');

    // Verify SET command has description
    const setDescription = setCommand.locator('p');
    await expect(setDescription).toBeVisible();

    // Verify SET command has syntax and example
    const setCode = setCommand.locator('code');
    const codeContent = await setCode.textContent();
    expect(codeContent).toBeTruthy();
    expect(codeContent?.toLowerCase()).toContain('set');
    expect(codeContent).toContain('STORED');
  });

  /**
   * Test Case 2: Check for GET command documentation
   * Input: Check for GET command documentation
   * Expected: GET command is documented with syntax and usage example
   */
  test('should display GET command with syntax and usage example', async ({ page }) => {
    // Navigate to commands section
    const commandsSection = page.locator('[data-testid="commands-section"]');
    await expect(commandsSection).toBeVisible();

    // Verify GET command block exists
    const getCommand = page.locator('[data-testid="command-get"]');
    await expect(getCommand).toBeVisible();

    // Verify GET command has heading
    const getHeading = getCommand.locator('h4');
    await expect(getHeading).toContainText('GET');

    // Verify GET command has description
    const getDescription = getCommand.locator('p');
    await expect(getDescription).toBeVisible();

    // Verify GET command has syntax and example
    const getCode = getCommand.locator('code');
    const codeContent = await getCode.textContent();
    expect(codeContent).toBeTruthy();
    expect(codeContent?.toLowerCase()).toContain('get');
    expect(codeContent).toContain('VALUE');
    expect(codeContent).toContain('END');
  });

  /**
   * Test Case 3: Check for DELETE command documentation
   * Input: Check for DELETE command documentation
   * Expected: DELETE command is documented with syntax and usage example
   */
  test('should display DELETE command with syntax and usage example', async ({ page }) => {
    // Navigate to commands section
    const commandsSection = page.locator('[data-testid="commands-section"]');
    await expect(commandsSection).toBeVisible();

    // Verify DELETE command block exists
    const deleteCommand = page.locator('[data-testid="command-delete"]');
    await expect(deleteCommand).toBeVisible();

    // Verify DELETE command has heading
    const deleteHeading = deleteCommand.locator('h4');
    await expect(deleteHeading).toContainText('DELETE');

    // Verify DELETE command has description
    const deleteDescription = deleteCommand.locator('p');
    await expect(deleteDescription).toBeVisible();

    // Verify DELETE command has syntax and example
    const deleteCode = deleteCommand.locator('code');
    const codeContent = await deleteCode.textContent();
    expect(codeContent).toBeTruthy();
    expect(codeContent?.toLowerCase()).toContain('delete');
    expect(codeContent).toContain('DELETED');
  });

  /**
   * Test Case 4: Check for ADD command documentation
   * Input: Check for ADD command documentation
   * Expected: ADD command is documented
   */
  test('should display ADD command documentation', async ({ page }) => {
    // Navigate to commands section
    const commandsSection = page.locator('[data-testid="commands-section"]');
    await expect(commandsSection).toBeVisible();

    // Verify ADD command block exists
    const addCommand = page.locator('[data-testid="command-add"]');
    await expect(addCommand).toBeVisible();

    // Verify ADD command has heading
    const addHeading = addCommand.locator('h4');
    await expect(addHeading).toContainText('ADD');

    // Verify ADD command has description
    const addDescription = addCommand.locator('p');
    await expect(addDescription).toBeVisible();
    await expect(addDescription).toContainText('only if the key does not already exist');

    // Verify ADD command has code example
    const addCode = addCommand.locator('code');
    const codeContent = await addCode.textContent();
    expect(codeContent).toBeTruthy();
    expect(codeContent?.toLowerCase()).toContain('add');
  });

  /**
   * Test Case 5: Check for REPLACE command documentation
   * Input: Check for REPLACE command documentation
   * Expected: REPLACE command is documented
   */
  test('should display REPLACE command documentation', async ({ page }) => {
    // Navigate to commands section
    const commandsSection = page.locator('[data-testid="commands-section"]');
    await expect(commandsSection).toBeVisible();

    // Verify REPLACE command block exists
    const replaceCommand = page.locator('[data-testid="command-replace"]');
    await expect(replaceCommand).toBeVisible();

    // Verify REPLACE command has heading
    const replaceHeading = replaceCommand.locator('h4');
    await expect(replaceHeading).toContainText('REPLACE');

    // Verify REPLACE command has description
    const replaceDescription = replaceCommand.locator('p');
    await expect(replaceDescription).toBeVisible();
    await expect(replaceDescription).toContainText('only if the key already exists');

    // Verify REPLACE command has code example
    const replaceCode = replaceCommand.locator('code');
    const codeContent = await replaceCode.textContent();
    expect(codeContent).toBeTruthy();
    expect(codeContent?.toLowerCase()).toContain('replace');
  });

  /**
   * Test Case 6: Check for APPEND/PREPEND documentation
   * Input: Check for APPEND/PREPEND documentation
   * Expected: APPEND and PREPEND commands are documented
   */
  test('should display APPEND and PREPEND commands documentation', async ({ page }) => {
    // Navigate to commands section
    const commandsSection = page.locator('[data-testid="commands-section"]');
    await expect(commandsSection).toBeVisible();

    // Verify APPEND command block exists
    const appendCommand = page.locator('[data-testid="command-append"]');
    await expect(appendCommand).toBeVisible();

    // Verify APPEND command has heading
    const appendHeading = appendCommand.locator('h4');
    await expect(appendHeading).toContainText('APPEND');

    // Verify APPEND command has description about appending data
    const appendDescription = appendCommand.locator('p');
    await expect(appendDescription).toContainText('Append data');

    // Verify PREPEND command block exists
    const prependCommand = page.locator('[data-testid="command-prepend"]');
    await expect(prependCommand).toBeVisible();

    // Verify PREPEND command has heading
    const prependHeading = prependCommand.locator('h4');
    await expect(prependHeading).toContainText('PREPEND');

    // Verify PREPEND command has description about prepending data
    const prependDescription = prependCommand.locator('p');
    await expect(prependDescription).toContainText('Prepend data');
  });

  /**
   * Test Case 7: Check for INFO command documentation
   * Input: Check for INFO command documentation
   * Expected: INFO command (MirDB-specific) is documented
   */
  test('should display INFO command (MirDB-specific) documentation', async ({ page }) => {
    // Navigate to commands section
    const commandsSection = page.locator('[data-testid="commands-section"]');
    await expect(commandsSection).toBeVisible();

    // Verify INFO command block exists
    const infoCommand = page.locator('[data-testid="command-info"]');
    await expect(infoCommand).toBeVisible();

    // Verify INFO command has heading
    const infoHeading = infoCommand.locator('h4');
    await expect(infoHeading).toContainText('INFO');

    // Verify INFO command has description mentioning it's MirDB-specific
    const infoDescription = infoCommand.locator('p');
    await expect(infoDescription).toBeVisible();
    await expect(infoDescription).toContainText('MirDB-specific');

    // Verify INFO command has code example
    const infoCode = infoCommand.locator('code');
    const codeContent = await infoCode.textContent();
    expect(codeContent).toBeTruthy();
    expect(codeContent?.toLowerCase()).toContain('info');
  });

  /**
   * Additional test: Verify commands section has proper structure
   */
  test('should have commands section with proper heading and categories', async ({ page }) => {
    // Verify commands section exists
    const commandsSection = page.locator('[data-testid="commands-section"]');
    await expect(commandsSection).toBeVisible();

    // Verify commands section has a heading
    const commandsHeading = page.locator('[data-testid="commands-heading"]');
    await expect(commandsHeading).toBeVisible();
    await expect(commandsHeading).toContainText('Supported Commands');

    // Verify Storage Commands category exists
    const storageHeading = page.locator('[data-testid="storage-commands-heading"]');
    await expect(storageHeading).toBeVisible();
    await expect(storageHeading).toContainText('Storage Commands');

    // Verify Retrieval Commands category exists
    const retrievalHeading = page.locator('[data-testid="retrieval-commands-heading"]');
    await expect(retrievalHeading).toBeVisible();
    await expect(retrievalHeading).toContainText('Retrieval Commands');

    // Verify Deletion Commands category exists
    const deletionHeading = page.locator('[data-testid="deletion-commands-heading"]');
    await expect(deletionHeading).toBeVisible();
    await expect(deletionHeading).toContainText('Deletion Commands');

    // Verify Admin Commands category exists
    const adminHeading = page.locator('[data-testid="admin-commands-heading"]');
    await expect(adminHeading).toBeVisible();
    await expect(adminHeading).toContainText('Admin Commands');
  });
});
