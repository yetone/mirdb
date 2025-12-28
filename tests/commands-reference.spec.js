// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Test Suite: Memcached Commands Reference
 * Scenario: Verify supported memcached commands are documented with examples
 */

test.describe('Memcached Commands Reference', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to the homepage before each test
    await page.goto('/');
  });

  /**
   * Test Case 1: Storage commands are documented
   * Input: Check commands section for storage commands
   * Expected: SET, ADD, REPLACE, APPEND, PREPEND commands are documented
   */
  test('TC1: Storage commands (SET, ADD, REPLACE, APPEND, PREPEND) are documented', async ({ page }) => {
    // Verify the commands section exists
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Verify the storage commands category exists
    const storageCategory = page.locator('[data-testid="commands-storage"]');
    await expect(storageCategory).toBeVisible();

    // Verify each storage command is documented
    const storageCommands = ['SET', 'ADD', 'REPLACE', 'APPEND', 'PREPEND'];

    for (const cmd of storageCommands) {
      const commandElement = page.locator(`[data-testid="command-${cmd.toLowerCase()}"]`);
      await expect(commandElement).toBeVisible();

      // Verify the command name is displayed
      const commandName = commandElement.locator('.command-name');
      await expect(commandName).toContainText(cmd);
    }
  });

  /**
   * Test Case 2: Retrieval commands are documented
   * Input: Check commands section for retrieval commands
   * Expected: GET and GETS commands are documented
   */
  test('TC2: Retrieval commands (GET, GETS) are documented', async ({ page }) => {
    // Verify the commands section exists
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Verify the retrieval commands category exists
    const retrievalCategory = page.locator('[data-testid="commands-retrieval"]');
    await expect(retrievalCategory).toBeVisible();

    // Verify each retrieval command is documented
    const retrievalCommands = ['GET', 'GETS'];

    for (const cmd of retrievalCommands) {
      const commandElement = page.locator(`[data-testid="command-${cmd.toLowerCase()}"]`);
      await expect(commandElement).toBeVisible();

      // Verify the command name is displayed
      const commandName = commandElement.locator('.command-name');
      await expect(commandName).toContainText(cmd);
    }
  });

  /**
   * Test Case 3: DELETE command is documented
   * Input: Check commands section for DELETE command
   * Expected: DELETE command is documented
   */
  test('TC3: DELETE command is documented', async ({ page }) => {
    // Verify the commands section exists
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Verify the deletion commands category exists
    const deletionCategory = page.locator('[data-testid="commands-deletion"]');
    await expect(deletionCategory).toBeVisible();

    // Verify DELETE command is documented
    const deleteCommand = page.locator('[data-testid="command-delete"]');
    await expect(deleteCommand).toBeVisible();

    // Verify the command name is displayed
    const commandName = deleteCommand.locator('.command-name');
    await expect(commandName).toContainText('DELETE');
  });

  /**
   * Test Case 4: MirDB-specific commands are documented
   * Input: Check commands section for MirDB-specific commands
   * Expected: INFO and MAJOR_COMPACTION commands are documented
   */
  test('TC4: MirDB-specific commands (INFO, MAJOR_COMPACTION) are documented', async ({ page }) => {
    // Verify the commands section exists
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Verify the MirDB-specific commands category exists
    const mirdbCategory = page.locator('[data-testid="commands-mirdb"]');
    await expect(mirdbCategory).toBeVisible();

    // Verify INFO command is documented
    const infoCommand = page.locator('[data-testid="command-info"]');
    await expect(infoCommand).toBeVisible();
    const infoName = infoCommand.locator('.command-name');
    await expect(infoName).toContainText('INFO');

    // Verify MAJOR_COMPACTION command is documented
    const compactionCommand = page.locator('[data-testid="command-major_compaction"]');
    await expect(compactionCommand).toBeVisible();
    const compactionName = compactionCommand.locator('.command-name');
    await expect(compactionName).toContainText('MAJOR_COMPACTION');
  });

  /**
   * Test Case 5: Command examples include code snippets with syntax highlighting
   * Input: Verify command examples include code snippets
   * Expected: Each command has a code example with syntax highlighting
   */
  test('TC5: Command examples include code snippets with syntax highlighting', async ({ page }) => {
    // Verify the commands section exists
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // List of all commands that should have code examples
    const allCommands = ['set', 'add', 'replace', 'append', 'prepend', 'get', 'gets', 'delete', 'info', 'major_compaction'];

    for (const cmd of allCommands) {
      const commandElement = page.locator(`[data-testid="command-${cmd}"]`);
      await expect(commandElement).toBeVisible();

      // Verify each command has a code example
      const codeExample = commandElement.locator('.command-example pre code, .command-example code');
      await expect(codeExample).toBeVisible();

      // Verify the code example is inside a styled code block (has syntax highlighting class or styled container)
      const codeContainer = commandElement.locator('.command-example');
      await expect(codeContainer).toBeVisible();

      // Verify the code block has appropriate styling for syntax highlighting
      const hasCodeStyling = await codeContainer.evaluate((el) => {
        const pre = el.querySelector('pre');
        const code = el.querySelector('code');
        if (!pre || !code) return false;

        const preStyles = window.getComputedStyle(pre);
        const codeStyles = window.getComputedStyle(code);

        // Check for monospace font family (indicates code styling)
        const hasMonospace = codeStyles.fontFamily.toLowerCase().includes('mono') ||
                           codeStyles.fontFamily.toLowerCase().includes('consolas') ||
                           codeStyles.fontFamily.toLowerCase().includes('courier');

        // Check for code block background styling
        const hasBackground = preStyles.backgroundColor !== 'rgba(0, 0, 0, 0)' &&
                            preStyles.backgroundColor !== 'transparent';

        return hasMonospace || hasBackground;
      });

      expect(hasCodeStyling).toBe(true);
    }
  });
});
