// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Supported Commands Reference (REQ-4)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Check commands section for getter commands
   * Verifies that 'get' and 'gets' commands are listed
   */
  test('should display getter commands (get, gets) in commands section', async ({ page }) => {
    // Navigate to commands section
    const commandsSection = page.locator('[data-testid="commands-section"]');
    await expect(commandsSection).toBeVisible();

    // Scroll to commands section to ensure it's in view
    await commandsSection.scrollIntoViewIfNeeded();

    // Find the getter commands row
    const getterRow = page.locator('[data-testid="getter-commands-row"]');
    await expect(getterRow).toBeVisible();

    // Verify row contains 'get' and 'gets' commands
    const rowText = await getterRow.textContent();
    expect(rowText).toContain('get');
    expect(rowText).toContain('gets');

    // Verify the commands are displayed as code elements
    const getterCommands = getterRow.locator('code');
    await expect(getterCommands).toHaveCount(2);

    // Verify the specific commands
    const commands = await getterCommands.allTextContents();
    expect(commands).toContain('get');
    expect(commands).toContain('gets');
  });

  /**
   * Test Case 2: Check commands section for setter commands
   * Verifies that 'set', 'add', 'replace', 'append', 'prepend' commands are listed
   */
  test('should display setter commands (set, add, replace, append, prepend) in commands section', async ({ page }) => {
    // Navigate to commands section
    const commandsSection = page.locator('[data-testid="commands-section"]');
    await expect(commandsSection).toBeVisible();

    // Scroll to commands section
    await commandsSection.scrollIntoViewIfNeeded();

    // Find the setter commands row
    const setterRow = page.locator('[data-testid="setter-commands-row"]');
    await expect(setterRow).toBeVisible();

    // Verify row contains all setter commands
    const rowText = await setterRow.textContent();
    expect(rowText).toContain('set');
    expect(rowText).toContain('add');
    expect(rowText).toContain('replace');
    expect(rowText).toContain('append');
    expect(rowText).toContain('prepend');

    // Verify the commands are displayed as code elements
    const setterCommands = setterRow.locator('code');
    await expect(setterCommands).toHaveCount(5);

    // Verify the specific commands
    const commands = await setterCommands.allTextContents();
    expect(commands).toContain('set');
    expect(commands).toContain('add');
    expect(commands).toContain('replace');
    expect(commands).toContain('append');
    expect(commands).toContain('prepend');
  });

  /**
   * Test Case 3: Check commands section for delete command
   * Verifies that 'delete' command is listed
   */
  test('should display delete command in commands section', async ({ page }) => {
    // Navigate to commands section
    const commandsSection = page.locator('[data-testid="commands-section"]');
    await expect(commandsSection).toBeVisible();

    // Scroll to commands section
    await commandsSection.scrollIntoViewIfNeeded();

    // Find the other commands row
    const otherRow = page.locator('[data-testid="other-commands-row"]');
    await expect(otherRow).toBeVisible();

    // Verify row contains 'delete' command
    const rowText = await otherRow.textContent();
    expect(rowText).toContain('delete');

    // Verify the command is displayed as a code element
    const deleteCommand = otherRow.locator('code');
    await expect(deleteCommand).toHaveCount(1);
    await expect(deleteCommand).toHaveText('delete');
  });

  /**
   * Test Case 4: Verify command presentation format
   * Verifies that commands are displayed in a table or organized list format
   */
  test('should display commands in a table format with proper structure', async ({ page }) => {
    // Navigate to commands section
    const commandsSection = page.locator('[data-testid="commands-section"]');
    await expect(commandsSection).toBeVisible();

    // Scroll to commands section
    await commandsSection.scrollIntoViewIfNeeded();

    // Verify the commands-table container exists
    const commandsTable = page.locator('[data-testid="commands-table"]');
    await expect(commandsTable).toBeVisible();

    // Verify it contains an actual HTML table
    const table = commandsTable.locator('table');
    await expect(table).toBeVisible();

    // Verify table has thead and tbody
    const thead = table.locator('thead');
    const tbody = table.locator('tbody');
    await expect(thead).toBeVisible();
    await expect(tbody).toBeVisible();

    // Verify table headers
    const headers = thead.locator('th');
    await expect(headers).toHaveCount(2);
    const headerTexts = await headers.allTextContents();
    expect(headerTexts).toContain('Operation');
    expect(headerTexts).toContain('Commands');

    // Verify table has 3 data rows (getter, setter, other)
    const dataRows = tbody.locator('tr');
    await expect(dataRows).toHaveCount(3);
  });

  /**
   * Additional test: Commands section has proper heading
   */
  test('should display Supported Commands heading', async ({ page }) => {
    const commandsSection = page.locator('[data-testid="commands-section"]');
    await expect(commandsSection).toBeVisible();

    const heading = commandsSection.locator('h2');
    await expect(heading).toHaveText('Supported Commands');
  });

  /**
   * Additional test: Commands section is accessible via navigation
   */
  test('should be navigable via anchor link', async ({ page }) => {
    // Navigate directly to commands section using anchor
    await page.goto('/#commands');

    // Wait for potential scroll animation
    await page.waitForTimeout(500);

    // Verify commands section is visible and in viewport
    const commandsSection = page.locator('[data-testid="commands-section"]');
    await expect(commandsSection).toBeVisible();
  });

  /**
   * Additional test: All command categories are properly labeled
   */
  test('should have properly labeled command categories', async ({ page }) => {
    const commandsSection = page.locator('[data-testid="commands-section"]');
    await expect(commandsSection).toBeVisible();
    await commandsSection.scrollIntoViewIfNeeded();

    // Verify Getter category
    const getterRow = page.locator('[data-testid="getter-commands-row"]');
    const getterLabel = getterRow.locator('td').first();
    await expect(getterLabel).toHaveText('Getter');

    // Verify Setter category
    const setterRow = page.locator('[data-testid="setter-commands-row"]');
    const setterLabel = setterRow.locator('td').first();
    await expect(setterLabel).toHaveText('Setter');

    // Verify Other category
    const otherRow = page.locator('[data-testid="other-commands-row"]');
    const otherLabel = otherRow.locator('td').first();
    await expect(otherLabel).toHaveText('Other');
  });
});
