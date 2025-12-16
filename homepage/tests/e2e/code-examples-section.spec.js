// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for Code Examples Section with Memcached Commands
 * Scenario: Verify that code examples demonstrating basic Memcached protocol usage
 * are displayed correctly as per REQ-4
 */

test.describe('Code Examples Section with Memcached Commands', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to homepage before each test
    await page.goto('/');
    // Scroll to code examples section
    await page.locator('#code-examples').scrollIntoViewIfNeeded();
  });

  /**
   * Test Case 1: Check for get command example
   * Input: Check for get command example
   * Expected: Code example shows 'get <key>' command with description
   */
  test('TC1: Get command example is displayed with description', async ({ page }) => {
    // Verify code examples section is visible
    const codeExamplesSection = page.locator('[data-testid="code-examples-section"]');
    await expect(codeExamplesSection).toBeVisible();

    // Verify section title
    const sectionTitle = page.locator('[data-testid="code-examples-title"]');
    await expect(sectionTitle).toBeVisible();
    await expect(sectionTitle).toContainText('Memcached Commands');

    // Verify get command card exists
    const getCommandCard = page.locator('[data-testid="command-card-get"]');
    await expect(getCommandCard).toBeVisible();

    // Verify command title shows 'get <key>'
    const getCommandTitle = page.locator('[data-testid="command-title-get"]');
    await expect(getCommandTitle).toBeVisible();
    const titleText = await getCommandTitle.textContent();
    expect(titleText).toContain('get');
    expect(titleText).toContain('<key>');

    // Verify description is present
    const getCommandDesc = page.locator('[data-testid="command-desc-get"]');
    await expect(getCommandDesc).toBeVisible();
    const descText = await getCommandDesc.textContent();
    expect(descText).toBeTruthy();
    expect(descText.toLowerCase()).toContain('retrieve');

    // Verify code block shows the get command
    const codeBlock = page.locator('[data-testid="code-block-get"]');
    await expect(codeBlock).toBeVisible();

    const codeContent = page.locator('[data-testid="code-get"]');
    await expect(codeContent).toBeVisible();
    const codeText = await codeContent.textContent();
    expect(codeText).toContain('get');
  });

  /**
   * Test Case 2: Check for set command example
   * Input: Check for set command example
   * Expected: Code example shows 'set <key> <value>' command with description
   */
  test('TC2: Set command example is displayed with description', async ({ page }) => {
    // Verify set command card exists
    const setCommandCard = page.locator('[data-testid="command-card-set"]');
    await expect(setCommandCard).toBeVisible();

    // Verify command title shows 'set <key> <value>'
    const setCommandTitle = page.locator('[data-testid="command-title-set"]');
    await expect(setCommandTitle).toBeVisible();
    const titleText = await setCommandTitle.textContent();
    expect(titleText).toContain('set');
    expect(titleText).toContain('<key>');
    expect(titleText).toContain('<value>');

    // Verify description is present
    const setCommandDesc = page.locator('[data-testid="command-desc-set"]');
    await expect(setCommandDesc).toBeVisible();
    const descText = await setCommandDesc.textContent();
    expect(descText).toBeTruthy();
    expect(descText.toLowerCase()).toContain('store');

    // Verify code block shows the set command
    const codeContent = page.locator('[data-testid="code-set"]');
    await expect(codeContent).toBeVisible();
    const codeText = await codeContent.textContent();
    expect(codeText).toContain('set');
  });

  /**
   * Test Case 3: Check for delete command example
   * Input: Check for delete command example
   * Expected: Code example shows 'delete <key>' command with description
   */
  test('TC3: Delete command example is displayed with description', async ({ page }) => {
    // Verify delete command card exists
    const deleteCommandCard = page.locator('[data-testid="command-card-delete"]');
    await expect(deleteCommandCard).toBeVisible();

    // Verify command title shows 'delete <key>'
    const deleteCommandTitle = page.locator('[data-testid="command-title-delete"]');
    await expect(deleteCommandTitle).toBeVisible();
    const titleText = await deleteCommandTitle.textContent();
    expect(titleText).toContain('delete');
    expect(titleText).toContain('<key>');

    // Verify description is present
    const deleteCommandDesc = page.locator('[data-testid="command-desc-delete"]');
    await expect(deleteCommandDesc).toBeVisible();
    const descText = await deleteCommandDesc.textContent();
    expect(descText).toBeTruthy();
    expect(descText.toLowerCase()).toContain('remove');

    // Verify code block shows the delete command
    const codeContent = page.locator('[data-testid="code-delete"]');
    await expect(codeContent).toBeVisible();
    const codeText = await codeContent.textContent();
    expect(codeText).toContain('delete');
  });

  /**
   * Test Case 4: Check for additional commands (gets, add, replace, append, prepend)
   * Input: Check for additional commands (gets, add, replace, append, prepend)
   * Expected: Additional Memcached commands are documented with examples
   */
  test('TC4: Additional Memcached commands are documented with examples', async ({ page }) => {
    // Define additional commands to check
    const additionalCommands = [
      { id: 'gets', expectedTitle: 'gets', expectedDesc: 'cas' },
      { id: 'add', expectedTitle: 'add', expectedDesc: 'exist' },
      { id: 'replace', expectedTitle: 'replace', expectedDesc: 'exist' },
      { id: 'append', expectedTitle: 'append', expectedDesc: 'append' },
      { id: 'prepend', expectedTitle: 'prepend', expectedDesc: 'prepend' }
    ];

    for (const cmd of additionalCommands) {
      // Verify command card exists
      const commandCard = page.locator(`[data-testid="command-card-${cmd.id}"]`);
      await expect(commandCard).toBeVisible();

      // Verify command title contains expected command name
      const commandTitle = page.locator(`[data-testid="command-title-${cmd.id}"]`);
      await expect(commandTitle).toBeVisible();
      const titleText = await commandTitle.textContent();
      expect(titleText.toLowerCase()).toContain(cmd.expectedTitle);

      // Verify description exists
      const commandDesc = page.locator(`[data-testid="command-desc-${cmd.id}"]`);
      await expect(commandDesc).toBeVisible();
      const descText = await commandDesc.textContent();
      expect(descText).toBeTruthy();

      // Verify code block exists with command
      const codeContent = page.locator(`[data-testid="code-${cmd.id}"]`);
      await expect(codeContent).toBeVisible();
      const codeText = await codeContent.textContent();
      expect(codeText.toLowerCase()).toContain(cmd.id);
    }
  });

  /**
   * Test Case 5: Test copy-to-clipboard functionality
   * Input: Test copy-to-clipboard functionality
   * Expected: Clicking copy button copies code content to clipboard and shows confirmation
   */
  test('TC5: Copy-to-clipboard functionality works correctly', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    // Find the copy button for the get command
    const copyBtn = page.locator('[data-testid="copy-btn-get"]');
    await expect(copyBtn).toBeVisible();

    // Verify initial button text
    await expect(copyBtn).toHaveText('Copy');

    // Click the copy button
    await copyBtn.click();

    // Verify button changes to "Copied!"
    await expect(copyBtn).toHaveText('Copied!');

    // Verify button has the 'copied' class for styling
    await expect(copyBtn).toHaveClass(/copied/);

    // Verify the clipboard content matches the code
    const codeContent = page.locator('[data-testid="code-get"]');
    const expectedCode = await codeContent.textContent();

    const clipboardContent = await page.evaluate(() => navigator.clipboard.readText());
    expect(clipboardContent).toBe(expectedCode);

    // Wait for button to reset (2 seconds)
    await page.waitForTimeout(2100);

    // Verify button text is back to "Copy"
    await expect(copyBtn).toHaveText('Copy');

    // Verify 'copied' class is removed
    await expect(copyBtn).not.toHaveClass(/copied/);
  });

  /**
   * Test: Verify all command cards have copy buttons
   */
  test('TC5-B: All command cards have copy buttons', async ({ page }) => {
    const commands = ['get', 'set', 'delete', 'gets', 'add', 'replace', 'append', 'prepend'];

    for (const cmd of commands) {
      const copyBtn = page.locator(`[data-testid="copy-btn-${cmd}"]`);
      await expect(copyBtn).toBeVisible();
      await expect(copyBtn).toHaveText('Copy');
    }
  });

  /**
   * Test: Verify copy buttons have proper accessibility labels
   */
  test('TC5-C: Copy buttons have accessibility labels', async ({ page }) => {
    const copyBtns = page.locator('.copy-btn');
    const count = await copyBtns.count();

    expect(count).toBeGreaterThanOrEqual(8);

    for (let i = 0; i < count; i++) {
      const btn = copyBtns.nth(i);
      const ariaLabel = await btn.getAttribute('aria-label');
      expect(ariaLabel).toBeTruthy();
      expect(ariaLabel.toLowerCase()).toContain('copy');
    }
  });

  /**
   * Test: Verify code blocks have proper structure
   */
  test('TC-Structure: Code blocks have proper pre/code structure', async ({ page }) => {
    const codeBlocks = page.locator('.code-block');
    const count = await codeBlocks.count();

    expect(count).toBeGreaterThanOrEqual(8);

    for (let i = 0; i < count; i++) {
      const block = codeBlocks.nth(i);

      // Each code block should have a pre element
      const pre = block.locator('pre');
      await expect(pre).toBeVisible();

      // Each pre should have a code element
      const code = pre.locator('code');
      await expect(code).toBeVisible();

      // Code element should have language-memcached class
      const codeClass = await code.getAttribute('class');
      expect(codeClass).toContain('language-memcached');
    }
  });

  /**
   * Test: Verify section description is visible
   */
  test('TC-Description: Section has descriptive text', async ({ page }) => {
    const description = page.locator('[data-testid="code-examples-description"]');
    await expect(description).toBeVisible();

    const text = await description.textContent();
    expect(text.toLowerCase()).toContain('memcached');
    expect(text.toLowerCase()).toContain('commands');
  });

  /**
   * Test: Verify commands grid layout
   */
  test('TC-Layout: Commands are displayed in a grid layout', async ({ page }) => {
    const commandsGrid = page.locator('[data-testid="commands-grid"]');
    await expect(commandsGrid).toBeVisible();

    const display = await commandsGrid.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });

    expect(display).toBe('grid');
  });
});
