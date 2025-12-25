// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for Supported Commands Reference - Scenario ID: 20
 *
 * These tests verify that all Memcached commands supported by MirDB
 * are properly documented on the homepage.
 *
 * Scenario: Supported Commands Reference
 * UUID: c946c861-0ac4-410a-a45d-700574b2bc86
 */

test.describe('Supported Commands Reference', () => {

  test.beforeEach(async ({ page }) => {
    // Navigate to the homepage
    await page.goto('/');
    // Wait for the page to be fully loaded
    await page.waitForLoadState('domcontentloaded');
  });

  /**
   * Test Case 1: Storage commands list
   *
   * Verifies that SET, ADD, REPLACE, APPEND, PREPEND commands are documented
   * in the supported commands section.
   */
  test('should display storage commands (SET, ADD, REPLACE, APPEND, PREPEND)', async ({ page }) => {
    // Navigate to the commands section within quick start
    const commandsStep = page.locator('[data-testid="commands-step"]');
    await expect(commandsStep).toBeVisible();

    // Find the commands code block
    const commandsCode = page.locator('[data-testid="commands-code"]');
    await expect(commandsCode).toBeVisible();

    // Get the commands content
    const commandsContent = await commandsCode.locator('code').textContent();

    // Verify all storage commands are listed
    expect(commandsContent).toContain('SET');
    expect(commandsContent).toContain('ADD');
    expect(commandsContent).toContain('REPLACE');
    expect(commandsContent).toContain('APPEND');
    expect(commandsContent).toContain('PREPEND');

    // Verify storage commands section header is present
    expect(commandsContent).toContain('Storage Commands');
  });

  /**
   * Test Case 2: Retrieval commands list
   *
   * Verifies that GET and GETS commands are documented
   * in the supported commands section.
   */
  test('should display retrieval commands (GET, GETS)', async ({ page }) => {
    // Navigate to the commands section
    const commandsStep = page.locator('[data-testid="commands-step"]');
    await expect(commandsStep).toBeVisible();

    // Find the commands code block
    const commandsCode = page.locator('[data-testid="commands-code"]');
    await expect(commandsCode).toBeVisible();

    // Get the commands content
    const commandsContent = await commandsCode.locator('code').textContent();

    // Verify GET and GETS commands are listed
    expect(commandsContent).toContain('GET');
    expect(commandsContent).toContain('GETS');

    // Verify retrieval commands section header is present
    expect(commandsContent).toContain('Retrieval Commands');
  });

  /**
   * Test Case 3: Deletion command
   *
   * Verifies that DELETE command is documented
   * in the supported commands section.
   */
  test('should display deletion command (DELETE)', async ({ page }) => {
    // Navigate to the commands section
    const commandsStep = page.locator('[data-testid="commands-step"]');
    await expect(commandsStep).toBeVisible();

    // Find the commands code block
    const commandsCode = page.locator('[data-testid="commands-code"]');
    await expect(commandsCode).toBeVisible();

    // Get the commands content
    const commandsContent = await commandsCode.locator('code').textContent();

    // Verify DELETE command is listed
    expect(commandsContent).toContain('DELETE');

    // Verify deletion commands section header is present
    expect(commandsContent).toContain('Deletion Commands');
  });

  /**
   * Test Case 4: MirDB-specific commands
   *
   * Verifies that INFO and MAJOR_COMPACTION commands are documented
   * in the supported commands section. These are MirDB-specific commands
   * not part of standard Memcached protocol.
   */
  test('should display MirDB-specific commands (INFO, MAJOR_COMPACTION)', async ({ page }) => {
    // Navigate to the commands section
    const commandsStep = page.locator('[data-testid="commands-step"]');
    await expect(commandsStep).toBeVisible();

    // Find the commands code block
    const commandsCode = page.locator('[data-testid="commands-code"]');
    await expect(commandsCode).toBeVisible();

    // Get the commands content
    const commandsContent = await commandsCode.locator('code').textContent();

    // Verify INFO command is listed
    expect(commandsContent).toContain('INFO');

    // Verify MAJOR_COMPACTION command is listed
    expect(commandsContent).toContain('MAJOR_COMPACTION');

    // Verify MirDB-Specific commands section header is present
    expect(commandsContent).toContain('MirDB-Specific');

    // Verify command descriptions are present
    expect(commandsContent).toContain('server information');
    expect(commandsContent).toContain('compaction');
  });

  /**
   * Additional test: Commands section is navigable
   *
   * Verifies that users can navigate to the commands section
   * which is part of the Quick Start section.
   */
  test('should have navigable commands section within Quick Start', async ({ page }) => {
    // Click on Quick Start navigation link
    const quickStartLink = page.locator('a[href="#quick-start"]').first();
    await quickStartLink.click();

    // Wait for scroll to complete
    await page.waitForTimeout(500);

    // Verify the commands step is in viewport
    const commandsStep = page.locator('[data-testid="commands-step"]');
    await expect(commandsStep).toBeVisible();

    // Verify section title
    const commandsTitle = commandsStep.locator('h3');
    await expect(commandsTitle).toContainText('Supported Commands');
  });

  /**
   * Additional test: Commands section has proper structure
   *
   * Verifies that the commands documentation has proper
   * organization with categories.
   */
  test('should have properly structured commands documentation', async ({ page }) => {
    const commandsStep = page.locator('[data-testid="commands-step"]');
    await expect(commandsStep).toBeVisible();

    const commandsCode = page.locator('[data-testid="commands-code"]');
    const commandsContent = await commandsCode.locator('code').textContent();

    // Verify all four command categories are present
    const expectedCategories = [
      'Storage Commands',
      'Retrieval Commands',
      'Deletion Commands',
      'MirDB-Specific'
    ];

    for (const category of expectedCategories) {
      expect(commandsContent).toContain(category);
    }

    // Verify the code has proper syntax highlighting class
    const codeElement = commandsCode.locator('code');
    await expect(codeElement).toHaveClass(/language-rust/);
  });
});
