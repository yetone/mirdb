// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for Project Status Display
 * Scenario: Verify project status and feature availability is displayed as specified in REQ-6 and US-3
 * Related Requirements: REQ-6, US-3
 */

test.describe('Project Status Display', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to the homepage before each test
    await page.goto('/');
  });

  /**
   * Test Case 1: Project status section shows implemented vs planned features
   * Input: Check for project status section
   * Expected: Project status section shows implemented vs planned features
   */
  test('TC1: Project status section shows implemented vs planned features', async ({ page }) => {
    // Navigate to project status section
    const projectStatusSection = page.locator('[data-testid="project-status-section"]');
    await projectStatusSection.scrollIntoViewIfNeeded();
    await expect(projectStatusSection).toBeVisible();

    // Verify the section has a title
    const sectionTitle = projectStatusSection.locator('h2');
    await expect(sectionTitle).toBeVisible();
    await expect(sectionTitle).toHaveText('Project Status');

    // Verify implemented features card exists
    const implementedFeatures = page.locator('[data-testid="implemented-features"]');
    await expect(implementedFeatures).toBeVisible();

    // Verify implemented features title
    const implementedTitle = implementedFeatures.locator('.status-title');
    await expect(implementedTitle).toBeVisible();
    await expect(implementedTitle).toContainText('Implemented');

    // Verify implemented features list contains items
    const implementedList = page.locator('[data-testid="implemented-features-list"]');
    await expect(implementedList).toBeVisible();
    const implementedItems = implementedList.locator('li');
    const implementedCount = await implementedItems.count();
    expect(implementedCount).toBeGreaterThan(0);

    // Verify key implemented features are listed
    const implementedText = await implementedList.textContent();
    expect(implementedText.toLowerCase()).toContain('memcached');
    expect(implementedText.toLowerCase()).toContain('memtable');
    expect(implementedText.toLowerCase()).toContain('compaction');
    expect(implementedText.toLowerCase()).toContain('sstable');

    // Verify planned features card exists
    const plannedFeatures = page.locator('[data-testid="planned-features"]');
    await expect(plannedFeatures).toBeVisible();

    // Verify planned features title
    const plannedTitle = plannedFeatures.locator('.status-title');
    await expect(plannedTitle).toBeVisible();
    await expect(plannedTitle).toContainText('Planned');

    // Verify planned features list contains items
    const plannedList = page.locator('[data-testid="planned-features-list"]');
    await expect(plannedList).toBeVisible();
    const plannedItems = plannedList.locator('li');
    const plannedCount = await plannedItems.count();
    expect(plannedCount).toBeGreaterThan(0);

    // Verify Raft consensus is in planned features
    const plannedText = await plannedList.textContent();
    expect(plannedText.toLowerCase()).toContain('raft');
  });

  /**
   * Test Case 2: List of supported memcached commands is displayed
   * Input: Check for supported commands list
   * Expected: List of supported memcached commands is displayed (SET, GET, DELETE, etc.)
   */
  test('TC2: List of supported memcached commands is displayed', async ({ page }) => {
    // Navigate to project status section
    const projectStatusSection = page.locator('[data-testid="project-status-section"]');
    await projectStatusSection.scrollIntoViewIfNeeded();

    // Verify supported commands section exists
    const commandsSection = page.locator('[data-testid="supported-commands-section"]');
    await expect(commandsSection).toBeVisible();

    // Verify the section has a title for supported commands
    const commandsTitle = commandsSection.locator('h3');
    await expect(commandsTitle).toBeVisible();
    await expect(commandsTitle).toContainText('Supported Memcached Commands');

    // Verify commands description exists
    const commandsDescription = commandsSection.locator('.commands-description');
    await expect(commandsDescription).toBeVisible();
    const descriptionText = await commandsDescription.textContent();
    expect(descriptionText.toLowerCase()).toContain('memcached');
    expect(descriptionText.toLowerCase()).toContain('protocol');

    // Verify storage commands group exists
    const storageCommands = page.locator('[data-testid="storage-commands"]');
    await expect(storageCommands).toBeVisible();
    const storageList = page.locator('[data-testid="storage-commands-list"]');
    const storageText = await storageList.textContent();
    expect(storageText).toContain('SET');

    // Verify retrieval commands group exists
    const retrievalCommands = page.locator('[data-testid="retrieval-commands"]');
    await expect(retrievalCommands).toBeVisible();
    const retrievalList = page.locator('[data-testid="retrieval-commands-list"]');
    const retrievalText = await retrievalList.textContent();
    expect(retrievalText).toContain('GET');

    // Verify deletion commands group exists
    const deletionCommands = page.locator('[data-testid="deletion-commands"]');
    await expect(deletionCommands).toBeVisible();
    const deletionList = page.locator('[data-testid="deletion-commands-list"]');
    const deletionText = await deletionList.textContent();
    expect(deletionText).toContain('DELETE');
  });

  /**
   * Test Case 3: Storage commands are documented
   * Input: Check for storage commands documentation
   * Expected: Storage commands (SET, ADD, REPLACE, APPEND, PREPEND) are documented
   */
  test('TC3: Storage commands (SET, ADD, REPLACE, APPEND, PREPEND) are documented', async ({ page }) => {
    // Navigate to project status section
    const projectStatusSection = page.locator('[data-testid="project-status-section"]');
    await projectStatusSection.scrollIntoViewIfNeeded();

    // Verify storage commands section exists
    const storageCommands = page.locator('[data-testid="storage-commands"]');
    await expect(storageCommands).toBeVisible();

    // Verify storage commands title
    const storageTitle = storageCommands.locator('h4');
    await expect(storageTitle).toBeVisible();
    await expect(storageTitle).toContainText('Storage Commands');

    // Verify storage commands list
    const storageList = page.locator('[data-testid="storage-commands-list"]');
    await expect(storageList).toBeVisible();

    // Get all list items
    const listItems = storageList.locator('li');
    const itemCount = await listItems.count();
    expect(itemCount).toBe(5); // SET, ADD, REPLACE, APPEND, PREPEND

    // Verify each required storage command is present
    const storageText = await storageList.textContent();

    // Check SET command
    expect(storageText).toContain('SET');
    expect(storageText.toLowerCase()).toContain('store');

    // Check ADD command
    expect(storageText).toContain('ADD');
    expect(storageText.toLowerCase()).toContain("doesn't exist");

    // Check REPLACE command
    expect(storageText).toContain('REPLACE');
    expect(storageText.toLowerCase()).toContain('if key exists');

    // Check APPEND command
    expect(storageText).toContain('APPEND');
    expect(storageText.toLowerCase()).toContain('append');

    // Check PREPEND command
    expect(storageText).toContain('PREPEND');
    expect(storageText.toLowerCase()).toContain('prepend');

    // Verify each command is displayed with code formatting
    const codeElements = storageList.locator('code');
    const codeCount = await codeElements.count();
    expect(codeCount).toBe(5); // Each command should be in a code element
  });

  /**
   * Additional test: Verify all command groups are displayed
   */
  test('should display all command groups with proper structure', async ({ page }) => {
    // Navigate to project status section
    const projectStatusSection = page.locator('[data-testid="project-status-section"]');
    await projectStatusSection.scrollIntoViewIfNeeded();

    // Verify all four command groups exist
    const commandGroups = [
      'storage-commands',
      'retrieval-commands',
      'deletion-commands',
      'mirdb-commands'
    ];

    for (const groupId of commandGroups) {
      const group = page.locator(`[data-testid="${groupId}"]`);
      await expect(group).toBeVisible();

      // Verify each group has a title (h4)
      const groupTitle = group.locator('h4');
      await expect(groupTitle).toBeVisible();

      // Verify each group has a command list
      const groupList = group.locator('.command-list');
      await expect(groupList).toBeVisible();

      // Verify each list has at least one item
      const listItems = groupList.locator('li');
      const itemCount = await listItems.count();
      expect(itemCount).toBeGreaterThan(0);
    }
  });

  /**
   * Additional test: Verify MirDB-specific commands are documented
   */
  test('should display MirDB-specific commands', async ({ page }) => {
    // Navigate to project status section
    const projectStatusSection = page.locator('[data-testid="project-status-section"]');
    await projectStatusSection.scrollIntoViewIfNeeded();

    // Verify MirDB commands section
    const mirdbCommands = page.locator('[data-testid="mirdb-commands"]');
    await expect(mirdbCommands).toBeVisible();

    // Verify the title mentions MirDB-specific
    const mirdbTitle = mirdbCommands.locator('h4');
    await expect(mirdbTitle).toContainText('MirDB-Specific');

    // Verify INFO and MAJOR_COMPACTION commands are listed
    const mirdbList = page.locator('[data-testid="mirdb-commands-list"]');
    const mirdbText = await mirdbList.textContent();
    expect(mirdbText).toContain('INFO');
    expect(mirdbText).toContain('MAJOR_COMPACTION');
  });

  /**
   * Additional test: Verify navigation to project status section
   */
  test('should be able to scroll to project status section', async ({ page }) => {
    // Scroll to project status section
    const projectStatusSection = page.locator('[data-testid="project-status-section"]');
    await projectStatusSection.scrollIntoViewIfNeeded();

    // Verify the section is visible
    await expect(projectStatusSection).toBeVisible();

    // Verify section is in viewport
    await expect(projectStatusSection).toBeInViewport();
  });
});
