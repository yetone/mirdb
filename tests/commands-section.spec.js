// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Commands Reference Section E2E Tests (REQ-4)
 * Verifies the commands section displays supported memcached and MirDB commands
 */

test.describe('Commands Reference Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: GET command documentation
   * Input: Query for GET command documentation
   * Expected: GET command syntax and description are displayed
   */
  test('TC1: GET command syntax and description are displayed', async ({ page }) => {
    // Navigate to Commands section
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Query for GET command
    const getCommandRow = commandsSection.locator('tr, li').filter({ hasText: /^GET\b/ }).first();
    await expect(getCommandRow).toBeVisible();

    // Verify GET command has a description
    const rowText = await getCommandRow.textContent();
    expect(rowText).toContain('GET');
    expect(rowText?.toLowerCase()).toMatch(/retrieve|get|value|key/i);
  });

  /**
   * Test Case 2: SET command documentation
   * Input: Query for SET command documentation
   * Expected: SET command syntax and description are displayed
   */
  test('TC2: SET command syntax and description are displayed', async ({ page }) => {
    // Navigate to Commands section
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Query for SET command
    const setCommandRow = commandsSection.locator('tr, li').filter({ hasText: /^SET\b/ }).first();
    await expect(setCommandRow).toBeVisible();

    // Verify SET command has a description
    const rowText = await setCommandRow.textContent();
    expect(rowText).toContain('SET');
    expect(rowText?.toLowerCase()).toMatch(/store|set|key|value|pair/i);
  });

  /**
   * Test Case 3: DELETE command documentation
   * Input: Query for DELETE command documentation
   * Expected: DELETE command syntax and description are displayed
   */
  test('TC3: DELETE command syntax and description are displayed', async ({ page }) => {
    // Navigate to Commands section
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Query for DELETE command
    const deleteCommandRow = commandsSection.locator('tr, li').filter({ hasText: /DELETE/ });
    await expect(deleteCommandRow.first()).toBeVisible();

    // Verify DELETE command has a description
    const rowText = await deleteCommandRow.first().textContent();
    expect(rowText).toContain('DELETE');
    expect(rowText?.toLowerCase()).toMatch(/delete|remove|key/i);
  });

  /**
   * Test Case 4: INFO command documentation (MirDB-specific)
   * Input: Query for INFO command documentation
   * Expected: MirDB-specific INFO command is documented
   */
  test('TC4: MirDB-specific INFO command is documented', async ({ page }) => {
    // Navigate to Commands section
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Query for INFO command
    const infoCommandRow = commandsSection.locator('tr, li').filter({ hasText: /INFO/ });
    await expect(infoCommandRow.first()).toBeVisible();

    // Verify INFO command has a description
    const rowText = await infoCommandRow.first().textContent();
    expect(rowText).toContain('INFO');
    expect(rowText?.toLowerCase()).toMatch(/info|status|server|database|statistics/i);
  });

  /**
   * Test Case 5: MAJOR_COMPACTION command documentation (MirDB-specific)
   * Input: Query for MAJOR_COMPACTION command documentation
   * Expected: MirDB-specific MAJOR_COMPACTION command is documented
   */
  test('TC5: MirDB-specific MAJOR_COMPACTION command is documented', async ({ page }) => {
    // Navigate to Commands section
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Query for MAJOR_COMPACTION command
    const compactionCommandRow = commandsSection.locator('tr, li').filter({ hasText: /MAJOR_COMPACTION/ });
    await expect(compactionCommandRow.first()).toBeVisible();

    // Verify MAJOR_COMPACTION command has a description
    const rowText = await compactionCommandRow.first().textContent();
    expect(rowText).toContain('MAJOR_COMPACTION');
    expect(rowText?.toLowerCase()).toMatch(/compaction|compact|manual|trigger/i);
  });

  /**
   * Test Case 6: Storage commands (ADD, REPLACE, APPEND, PREPEND)
   * Input: Query for storage commands
   * Expected: All storage commands are documented with syntax
   */
  test('TC6: All storage commands (ADD, REPLACE, APPEND, PREPEND) are documented', async ({ page }) => {
    // Navigate to Commands section
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Verify ADD command
    const addCommandRow = commandsSection.locator('tr, li').filter({ hasText: /^ADD\b/ }).first();
    await expect(addCommandRow).toBeVisible();
    const addText = await addCommandRow.textContent();
    expect(addText).toContain('ADD');
    expect(addText?.toLowerCase()).toMatch(/store|add|exist|key/i);

    // Verify REPLACE command
    const replaceCommandRow = commandsSection.locator('tr, li').filter({ hasText: /REPLACE/ }).first();
    await expect(replaceCommandRow).toBeVisible();
    const replaceText = await replaceCommandRow.textContent();
    expect(replaceText).toContain('REPLACE');
    expect(replaceText?.toLowerCase()).toMatch(/store|replace|exist|key/i);

    // Verify APPEND command
    const appendCommandRow = commandsSection.locator('tr, li').filter({ hasText: /APPEND/ }).first();
    await expect(appendCommandRow).toBeVisible();
    const appendText = await appendCommandRow.textContent();
    expect(appendText).toContain('APPEND');
    expect(appendText?.toLowerCase()).toMatch(/append|add|data|value/i);

    // Verify PREPEND command
    const prependCommandRow = commandsSection.locator('tr, li').filter({ hasText: /PREPEND/ }).first();
    await expect(prependCommandRow).toBeVisible();
    const prependText = await prependCommandRow.textContent();
    expect(prependText).toContain('PREPEND');
    expect(prependText?.toLowerCase()).toMatch(/prepend|data|value/i);
  });

  /**
   * Additional test: Commands section has proper heading
   */
  test('Commands section has proper heading', async ({ page }) => {
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    const heading = commandsSection.locator('h2');
    await expect(heading).toBeVisible();
    const headingText = await heading.textContent();
    expect(headingText?.toLowerCase()).toMatch(/command|supported/i);
  });

  /**
   * Additional test: Commands section is navigable from navigation menu
   */
  test('Commands section is navigable from navigation menu', async ({ page }) => {
    // Click on Commands navigation link
    const navLink = page.locator('nav a[href="#commands"]');
    await expect(navLink).toBeVisible();
    await navLink.click();

    // Verify section is scrolled into view
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeInViewport();
  });

  /**
   * Additional test: GETS retrieval command is documented
   */
  test('GETS retrieval command is documented', async ({ page }) => {
    // Navigate to Commands section
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Query for GETS command
    const getsCommandRow = commandsSection.locator('tr, li').filter({ hasText: /GETS/ });
    await expect(getsCommandRow.first()).toBeVisible();

    // Verify GETS command has a description
    const rowText = await getsCommandRow.first().textContent();
    expect(rowText).toContain('GETS');
    expect(rowText?.toLowerCase()).toMatch(/retrieve|get|cas|token/i);
  });

  /**
   * Additional test: MirDB-specific commands section exists
   */
  test('MirDB-specific commands are grouped or identified', async ({ page }) => {
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Check for MirDB-specific section/grouping
    const mirdbSpecificText = commandsSection.locator('h3, .command-group h3').filter({ hasText: /MirDB|specific/i });
    await expect(mirdbSpecificText.first()).toBeVisible();
  });
});
