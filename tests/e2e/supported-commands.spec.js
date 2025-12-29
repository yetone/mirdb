// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = 'file://' + path.resolve(__dirname, '../../index.html');

/**
 * Supported Commands Reference E2E Tests
 * Tests REQ-7 from PRD: Include supported memcached commands reference
 */
test.describe('Supported Commands Reference', () => {

  test.beforeEach(async ({ page }) => {
    await page.goto(indexPath);
  });

  /**
   * Test Case 1: Check for commands reference section
   * Expected: Page mentions or links to supported memcached commands
   */
  test('TC1: Page mentions or links to supported memcached commands', async ({ page }) => {
    // Find the commands section by id, class, or heading text
    const commandsSection = page.locator('#commands, .commands, section:has(h2:text-matches("commands", "i"))').first();

    // Check that commands section exists and is visible
    await expect(commandsSection).toBeVisible();

    // Verify section has a heading mentioning "commands"
    const heading = commandsSection.locator('h2');
    await expect(heading).toBeVisible();
    const headingText = await heading.textContent();
    expect(headingText?.toLowerCase()).toContain('command');
  });

  /**
   * Test Case 2: Verify storage commands listed
   * Expected: Commands SET, ADD, REPLACE are mentioned or linked
   */
  test('TC2: Storage commands SET, ADD, REPLACE are mentioned', async ({ page }) => {
    // Find the commands section
    const commandsSection = page.locator('#commands, .commands, section:has(h2:text-matches("commands", "i"))').first();
    await expect(commandsSection).toBeVisible();

    // Get all text content from the commands section
    const sectionText = await commandsSection.textContent();

    // Verify SET command is mentioned
    expect(sectionText?.toUpperCase()).toContain('SET');

    // Verify ADD command is mentioned
    expect(sectionText?.toUpperCase()).toContain('ADD');

    // Verify REPLACE command is mentioned
    expect(sectionText?.toUpperCase()).toContain('REPLACE');
  });

  /**
   * Test Case 3: Verify retrieval commands listed
   * Expected: Commands GET, GETS are mentioned or linked
   */
  test('TC3: Retrieval commands GET, GETS are mentioned', async ({ page }) => {
    // Find the commands section
    const commandsSection = page.locator('#commands, .commands, section:has(h2:text-matches("commands", "i"))').first();
    await expect(commandsSection).toBeVisible();

    // Get all text content from the commands section
    const sectionText = await commandsSection.textContent();

    // Verify GET command is mentioned
    expect(sectionText?.toUpperCase()).toContain('GET');

    // Verify GETS command is mentioned
    expect(sectionText?.toUpperCase()).toContain('GETS');
  });

  /**
   * Additional test: Verify DELETE command is listed
   * Expected: DELETE command is mentioned for completeness
   */
  test('TC4: DELETE command is mentioned', async ({ page }) => {
    // Find the commands section
    const commandsSection = page.locator('#commands, .commands, section:has(h2:text-matches("commands", "i"))').first();
    await expect(commandsSection).toBeVisible();

    // Get all text content from the commands section
    const sectionText = await commandsSection.textContent();

    // Verify DELETE command is mentioned
    expect(sectionText?.toUpperCase()).toContain('DELETE');
  });

  /**
   * Additional test: Verify commands are organized in groups
   * Expected: Commands are grouped by category (Storage, Retrieval, etc.)
   */
  test('TC5: Commands are organized in groups with headings', async ({ page }) => {
    // Find the commands section
    const commandsSection = page.locator('#commands, .commands, section:has(h2:text-matches("commands", "i"))').first();
    await expect(commandsSection).toBeVisible();

    // Check for command group containers
    const commandGroups = commandsSection.locator('.command-group, .commands-group, div:has(> h3)');
    const groupCount = await commandGroups.count();

    // Should have at least 2 groups (Storage, Retrieval)
    expect(groupCount).toBeGreaterThanOrEqual(2);

    // Check for group headings
    const groupHeadings = commandsSection.locator('h3');
    const headingCount = await groupHeadings.count();
    expect(headingCount).toBeGreaterThanOrEqual(2);
  });

});
