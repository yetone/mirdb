/**
 * Memcached Commands Documentation Tests
 * Owner: Scenario 12 - Memcached Commands Documentation
 *
 * Test cases:
 * - SET command documented with syntax
 * - GET command documented with syntax
 * - DELETE command documented
 * - QUIT command documented
 * - Command table structure
 */
const { test, expect } = require('@playwright/test');

test.describe('Memcached Commands Documentation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('http://localhost:3000/index.html');
  });

  test('should display SET command with syntax: set <key> <flags> <exptime> <bytes>', async ({ page }) => {
    // Navigate to commands section
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Check for SET command documentation
    const sectionText = await commandsSection.textContent();
    const lowerText = sectionText.toLowerCase();

    // Verify SET command is documented
    expect(lowerText).toContain('set');

    // Verify SET command syntax includes key parameters
    const hasSyntax = sectionText.includes('<key>') ||
                      sectionText.includes('key') &&
                      (sectionText.includes('<flags>') || sectionText.includes('flags')) &&
                      (sectionText.includes('<exptime>') || sectionText.includes('exptime') || sectionText.includes('ttl')) &&
                      (sectionText.includes('<bytes>') || sectionText.includes('bytes'));
    expect(hasSyntax).toBeTruthy();

    // Verify SET command description
    expect(lowerText).toMatch(/store|set.*value|key-value/i);
  });

  test('should display GET command with syntax: get <key>', async ({ page }) => {
    // Navigate to commands section
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Check for GET command documentation
    const sectionText = await commandsSection.textContent();
    const lowerText = sectionText.toLowerCase();

    // Verify GET command is documented
    expect(lowerText).toContain('get');

    // Verify GET command syntax includes key parameter
    const hasGetSyntax = sectionText.includes('get') && sectionText.includes('key');
    expect(hasGetSyntax).toBeTruthy();

    // Verify GET command description
    expect(lowerText).toMatch(/retrieve|get.*value|fetch/i);
  });

  test('should display DELETE command with syntax and expected response', async ({ page }) => {
    // Navigate to commands section
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Check for DELETE command documentation
    const sectionText = await commandsSection.textContent();
    const lowerText = sectionText.toLowerCase();

    // Verify DELETE command is documented
    expect(lowerText).toContain('delete');

    // Verify DELETE command description mentions removing keys
    expect(lowerText).toMatch(/remove|delete.*key|erase/i);
  });

  test('should display QUIT command for closing connections', async ({ page }) => {
    // Navigate to commands section
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Check for QUIT command documentation
    const sectionText = await commandsSection.textContent();
    const lowerText = sectionText.toLowerCase();

    // Verify QUIT command is documented
    expect(lowerText).toContain('quit');

    // Verify QUIT command description mentions closing/terminating connections
    expect(lowerText).toMatch(/close|connection|disconnect|terminate|exit/i);
  });

  test('should display commands in organized table with Command and Description columns', async ({ page }) => {
    // Navigate to commands section
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Verify there's a table in the commands section
    const commandsTable = commandsSection.locator('table.commands__table');
    await expect(commandsTable).toBeVisible();

    // Verify table has header row with Command and Description columns
    const headerRow = commandsTable.locator('thead tr');
    await expect(headerRow).toBeVisible();

    const headers = commandsTable.locator('th');
    const headerCount = await headers.count();
    expect(headerCount).toBeGreaterThanOrEqual(2);

    // Get header text
    const headerTexts = [];
    for (let i = 0; i < headerCount; i++) {
      const text = await headers.nth(i).textContent();
      headerTexts.push(text.toLowerCase());
    }

    // Verify Command column exists
    const hasCommandColumn = headerTexts.some(h => h.includes('command'));
    expect(hasCommandColumn).toBeTruthy();

    // Verify Description column exists
    const hasDescriptionColumn = headerTexts.some(h => h.includes('description'));
    expect(hasDescriptionColumn).toBeTruthy();

    // Verify table has body with command rows
    const bodyRows = commandsTable.locator('tbody tr');
    const rowCount = await bodyRows.count();
    expect(rowCount).toBeGreaterThanOrEqual(4); // At least SET, GET, DELETE, QUIT

    // Verify each row has cells
    for (let i = 0; i < Math.min(rowCount, 4); i++) {
      const row = bodyRows.nth(i);
      const cells = row.locator('td');
      const cellCount = await cells.count();
      expect(cellCount).toBeGreaterThanOrEqual(2);
    }
  });
});
