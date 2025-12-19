// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = 'file://' + path.resolve(__dirname, '../index.html');

test.describe('Supported Commands Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(indexPath);
  });

  // Test Case 1: Query for commands section in DOM
  test('TC1: commands section exists with proper id or aria-label', async ({ page }) => {
    // Check for section with id 'commands' or appropriate aria-label
    const commandsSection = page.locator('section#commands, [aria-label*="commands" i], #supported-commands');
    await expect(commandsSection).toBeVisible();
  });

  // Test Case 2: Search for SET command documentation
  test('TC2: SET command is listed with syntax example', async ({ page }) => {
    const commandsSection = page.locator('#commands, #supported-commands, [aria-label*="commands" i]');
    const textContent = await commandsSection.textContent();
    const upperText = textContent.toUpperCase();

    // Check for SET command
    expect(upperText).toContain('SET');

    // Check for syntax example (should contain set syntax pattern)
    const lowerText = textContent.toLowerCase();
    expect(lowerText.includes('set') && (lowerText.includes('<key>') || lowerText.includes('key'))).toBeTruthy();
  });

  // Test Case 3: Search for GET command documentation
  test('TC3: GET command is listed with syntax example', async ({ page }) => {
    const commandsSection = page.locator('#commands, #supported-commands, [aria-label*="commands" i]');
    const textContent = await commandsSection.textContent();
    const upperText = textContent.toUpperCase();

    // Check for GET command
    expect(upperText).toContain('GET');

    // Check for syntax example
    const lowerText = textContent.toLowerCase();
    expect(lowerText.includes('get') && (lowerText.includes('<key') || lowerText.includes('key'))).toBeTruthy();
  });

  // Test Case 4: Search for DELETE command documentation
  test('TC4: DELETE command is listed', async ({ page }) => {
    const commandsSection = page.locator('#commands, #supported-commands, [aria-label*="commands" i]');
    const textContent = await commandsSection.textContent();
    const upperText = textContent.toUpperCase();

    // Check for DELETE command
    expect(upperText).toContain('DELETE');
  });

  // Test Case 5: Search for ADD, REPLACE, APPEND, PREPEND commands
  test('TC5: All storage modifier commands are documented', async ({ page }) => {
    const commandsSection = page.locator('#commands, #supported-commands, [aria-label*="commands" i]');
    const textContent = await commandsSection.textContent();
    const upperText = textContent.toUpperCase();

    // Check for all storage modifier commands
    expect(upperText).toContain('ADD');
    expect(upperText).toContain('REPLACE');
    expect(upperText).toContain('APPEND');
    expect(upperText).toContain('PREPEND');
  });

  // Test Case 6: Search for INFO command (MirDB-specific)
  test('TC6: INFO command is documented as MirDB extension', async ({ page }) => {
    const commandsSection = page.locator('#commands, #supported-commands, [aria-label*="commands" i]');
    const textContent = await commandsSection.textContent();
    const upperText = textContent.toUpperCase();

    // Check for INFO command
    expect(upperText).toContain('INFO');

    // Check it's marked as MirDB-specific
    const lowerText = textContent.toLowerCase();
    expect(lowerText.includes('mirdb') || lowerText.includes('specific') || lowerText.includes('extension')).toBeTruthy();
  });

  // Test Case 7: Search for MAJOR_COMPACTION command
  test('TC7: MAJOR_COMPACTION command is documented', async ({ page }) => {
    const commandsSection = page.locator('#commands, #supported-commands, [aria-label*="commands" i]');
    const textContent = await commandsSection.textContent();
    const upperText = textContent.toUpperCase();

    // Check for MAJOR_COMPACTION command
    expect(upperText).toContain('MAJOR_COMPACTION');
  });
});
