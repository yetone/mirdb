/**
 * Demo API Integration Tests
 * Owner: Scenario 3 - Interactive Demo Functionality
 *
 * Test cases:
 * - SET command returns STORED
 * - GET command returns stored value
 * - DELETE command returns DELETED
 * - GET after DELETE returns END
 * - Invalid commands return appropriate errors
 *
 * Note: These tests use the in-memory simulation in demo.js
 * since we're testing the frontend demo functionality.
 */

const { test, expect } = require('@playwright/test');

test.describe('Demo API Integration Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForSelector('#demo');
  });

  /**
   * Helper function to execute a command and get the response
   */
  async function executeCommand(page, command) {
    const inputField = page.locator('[data-testid="demo-input"]');
    const submitBtn = page.locator('[data-testid="demo-submit"]');

    await inputField.fill(command);
    await submitBtn.click();

    // Wait for response
    await page.waitForTimeout(300);

    // Get the last response line
    const output = page.locator('#demo-output');
    const responseLines = await output.locator('.demo__output-line--response').allTextContents();
    return responseLines[responseLines.length - 1] || '';
  }

  test('TC2: SET command returns STORED', async ({ page }) => {
    // Execute SET command following Memcached protocol
    // SET <key> <flags> <exptime> <bytes>\r\n<data>\r\n
    const response = await executeCommand(page, 'SET mykey 0 0 5');

    expect(response).toContain('STORED');
  });

  test('TC3: GET command returns stored value', async ({ page }) => {
    // First SET a value
    await executeCommand(page, 'SET mykey 0 0 5');

    // Then GET the value
    const response = await executeCommand(page, 'GET mykey');

    // Response should be: VALUE mykey 0 5\r\nhello\r\nEND
    expect(response).toContain('VALUE mykey');
    expect(response).toContain('END');
  });

  test('TC4: DELETE command returns DELETED', async ({ page }) => {
    // First SET a value
    await executeCommand(page, 'SET deletetest 0 0 4');

    // Then DELETE it
    const response = await executeCommand(page, 'DELETE deletetest');

    expect(response).toContain('DELETED');
  });

  test('TC5: GET after DELETE returns END', async ({ page }) => {
    // SET a value
    await executeCommand(page, 'SET temptest 0 0 4');

    // DELETE it
    await executeCommand(page, 'DELETE temptest');

    // GET should return END (key not found)
    const response = await executeCommand(page, 'GET temptest');

    expect(response).toBe('END');
  });

  test('Invalid command returns error', async ({ page }) => {
    const inputField = page.locator('[data-testid="demo-input"]');
    const submitBtn = page.locator('[data-testid="demo-submit"]');

    await inputField.fill('INVALID_CMD test');
    await submitBtn.click();
    await page.waitForTimeout(300);

    // Check for error in output
    const output = page.locator('#demo-output');
    const hasError = await output.locator('.demo__output-line--response, .demo__output-line--error')
      .filter({ hasText: /ERROR|Unknown command/i })
      .count();

    expect(hasError).toBeGreaterThan(0);
  });

  test('SET without enough parameters returns error', async ({ page }) => {
    const inputField = page.locator('[data-testid="demo-input"]');
    const submitBtn = page.locator('[data-testid="demo-submit"]');

    // SET with missing parameters
    await inputField.fill('SET onlykey');
    await submitBtn.click();
    await page.waitForTimeout(300);

    const output = page.locator('#demo-output');
    const hasError = await output.locator('.demo__output-line--response, .demo__output-line--error')
      .filter({ hasText: /CLIENT_ERROR|bad command/i })
      .count();

    expect(hasError).toBeGreaterThan(0);
  });

  test('GET without key returns error', async ({ page }) => {
    const inputField = page.locator('[data-testid="demo-input"]');
    const submitBtn = page.locator('[data-testid="demo-submit"]');

    await inputField.fill('GET');
    await submitBtn.click();
    await page.waitForTimeout(300);

    const output = page.locator('#demo-output');
    const hasError = await output.locator('.demo__output-line--response, .demo__output-line--error')
      .filter({ hasText: /CLIENT_ERROR|missing key/i })
      .count();

    expect(hasError).toBeGreaterThan(0);
  });

  test('Multiple SET and GET operations work correctly', async ({ page }) => {
    // SET multiple keys
    await executeCommand(page, 'SET key1 0 0 6');
    await executeCommand(page, 'SET key2 0 0 6');
    await executeCommand(page, 'SET key3 0 0 6');

    // GET each key should return values
    const response1 = await executeCommand(page, 'GET key1');
    const response2 = await executeCommand(page, 'GET key2');
    const response3 = await executeCommand(page, 'GET key3');

    expect(response1).toContain('VALUE key1');
    expect(response2).toContain('VALUE key2');
    expect(response3).toContain('VALUE key3');
  });

  test('DELETE non-existent key returns NOT_FOUND', async ({ page }) => {
    const response = await executeCommand(page, 'DELETE nonexistent_key_xyz');

    expect(response).toContain('NOT_FOUND');
  });

  test('VERSION command returns version info', async ({ page }) => {
    const response = await executeCommand(page, 'VERSION');

    expect(response).toContain('VERSION');
    expect(response).toContain('MirDB');
  });

  test('STATS command returns statistics', async ({ page }) => {
    // Add some data first
    await executeCommand(page, 'SET statskey 0 0 4');

    const response = await executeCommand(page, 'STATS');

    expect(response).toContain('STAT');
    expect(response).toContain('END');
  });
});
