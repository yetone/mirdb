// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

// Base URL for the static HTML file
const BASE_URL = 'file://' + path.resolve(__dirname, '../dist/index.html');

test.describe('Quick-Start Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(BASE_URL);
  });

  test('Test Case 1: Installation command is displayed in a code block', async ({ page }) => {
    // Navigate to quick-start section
    const quickStartSection = page.locator('[data-testid="quickstart-section"]');
    await expect(quickStartSection).toBeVisible();

    // Check for installation code block with cargo build command
    const installationCodeBlock = page.locator('[data-testid="installation-code"]');
    await expect(installationCodeBlock).toBeVisible();

    // Verify cargo build command is present
    const codeContent = await installationCodeBlock.textContent();
    expect(codeContent).toContain('cargo build');
  });

  test('Test Case 2: Code blocks have one-click copy buttons that work correctly', async ({ page }) => {
    // Check that code blocks have copy buttons
    const copyButtons = page.locator('[data-testid^="copy-button"]');

    // Verify at least one copy button exists
    await expect(copyButtons.first()).toBeVisible();

    // Get count of copy buttons - should be multiple (installation, config, usage examples)
    const count = await copyButtons.count();
    expect(count).toBeGreaterThanOrEqual(1);

    // Check that each code block has an associated copy button
    const codeBlocks = page.locator('.code-block');
    const codeBlockCount = await codeBlocks.count();

    // Each code block should have a copy button
    for (let i = 0; i < codeBlockCount; i++) {
      const codeBlock = codeBlocks.nth(i);
      const copyButton = codeBlock.locator('[data-testid^="copy-button"]');
      await expect(copyButton).toBeVisible();
    }
  });

  test('Test Case 3: SET command example with realistic key-value pair is displayed', async ({ page }) => {
    // Check for SET command example
    const setCommandExample = page.locator('[data-testid="example-set"]');
    await expect(setCommandExample).toBeVisible();

    // Verify SET command syntax is present with realistic data
    const setContent = await setCommandExample.textContent();
    expect(setContent).toMatch(/set\s+\w+/i); // SET command with a key
    expect(setContent.length).toBeGreaterThan(10); // Has meaningful content
  });

  test('Test Case 4: GET command example is displayed showing retrieval syntax', async ({ page }) => {
    // Check for GET command example
    const getCommandExample = page.locator('[data-testid="example-get"]');
    await expect(getCommandExample).toBeVisible();

    // Verify GET command syntax is present
    const getContent = await getCommandExample.textContent();
    expect(getContent).toMatch(/get\s+\w+/i); // GET command with a key
  });

  test('Test Case 5: DELETE command example is displayed showing deletion syntax', async ({ page }) => {
    // Check for DELETE command example
    const deleteCommandExample = page.locator('[data-testid="example-delete"]');
    await expect(deleteCommandExample).toBeVisible();

    // Verify DELETE command syntax is present
    const deleteContent = await deleteCommandExample.textContent();
    expect(deleteContent).toMatch(/delete\s+\w+/i); // DELETE command with a key
  });

  test('Test Case 6: Basic TOML configuration example is provided', async ({ page }) => {
    // Check for configuration example
    const configExample = page.locator('[data-testid="config-example"]');
    await expect(configExample).toBeVisible();

    // Verify TOML configuration is present
    const configContent = await configExample.textContent();

    // Should contain TOML-style configuration keys
    expect(configContent).toContain('addr');
    expect(configContent).toMatch(/=.*["']?[\d.:]+["']?/); // Contains assignment
  });

  test('Test Case 7: Memcached client connection example is present', async ({ page }) => {
    // Check for memcached client connection example
    const clientExample = page.locator('[data-testid="client-connection-example"]');
    await expect(clientExample).toBeVisible();

    // Verify connection example content
    const clientContent = await clientExample.textContent();

    // Should contain connection-related content
    expect(clientContent.length).toBeGreaterThan(20);
    // Should reference memcached client or connection
    const hasConnectionContent =
      clientContent.toLowerCase().includes('memcache') ||
      clientContent.toLowerCase().includes('connect') ||
      clientContent.toLowerCase().includes('client') ||
      clientContent.includes('localhost') ||
      clientContent.includes('12333');
    expect(hasConnectionContent).toBe(true);
  });
});
