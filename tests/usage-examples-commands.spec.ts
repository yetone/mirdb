import { test, expect } from '@playwright/test';

test.describe('Usage Examples with Commands', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('page contains code example showing SET command with proper format', async ({ page }) => {
    // Test Case 1: Search for SET command in code examples
    // Expected: Page contains code example showing 'set' command with proper format

    const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
    await expect(gettingStartedSection).toBeVisible();

    // Find code blocks within the getting started section
    const codeContent = await gettingStartedSection.locator('pre').allTextContents();
    const allCodeText = codeContent.join(' ').toLowerCase();

    // Verify the SET command is present with proper memcached format
    // Memcached SET format: set <key> <flags> <exptime> <bytes>
    expect(allCodeText).toContain('set');
    expect(allCodeText).toMatch(/set\s+\w+\s+\d+\s+\d+\s+\d+/);
  });

  test('page contains code example showing GET command usage', async ({ page }) => {
    // Test Case 2: Search for GET command in code examples
    // Expected: Page contains code example showing 'get' command usage

    const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
    await expect(gettingStartedSection).toBeVisible();

    // Find code blocks within the getting started section
    const codeContent = await gettingStartedSection.locator('pre').allTextContents();
    const allCodeText = codeContent.join(' ').toLowerCase();

    // Verify the GET command is present
    expect(allCodeText).toContain('get');
    expect(allCodeText).toMatch(/get\s+\w+/);
  });

  test('examples show expected responses like STORED, VALUE, END', async ({ page }) => {
    // Test Case 3: Search for response codes in examples
    // Expected: Examples show expected responses like STORED, VALUE, END

    const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
    await expect(gettingStartedSection).toBeVisible();

    // Find code blocks within the getting started section
    const codeContent = await gettingStartedSection.locator('pre').allTextContents();
    const allCodeText = codeContent.join(' ');

    // Verify response codes are present
    expect(allCodeText).toContain('STORED');
    expect(allCodeText).toContain('VALUE');
    expect(allCodeText).toContain('END');
  });

  test('page shows example of connecting via telnet to localhost:12333', async ({ page }) => {
    // Test Case 4: Verify telnet connection example
    // Expected: Page shows example of connecting via telnet to localhost:12333

    const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
    await expect(gettingStartedSection).toBeVisible();

    // Find code blocks within the getting started section
    const codeContent = await gettingStartedSection.locator('pre').allTextContents();
    const allCodeText = codeContent.join(' ').toLowerCase();

    // Verify telnet connection example is present with correct port
    expect(allCodeText).toContain('telnet');
    expect(allCodeText).toContain('localhost');
    expect(allCodeText).toContain('12333');
  });

  test('page contains code example showing DELETE command', async ({ page }) => {
    // Additional test for DELETE command (REQ-4 specifies SET, GET, DELETE)

    const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
    await expect(gettingStartedSection).toBeVisible();

    // Find code blocks within the getting started section
    const codeContent = await gettingStartedSection.locator('pre').allTextContents();
    const allCodeText = codeContent.join(' ').toLowerCase();

    // Verify the DELETE command is present
    expect(allCodeText).toContain('delete');
    expect(allCodeText).toMatch(/delete\s+\w+/);
  });
});
