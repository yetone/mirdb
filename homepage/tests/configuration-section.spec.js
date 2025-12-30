// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Configuration Options Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Port 12333 is mentioned as default configuration', async ({ page }) => {
    // Test Case 1: Query page content for port number '12333'
    // Expected: Port 12333 is mentioned as default configuration
    const configSection = page.locator('#config, [data-testid="config"], .configuration');
    await expect(configSection).toBeVisible();

    // Check for port 12333 in the configuration section
    const configContent = await configSection.textContent();
    const hasPort12333 = configContent?.includes('12333');

    expect(hasPort12333).toBeTruthy();
  });

  test('TC2: Default storage path /tmp/mirdb is mentioned', async ({ page }) => {
    // Test Case 2: Query page content for storage path '/tmp/mirdb'
    // Expected: Default storage path is mentioned
    const configSection = page.locator('#config, [data-testid="config"], .configuration');
    await expect(configSection).toBeVisible();

    // Check for storage path /tmp/mirdb in the configuration section
    const configContent = await configSection.textContent();
    const hasStoragePath = configContent?.includes('/tmp/mirdb');

    expect(hasStoragePath).toBeTruthy();
  });

  test('TC3: Memtable size 4MB is displayed', async ({ page }) => {
    // Test Case 3: Query page content for memtable size '4MB'
    // Expected: Memtable configuration value is displayed
    const configSection = page.locator('#config, [data-testid="config"], .configuration');
    await expect(configSection).toBeVisible();

    // Check for memtable size 4MB in the configuration section
    const configContent = await configSection.textContent();
    const hasMemtableSize = configContent?.includes('4MB');

    expect(hasMemtableSize).toBeTruthy();
  });

  test('TC4: Configuration options are displayed in organized format (table or list)', async ({ page }) => {
    // Test Case 4: Query for configuration table or list structure
    // Expected: Configuration options are displayed in organized format (table or list)
    const configSection = page.locator('#config, [data-testid="config"], .configuration');
    await expect(configSection).toBeVisible();

    // Check for either a table or list structure for configuration options
    const hasTable = await configSection.locator('table').count() > 0;
    const hasList = await configSection.locator('ul, ol').count() > 0;
    const hasDefinitionList = await configSection.locator('dl').count() > 0;

    // Configuration should be in an organized format (table or list)
    expect(hasTable || hasList || hasDefinitionList).toBeTruthy();
  });
});
