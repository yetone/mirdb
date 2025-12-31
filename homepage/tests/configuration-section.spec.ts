import { test, expect } from '@playwright/test';

test.describe('Configuration Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Check for Listen Address parameter', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Check for Listen Address parameter in the configuration table
    const configTable = configSection.locator('table');
    await expect(configTable).toBeVisible();

    // Verify Listen Address row exists with correct default value
    const listenAddressRow = configTable.locator('tr').filter({ hasText: /Listen Address/i });
    await expect(listenAddressRow).toBeVisible();
    await expect(listenAddressRow).toContainText('0.0.0.0:12333');
  });

  test('TC2: Check for Max LSM Levels parameter', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Check for Max LSM Levels parameter in the configuration table
    const configTable = configSection.locator('table');
    await expect(configTable).toBeVisible();

    // Verify Max LSM Levels row exists with correct default value
    const maxLsmLevelsRow = configTable.locator('tr').filter({ hasText: /Max LSM Levels/i });
    await expect(maxLsmLevelsRow).toBeVisible();
    await expect(maxLsmLevelsRow).toContainText('7');
  });

  test('TC3: Check for Work Directory parameter', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Check for Work Directory parameter in the configuration table
    const configTable = configSection.locator('table');
    await expect(configTable).toBeVisible();

    // Verify Work Directory row exists with correct default value
    const workDirRow = configTable.locator('tr').filter({ hasText: /Work Directory/i });
    await expect(workDirRow).toBeVisible();
    await expect(workDirRow).toContainText('/tmp/mirdb');
  });

  test('TC4: Check for SSTable Max Size parameter', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Check for SSTable Max Size parameter in the configuration table
    const configTable = configSection.locator('table');
    await expect(configTable).toBeVisible();

    // Verify SSTable Max Size row exists with correct default value
    const sstableRow = configTable.locator('tr').filter({ hasText: /SSTable Max Size/i });
    await expect(sstableRow).toBeVisible();
    await expect(sstableRow).toContainText('100MB');
  });

  test('TC5: Check for Memtable Max Size parameter', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Check for Memtable Max Size parameter in the configuration table
    const configTable = configSection.locator('table');
    await expect(configTable).toBeVisible();

    // Verify Memtable Max Size row exists with correct default value
    const memtableRow = configTable.locator('tr').filter({ hasText: /Memtable Max Size/i });
    await expect(memtableRow).toBeVisible();
    await expect(memtableRow).toContainText('4MB');
  });

  test('TC6: Check for Block Size parameter', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Check for Block Size parameter in the configuration table
    const configTable = configSection.locator('table');
    await expect(configTable).toBeVisible();

    // Verify Block Size row exists with correct default value
    const blockSizeRow = configTable.locator('tr').filter({ hasText: /Block Size/i });
    await expect(blockSizeRow).toBeVisible();
    await expect(blockSizeRow).toContainText('4KB');
  });

  test('TC7: Verify link to full configuration docs', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Check for link to full configuration documentation
    const configDocsLink = configSection.locator('a').filter({ hasText: /documentation|full.*config|config.*docs|learn more/i });
    await expect(configDocsLink).toBeVisible();

    // Verify the link has a valid href attribute
    const href = await configDocsLink.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href?.length).toBeGreaterThan(0);
  });
});
