// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Default Configuration Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Default listen address shows 0.0.0.0:12333', async ({ page }) => {
    // Navigate to Configuration section
    const configSection = page.locator('#configuration, [data-testid="configuration-section"]');
    await expect(configSection).toBeVisible();

    // Find the listen address configuration item
    const listenAddressItem = page.locator('[data-testid="config-listen-address"]');
    await expect(listenAddressItem).toBeVisible();

    // Verify the value shows '0.0.0.0:12333'
    const listenAddressValue = page.locator('[data-testid="config-value-listen-address"]');
    await expect(listenAddressValue).toHaveText('0.0.0.0:12333');
  });

  test('TC2: Default memtable max size shows 4MB', async ({ page }) => {
    // Navigate to Configuration section
    const configSection = page.locator('#configuration, [data-testid="configuration-section"]');
    await expect(configSection).toBeVisible();

    // Find the memtable size configuration item
    const memtableSizeItem = page.locator('[data-testid="config-memtable-size"]');
    await expect(memtableSizeItem).toBeVisible();

    // Verify the value shows '4MB'
    const memtableSizeValue = page.locator('[data-testid="config-value-memtable-size"]');
    await expect(memtableSizeValue).toHaveText('4MB');
  });

  test('TC3: Default SSTable max size shows 100MB', async ({ page }) => {
    // Navigate to Configuration section
    const configSection = page.locator('#configuration, [data-testid="configuration-section"]');
    await expect(configSection).toBeVisible();

    // Find the SSTable size configuration item
    const sstableSizeItem = page.locator('[data-testid="config-sstable-size"]');
    await expect(sstableSizeItem).toBeVisible();

    // Verify the value shows '100MB'
    const sstableSizeValue = page.locator('[data-testid="config-value-sstable-size"]');
    await expect(sstableSizeValue).toHaveText('100MB');
  });

  test('TC4: Default work directory shows /tmp/mirdb', async ({ page }) => {
    // Navigate to Configuration section
    const configSection = page.locator('#configuration, [data-testid="configuration-section"]');
    await expect(configSection).toBeVisible();

    // Find the work directory configuration item
    const workDirItem = page.locator('[data-testid="config-work-directory"]');
    await expect(workDirItem).toBeVisible();

    // Verify the value shows '/tmp/mirdb'
    const workDirValue = page.locator('[data-testid="config-value-work-directory"]');
    await expect(workDirValue).toHaveText('/tmp/mirdb');
  });
});
