// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Default Configuration Display (REQ-6)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('should display default listen address 0.0.0.0:12333', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('[data-testid="default-config"]');
    await configSection.scrollIntoViewIfNeeded();

    // Check that listen address configuration is displayed
    const listenAddressItem = page.locator('[data-config="listen-address"]');
    await expect(listenAddressItem).toBeVisible();
    await expect(listenAddressItem).toContainText('0.0.0.0:12333');
  });

  test('should display max LSM levels 7', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('[data-testid="default-config"]');
    await configSection.scrollIntoViewIfNeeded();

    // Check that LSM levels configuration is displayed
    const lsmLevelsItem = page.locator('[data-config="lsm-levels"]');
    await expect(lsmLevelsItem).toBeVisible();
    await expect(lsmLevelsItem).toContainText('7');
  });

  test('should display default work directory /tmp/mirdb', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('[data-testid="default-config"]');
    await configSection.scrollIntoViewIfNeeded();

    // Check that work directory configuration is displayed
    const workDirItem = page.locator('[data-config="work-directory"]');
    await expect(workDirItem).toBeVisible();
    await expect(workDirItem).toContainText('/tmp/mirdb');
  });

  test('should display SSTable max size 100MB', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('[data-testid="default-config"]');
    await configSection.scrollIntoViewIfNeeded();

    // Check that SSTable size configuration is displayed
    const sstableSizeItem = page.locator('[data-config="sstable-size"]');
    await expect(sstableSizeItem).toBeVisible();
    await expect(sstableSizeItem).toContainText('100MB');
  });

  test('should display memtable max size 4MB', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('[data-testid="default-config"]');
    await configSection.scrollIntoViewIfNeeded();

    // Check that memtable size configuration is displayed
    const memtableSizeItem = page.locator('[data-config="memtable-size"]');
    await expect(memtableSizeItem).toBeVisible();
    await expect(memtableSizeItem).toContainText('4MB');
  });
});
