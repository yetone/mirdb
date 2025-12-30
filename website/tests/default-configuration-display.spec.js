// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Default Configuration Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Default listen address shows 0.0.0.0:12333', async ({ page }) => {
    // Locate configuration section
    const configSection = page.locator('section#configuration, [data-testid="configuration-section"], .configuration');
    await configSection.scrollIntoViewIfNeeded();
    await expect(configSection).toBeVisible();

    // Check for listen address value
    const listenAddressItem = configSection.locator('[data-testid="config-listen-address"], .config-item:has-text("Listen Address")');
    await expect(listenAddressItem).toBeVisible();

    // Verify the default value is displayed
    const configText = await listenAddressItem.textContent();
    expect(configText).toContain('0.0.0.0:12333');
  });

  test('TC2: Default memtable max size shows 4MB', async ({ page }) => {
    // Locate configuration section
    const configSection = page.locator('section#configuration, [data-testid="configuration-section"], .configuration');
    await configSection.scrollIntoViewIfNeeded();
    await expect(configSection).toBeVisible();

    // Check for memtable size value
    const memtableItem = configSection.locator('[data-testid="config-memtable-size"], .config-item:has-text("Memtable")');
    await expect(memtableItem).toBeVisible();

    // Verify the default value is displayed
    const configText = await memtableItem.textContent();
    expect(configText).toContain('4MB');
  });

  test('TC3: Default SSTable max size shows 100MB', async ({ page }) => {
    // Locate configuration section
    const configSection = page.locator('section#configuration, [data-testid="configuration-section"], .configuration');
    await configSection.scrollIntoViewIfNeeded();
    await expect(configSection).toBeVisible();

    // Check for SSTable size value
    const sstableItem = configSection.locator('[data-testid="config-sstable-size"], .config-item:has-text("SSTable")');
    await expect(sstableItem).toBeVisible();

    // Verify the default value is displayed
    const configText = await sstableItem.textContent();
    expect(configText).toContain('100MB');
  });

  test('TC4: Default work directory shows /tmp/mirdb', async ({ page }) => {
    // Locate configuration section
    const configSection = page.locator('section#configuration, [data-testid="configuration-section"], .configuration');
    await configSection.scrollIntoViewIfNeeded();
    await expect(configSection).toBeVisible();

    // Check for work directory value
    const workDirItem = configSection.locator('[data-testid="config-work-directory"], .config-item:has-text("Work Directory")');
    await expect(workDirItem).toBeVisible();

    // Verify the default value is displayed
    const configText = await workDirItem.textContent();
    expect(configText).toContain('/tmp/mirdb');
  });
});
