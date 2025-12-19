// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

// Base URL for the static HTML file
const BASE_URL = 'file://' + path.resolve(__dirname, '../dist/index.html');

test.describe('Configuration Reference Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(BASE_URL);
  });

  test('Test Case 1: Configuration parameters table with key parameters (addr, max_level, work_dir) is visible', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('[data-testid="configuration-section"]');
    await expect(configSection).toBeVisible();

    // Check for configuration tables
    const networkTable = page.locator('[data-testid="config-table-network"]');
    await expect(networkTable).toBeVisible();

    const storageTable = page.locator('[data-testid="config-table-storage"]');
    await expect(storageTable).toBeVisible();

    // Verify key configuration parameters are present
    const addrParam = page.locator('[data-testid="config-param-addr"]');
    await expect(addrParam).toBeVisible();
    const addrContent = await addrParam.textContent();
    expect(addrContent).toContain('addr');

    const maxLevelParam = page.locator('[data-testid="config-param-max-level"]');
    await expect(maxLevelParam).toBeVisible();
    const maxLevelContent = await maxLevelParam.textContent();
    expect(maxLevelContent).toContain('max_level');

    const workDirParam = page.locator('[data-testid="config-param-work-dir"]');
    await expect(workDirParam).toBeVisible();
    const workDirContent = await workDirParam.textContent();
    expect(workDirContent).toContain('work_dir');
  });

  test('Test Case 2: Default values are shown for each parameter (e.g., addr: 0.0.0.0:12333)', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('[data-testid="configuration-section"]');
    await expect(configSection).toBeVisible();

    // Check addr default value
    const addrParam = page.locator('[data-testid="config-param-addr"]');
    const addrContent = await addrParam.textContent();
    expect(addrContent).toContain('0.0.0.0:12333');

    // Check work_dir default value
    const workDirParam = page.locator('[data-testid="config-param-work-dir"]');
    const workDirContent = await workDirParam.textContent();
    expect(workDirContent).toContain('/tmp/mirdb');

    // Check max_level default value
    const maxLevelParam = page.locator('[data-testid="config-param-max-level"]');
    const maxLevelContent = await maxLevelParam.textContent();
    expect(maxLevelContent).toContain('7');

    // Check sst_max_size default value
    const sstMaxSizeParam = page.locator('[data-testid="config-param-sst-max-size"]');
    const sstMaxSizeContent = await sstMaxSizeParam.textContent();
    expect(sstMaxSizeContent).toContain('100M');

    // Check mem_table_max_size default value
    const memTableMaxSizeParam = page.locator('[data-testid="config-param-mem-table-max-size"]');
    const memTableMaxSizeContent = await memTableMaxSizeParam.textContent();
    expect(memTableMaxSizeContent).toContain('4M');
  });

  test('Test Case 3: Link to full configuration documentation is available', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('[data-testid="configuration-section"]');
    await expect(configSection).toBeVisible();

    // Check for documentation link container
    const docsLinkContainer = page.locator('[data-testid="config-docs-link-container"]');
    await expect(docsLinkContainer).toBeVisible();

    // Check for the actual documentation link
    const docsLink = page.locator('[data-testid="config-docs-link"]');
    await expect(docsLink).toBeVisible();

    // Verify link has correct href attribute pointing to GitHub configuration docs
    const href = await docsLink.getAttribute('href');
    expect(href).toContain('github.com/pjzhong/mirdb');
    expect(href).toContain('configuration');

    // Verify link opens in new tab
    const target = await docsLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify link text
    const linkText = await docsLink.textContent();
    expect(linkText).toContain('View Full Documentation');
  });
});
