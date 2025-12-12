import { test, expect } from '@playwright/test';

/**
 * E2E Tests for Configuration Options Documentation
 * Scenario: Verify that configuration options and parameters are documented with clear explanations
 */

test.describe('Configuration Options Documentation', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to homepage before each test
    await page.goto('/');
  });

  /**
   * Test Case 1: Check for configuration section
   * Input: Check for configuration section
   * Expected: Configuration documentation section is present
   */
  test('should display configuration documentation section', async ({ page }) => {
    // Verify the configuration section exists
    const configSection = page.locator('[data-testid="configuration-section"]');
    await expect(configSection).toBeVisible();

    // Verify the section has a clear heading
    const heading = page.locator('[data-testid="configuration-heading"]');
    await expect(heading).toBeVisible();

    // Verify heading text mentions "Configuration"
    const headingText = await heading.textContent();
    expect(headingText?.toLowerCase()).toContain('configuration');
  });

  /**
   * Test Case 2: Verify listen address configuration
   * Input: Verify listen address configuration
   * Expected: Listen address option is documented with default value (0.0.0.0:12333)
   */
  test('should display listen address option with default value 0.0.0.0:12333', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('[data-testid="configuration-section"]');
    await expect(configSection).toBeVisible();

    // Verify the network configuration section exists
    const networkConfig = page.locator('[data-testid="config-network"]');
    await expect(networkConfig).toBeVisible();

    // Verify the addr parameter row exists
    const addrParam = page.locator('[data-testid="config-param-addr"]');
    await expect(addrParam).toBeVisible();

    // Verify the parameter contains the expected content
    const addrText = await addrParam.textContent();
    expect(addrText).toBeTruthy();
    expect(addrText).toContain('addr');
    expect(addrText).toContain('0.0.0.0:12333');

    // Also check the example code block contains the default value
    const codeBlock = page.locator('[data-testid="config-example-code-block"]');
    const codeContent = await codeBlock.textContent();
    expect(codeContent).toContain('0.0.0.0:12333');
  });

  /**
   * Test Case 3: Verify work directory configuration
   * Input: Verify work directory configuration
   * Expected: Work directory option is documented with default value
   */
  test('should display work directory option with default value', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('[data-testid="configuration-section"]');
    await expect(configSection).toBeVisible();

    // Verify the storage configuration section exists
    const storageConfig = page.locator('[data-testid="config-storage"]');
    await expect(storageConfig).toBeVisible();

    // Verify the work_dir parameter row exists
    const workDirParam = page.locator('[data-testid="config-param-work-dir"]');
    await expect(workDirParam).toBeVisible();

    // Verify the parameter contains the expected content
    const workDirText = await workDirParam.textContent();
    expect(workDirText).toBeTruthy();
    expect(workDirText).toContain('work_dir');
    expect(workDirText).toContain('/tmp/mirdb');

    // Also check the example code block contains work_dir
    const codeBlock = page.locator('[data-testid="config-example-code-block"]');
    const codeContent = await codeBlock.textContent();
    expect(codeContent).toContain('work_dir');
  });

  /**
   * Test Case 4: Verify size limits configuration
   * Input: Verify size limits configuration
   * Expected: SSTable size and memtable size options are documented
   */
  test('should display SSTable size and memtable size options', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('[data-testid="configuration-section"]');
    await expect(configSection).toBeVisible();

    // Verify the size limits configuration section exists
    const sizeLimitsConfig = page.locator('[data-testid="config-size-limits"]');
    await expect(sizeLimitsConfig).toBeVisible();

    // Verify the sst_max_size parameter row exists
    const sstSizeParam = page.locator('[data-testid="config-param-sst-max-size"]');
    await expect(sstSizeParam).toBeVisible();

    // Verify SSTable size content
    const sstText = await sstSizeParam.textContent();
    expect(sstText).toBeTruthy();
    expect(sstText).toContain('sst_max_size');
    expect(sstText).toContain('100M');

    // Verify the mem_table_max_size parameter row exists
    const memTableParam = page.locator('[data-testid="config-param-mem-table-max-size"]');
    await expect(memTableParam).toBeVisible();

    // Verify memtable size content
    const memTableText = await memTableParam.textContent();
    expect(memTableText).toBeTruthy();
    expect(memTableText).toContain('mem_table_max_size');
    expect(memTableText).toContain('4M');

    // Verify block_size is also documented
    const blockSizeParam = page.locator('[data-testid="config-param-block-size"]');
    await expect(blockSizeParam).toBeVisible();

    const blockSizeText = await blockSizeParam.textContent();
    expect(blockSizeText).toBeTruthy();
    expect(blockSizeText).toContain('block_size');
    expect(blockSizeText).toContain('4K');
  });
});
