/**
 * Performance Section E2E Tests
 * Owner: Scenario 4 - Performance and Configuration Section
 *
 * Test coverage:
 * - Performance section presence and structure
 * - Performance metrics/characteristics display
 * - Configuration table with all options
 * - Default values documentation
 */
import { test, expect } from '@playwright/test';

test.describe('Performance and Configuration Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('http://127.0.0.1:1111/');
  });

  test('TC1: Performance section exists with performance metrics', async ({ page }) => {
    // Verify performance section exists
    const performanceSection = page.locator('[data-testid="performance-section"]');
    await expect(performanceSection).toBeVisible();

    // Verify section has Performance & Configuration heading
    const heading = performanceSection.locator('h2');
    await expect(heading).toContainText('Performance');

    // Verify performance metrics subsection exists
    const metricsSection = page.locator('[data-testid="performance-metrics"]');
    await expect(metricsSection).toBeVisible();

    // Verify performance characteristics heading
    const metricsHeading = metricsSection.locator('h3');
    await expect(metricsHeading).toContainText('Performance Characteristics');

    // Verify metrics list contains performance-related content
    const metricsList = metricsSection.locator('.metrics-list');
    await expect(metricsList).toBeVisible();

    // Verify key performance characteristics are mentioned
    const metricsText = await metricsSection.textContent();
    expect(metricsText).toMatch(/write.*throughput/i);
    expect(metricsText).toMatch(/LSM-tree/i);
  });

  test('TC2: Configuration table shows required options', async ({ page }) => {
    // Verify configuration options section exists
    const configSection = page.locator('[data-testid="config-options"]');
    await expect(configSection).toBeVisible();

    // Verify configuration table exists
    const configTable = page.locator('[data-testid="config-table"]');
    await expect(configTable).toBeVisible();

    // Verify table has correct headers
    const headers = configTable.locator('thead th');
    await expect(headers.nth(0)).toContainText('Option');
    await expect(headers.nth(1)).toContainText('Default');
    await expect(headers.nth(2)).toContainText('Description');

    // Verify listen address option exists
    const listenAddressRow = page.locator('[data-testid="config-listen-address"]');
    await expect(listenAddressRow).toBeVisible();
    await expect(listenAddressRow).toContainText('addr');

    // Verify max levels option exists
    const maxLevelRow = page.locator('[data-testid="config-max-level"]');
    await expect(maxLevelRow).toBeVisible();
    await expect(maxLevelRow).toContainText('max_level');

    // Verify work directory option exists
    const workDirRow = page.locator('[data-testid="config-work-dir"]');
    await expect(workDirRow).toBeVisible();
    await expect(workDirRow).toContainText('work_dir');

    // Verify SSTable size option exists
    const sstSizeRow = page.locator('[data-testid="config-sst-size"]');
    await expect(sstSizeRow).toBeVisible();
    await expect(sstSizeRow).toContainText('sst_max_size');
  });

  test('TC3: Configuration options include correct default values', async ({ page }) => {
    // Verify default value for listen address is 0.0.0.0:12333
    const listenAddressRow = page.locator('[data-testid="config-listen-address"]');
    await expect(listenAddressRow).toContainText('0.0.0.0:12333');

    // Verify default value for max levels is 7
    const maxLevelRow = page.locator('[data-testid="config-max-level"]');
    await expect(maxLevelRow).toContainText('7');

    // Verify default value for work directory
    const workDirRow = page.locator('[data-testid="config-work-dir"]');
    await expect(workDirRow).toContainText('/tmp/mirdb');

    // Verify default value for SSTable size is 100MB
    const sstSizeRow = page.locator('[data-testid="config-sst-size"]');
    await expect(sstSizeRow).toContainText('100MB');

    // Verify default value for memtable size is 4MB
    const memtableSizeRow = page.locator('[data-testid="config-memtable-size"]');
    await expect(memtableSizeRow).toBeVisible();
    await expect(memtableSizeRow).toContainText('4MB');

    // Verify default value for block size is 4KB
    const blockSizeRow = page.locator('[data-testid="config-block-size"]');
    await expect(blockSizeRow).toBeVisible();
    await expect(blockSizeRow).toContainText('4KB');
  });
});
