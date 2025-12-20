// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

/**
 * Configuration Information Display Tests
 *
 * This test suite verifies that default configuration values are displayed
 * for quick reference as specified in REQ-8.
 *
 * Scenario: Configuration Information Display
 * - Verifies default port (12333) is displayed
 * - Verifies other configuration values (SSTable size, memtable size, etc.)
 */

test.describe('Configuration Information Display (REQ-8)', () => {
  test.beforeEach(async ({ page }) => {
    const filePath = path.resolve(__dirname, '../index.html');
    await page.goto(`file://${filePath}`);
  });

  /**
   * Test Case 1: Page displays default port 12333
   * Input: Search page for '12333' port number
   * Expected: Page displays default port 12333
   */
  test('TC1: Page displays default port 12333', async ({ page }) => {
    // Search for the port number 12333 on the page
    const pageContent = await page.locator('body').textContent();

    // Verify the port number 12333 is displayed
    expect(pageContent).toContain('12333');

    // Verify it's displayed in a context that identifies it as a port/configuration
    const hasPortContext =
      pageContent.toLowerCase().includes('port') ||
      pageContent.toLowerCase().includes('addr') ||
      pageContent.toLowerCase().includes('connect');
    expect(hasPortContext).toBeTruthy();

    // Look for a specific element that contains the port number with label
    const quickReference = page.locator('.quick-reference, .reference-grid, [data-testid*="config"]');
    const quickRefCount = await quickReference.count();

    if (quickRefCount > 0) {
      const refContent = await quickReference.first().textContent();
      expect(refContent).toContain('12333');
    }
  });

  /**
   * Test Case 2: Page shows at least one default configuration value
   * Input: Search for configuration-related content
   * Expected: Page shows at least one default configuration value
   */
  test('TC2: Page shows at least one default configuration value', async ({ page }) => {
    const pageContent = await page.locator('body').textContent();

    // Check for configuration-related content (any of the default values from PRD)
    const configValues = {
      port: pageContent.includes('12333'),
      maxLevel: pageContent.includes('7') && pageContent.toLowerCase().includes('level'),
      sstableSize: pageContent.includes('100') && pageContent.toLowerCase().includes('sstable'),
      memtableSize: pageContent.includes('4') && (
        pageContent.toLowerCase().includes('memtable') ||
        pageContent.toLowerCase().includes('mem_table')
      ),
      blockSize: pageContent.includes('4') && (
        pageContent.toLowerCase().includes('block') ||
        pageContent.toLowerCase().includes('block_size')
      ),
      workDir: pageContent.includes('/tmp/mirdb'),
      tomlFormat: pageContent.toLowerCase().includes('toml')
    };

    // At least one configuration value should be present
    const hasAtLeastOneConfigValue = Object.values(configValues).some(v => v === true);
    expect(hasAtLeastOneConfigValue).toBeTruthy();
  });

  /**
   * Additional test: Verify configuration section or reference area exists
   */
  test('Configuration reference section exists with labeled values', async ({ page }) => {
    // Look for a quick reference or configuration section
    const configSection = page.locator('.quick-reference, .configuration, .config-section, .reference-grid');
    const configCount = await configSection.count();

    if (configCount > 0) {
      await expect(configSection.first()).toBeVisible();

      // Verify it contains labeled configuration items
      const referenceItems = configSection.locator('.reference-item, .config-item, dt, dd');
      const itemCount = await referenceItems.count();

      // Should have at least one labeled configuration value
      expect(itemCount).toBeGreaterThanOrEqual(1);
    } else {
      // Check if configuration is shown in getting started section
      const gettingStarted = page.locator('[data-testid="getting-started-section"]');
      await expect(gettingStarted).toBeVisible();

      const content = await gettingStarted.textContent();
      // Should have configuration values mentioned
      expect(content).toContain('12333');
    }
  });

  /**
   * Additional test: Verify multiple configuration parameters are displayed
   * As per REQ-8 and US-5, technical decision makers need configuration details
   */
  test('Multiple configuration parameters are displayed for technical reference', async ({ page }) => {
    const pageContent = await page.locator('body').textContent();

    // Count how many key configuration values are displayed
    let configCount = 0;

    // Port
    if (pageContent.includes('12333')) configCount++;

    // SSTable/Storage related
    if (pageContent.toLowerCase().includes('sstable') ||
        pageContent.toLowerCase().includes('sst')) configCount++;

    // Memtable related
    if (pageContent.toLowerCase().includes('memtable') ||
        pageContent.toLowerCase().includes('mem_table')) configCount++;

    // Block size related
    if (pageContent.toLowerCase().includes('block')) configCount++;

    // Work directory
    if (pageContent.includes('/tmp/mirdb')) configCount++;

    // Config format (TOML)
    if (pageContent.toLowerCase().includes('toml')) configCount++;

    // LSM levels
    if (pageContent.toLowerCase().includes('level')) configCount++;

    // At least 2 configuration parameters should be shown for technical decision makers
    expect(configCount).toBeGreaterThanOrEqual(2);
  });

  /**
   * Additional test: Verify configuration values are shown in code examples
   * Configuration examples help developers understand how to set up MirDB
   */
  test('Configuration example code block shows default values', async ({ page }) => {
    // Look for code blocks that contain configuration
    const codeBlocks = page.locator('pre, code');
    const codeCount = await codeBlocks.count();
    expect(codeCount).toBeGreaterThan(0);

    // Check if any code block contains configuration-like content
    let hasConfigInCode = false;

    for (let i = 0; i < codeCount; i++) {
      const blockContent = await codeBlocks.nth(i).textContent();
      if (blockContent.includes('addr') ||
          blockContent.includes('12333') ||
          blockContent.includes('work_dir') ||
          blockContent.includes('max_level') ||
          blockContent.toLowerCase().includes('toml') ||
          blockContent.toLowerCase().includes('config')) {
        hasConfigInCode = true;
        break;
      }
    }

    expect(hasConfigInCode).toBeTruthy();
  });
});
