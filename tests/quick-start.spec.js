// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = 'file://' + path.resolve(__dirname, '../index.html');

/**
 * Quick Start Installation Instructions Tests
 * Scenario: Verify that installation and quick-start instructions are clearly displayed (REQ-4, US-3)
 */

test.describe('Quick Start Installation Instructions', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(indexPath);
  });

  /**
   * Test Case 1: Check for quick start section existence
   * Input: Check for quick start section existence
   * Expected: Section with installation instructions is present
   */
  test('should have a quick start section with installation instructions', async ({ page }) => {
    // Verify the quick start section exists
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    // Verify the section has a heading
    const heading = quickStartSection.locator('h2');
    await expect(heading).toContainText('Quick Start');

    // Verify installation instructions are present
    const installationSection = quickStartSection.locator('.installation');
    await expect(installationSection).toBeVisible();

    // Verify there are code blocks with installation commands
    const codeBlocks = quickStartSection.locator('pre code');
    await expect(codeBlocks.first()).toBeVisible();

    // Verify installation commands include git clone, cd, and cargo build
    const installationCode = installationSection.locator('pre code');
    const installText = await installationCode.textContent();
    expect(installText).toContain('git clone');
    expect(installText).toContain('cargo build');
  });

  /**
   * Test Case 2: Verify default listen address documentation
   * Input: Verify default listen address documentation
   * Expected: Text mentions '0.0.0.0:12333' as default listen address
   */
  test('should document default listen address as 0.0.0.0:12333', async ({ page }) => {
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    // Check the configuration info section
    const configInfo = quickStartSection.locator('.config-info');
    await expect(configInfo).toBeVisible();

    // Verify the listen address is documented
    const configText = await configInfo.textContent();
    expect(configText).toContain('0.0.0.0:12333');

    // Verify it's labeled as listen address
    expect(configText.toLowerCase()).toContain('listen address');
  });

  /**
   * Test Case 3: Verify default work directory documentation
   * Input: Verify default work directory documentation
   * Expected: Text mentions '/tmp/mirdb' as default work directory
   */
  test('should document default work directory as /tmp/mirdb', async ({ page }) => {
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    // Check the configuration info section
    const configInfo = quickStartSection.locator('.config-info');
    await expect(configInfo).toBeVisible();

    // Verify the work directory is documented
    const configText = await configInfo.textContent();
    expect(configText).toContain('/tmp/mirdb');

    // Verify it's labeled as work directory
    expect(configText.toLowerCase()).toContain('work directory');
  });

  /**
   * Test Case 4: Verify memtable max size documentation
   * Input: Verify memtable max size documentation
   * Expected: Text mentions '4MB' as memtable max size
   */
  test('should document memtable max size as 4MB', async ({ page }) => {
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    // Check the configuration info section
    const configInfo = quickStartSection.locator('.config-info');
    await expect(configInfo).toBeVisible();

    // Verify the memtable max size is documented
    const configText = await configInfo.textContent();
    expect(configText).toContain('4MB');

    // Verify it's labeled as memtable related
    expect(configText.toLowerCase()).toContain('memtable');
  });

  /**
   * Test Case 5: Verify SSTable max size documentation
   * Input: Verify SSTable max size documentation
   * Expected: Text mentions '100MB' as SSTable max size
   */
  test('should document SSTable max size as 100MB', async ({ page }) => {
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    // Check the configuration info section
    const configInfo = quickStartSection.locator('.config-info');
    await expect(configInfo).toBeVisible();

    // Verify the SSTable max size is documented
    const configText = await configInfo.textContent();
    expect(configText).toContain('100MB');

    // Verify it's labeled as SSTable related
    expect(configText.toLowerCase()).toContain('sstable');
  });

  /**
   * Additional test: Verify all configuration items are displayed in a structured list
   */
  test('should display configuration items in a structured list', async ({ page }) => {
    const quickStartSection = page.locator('#quick-start');
    const configInfo = quickStartSection.locator('.config-info');

    // Verify there's a list of configuration items
    const configList = configInfo.locator('ul');
    await expect(configList).toBeVisible();

    // Verify there are 4 configuration items
    const listItems = configInfo.locator('li');
    await expect(listItems).toHaveCount(4);

    // Verify each item contains the expected configuration
    const items = await listItems.allTextContents();

    const hasListenAddress = items.some(item => item.includes('0.0.0.0:12333'));
    const hasWorkDir = items.some(item => item.includes('/tmp/mirdb'));
    const hasMemtableSize = items.some(item => item.includes('4MB'));
    const hasSStableSize = items.some(item => item.includes('100MB'));

    expect(hasListenAddress).toBe(true);
    expect(hasWorkDir).toBe(true);
    expect(hasMemtableSize).toBe(true);
    expect(hasSStableSize).toBe(true);
  });

  /**
   * Additional test: Verify usage instructions are present
   */
  test('should have usage instructions with code examples', async ({ page }) => {
    const quickStartSection = page.locator('#quick-start');
    const usageSection = quickStartSection.locator('.usage');

    await expect(usageSection).toBeVisible();

    // Verify usage heading
    const usageHeading = usageSection.locator('h3');
    await expect(usageHeading).toContainText('Usage');

    // Verify usage code block
    const usageCode = usageSection.locator('pre code');
    const usageText = await usageCode.textContent();

    // Verify it contains basic usage commands
    expect(usageText).toContain('mirdb-server');
    expect(usageText).toContain('telnet');
    expect(usageText).toContain('set');
    expect(usageText).toContain('get');
  });
});
