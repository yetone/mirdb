/**
 * E2E Tests for Configuration Options Display
 * Scenario: Verify that configuration options and default values are documented on the homepage
 * Test Cases: 1, 2, 3, 4
 */

const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = 'file://' + path.resolve(__dirname, '../../index.html');

test.describe('Configuration Options Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(indexPath);
  });

  test.describe('Test Case 1: Configuration-related content', () => {
    test('should contain configuration section or options on the page', async ({ page }) => {
      // Search for configuration-related content
      const pageContent = await page.textContent('body');
      const hasConfiguration =
        pageContent.toLowerCase().includes('configuration') ||
        pageContent.toLowerCase().includes('config') ||
        pageContent.toLowerCase().includes('settings') ||
        pageContent.toLowerCase().includes('options');

      expect(hasConfiguration).toBe(true);
    });

    test('should have a dedicated configuration section', async ({ page }) => {
      // Look for configuration section by ID or heading
      const configSection = page.locator('#configuration, #config, section:has-text("Configuration")');
      await expect(configSection.first()).toBeVisible();
    });

    test('should display configuration heading', async ({ page }) => {
      // Check for configuration heading
      const configHeading = page.locator('h2:has-text("Configuration"), h3:has-text("Configuration")');
      await expect(configHeading.first()).toBeVisible();
    });
  });

  test.describe('Test Case 2: Default port value', () => {
    test('should display default listen address/port: 0.0.0.0:12333', async ({ page }) => {
      const pageContent = await page.textContent('body');

      // Check for the default listen address
      expect(pageContent).toContain('0.0.0.0:12333');
    });

    test('should display addr configuration parameter', async ({ page }) => {
      const pageContent = await page.textContent('body');

      // Check for addr parameter mention
      const hasAddrParam =
        pageContent.includes('addr') ||
        pageContent.toLowerCase().includes('listen address') ||
        pageContent.toLowerCase().includes('port');

      expect(hasAddrParam).toBe(true);
    });
  });

  test.describe('Test Case 3: Key configuration defaults', () => {
    test('should display memtable size default (4MB)', async ({ page }) => {
      const pageContent = await page.textContent('body');

      // Check for memtable size - could be written as 4MB, 4M, 4 MB
      const hasMemtableSize =
        pageContent.includes('4MB') ||
        pageContent.includes('4M') ||
        pageContent.includes('4 MB') ||
        (pageContent.toLowerCase().includes('memtable') && pageContent.includes('4'));

      expect(hasMemtableSize).toBe(true);
    });

    test('should display SSTable max size default (100MB)', async ({ page }) => {
      const pageContent = await page.textContent('body');

      // Check for SSTable max size
      const hasSstMaxSize =
        pageContent.includes('100MB') ||
        pageContent.includes('100M') ||
        pageContent.includes('100 MB') ||
        (pageContent.toLowerCase().includes('sstable') && pageContent.includes('100'));

      expect(hasSstMaxSize).toBe(true);
    });

    test('should display max levels default (7)', async ({ page }) => {
      const pageContent = await page.textContent('body');

      // Check for max levels configuration
      const hasMaxLevels =
        (pageContent.toLowerCase().includes('max') &&
         pageContent.toLowerCase().includes('level') &&
         pageContent.includes('7')) ||
        pageContent.includes('max_level') ||
        pageContent.includes('maxLevel');

      expect(hasMaxLevels).toBe(true);
    });

    test('should display all key defaults in configuration section', async ({ page }) => {
      // Verify the configuration section contains all required defaults
      const configSection = page.locator('#configuration');
      const configText = await configSection.textContent();

      // Check for all key configuration values
      expect(configText).toContain('12333'); // Port
      expect(configText.toLowerCase()).toMatch(/memtable|mem_table/i); // Memtable reference
      expect(configText.toLowerCase()).toMatch(/sstable|sst/i); // SSTable reference
      expect(configText.toLowerCase()).toMatch(/level/i); // Level reference
    });
  });

  test.describe('Test Case 4: Configuration code example', () => {
    test('should display a configuration code example', async ({ page }) => {
      // Look for code blocks in configuration section
      const configCodeBlock = page.locator('#configuration pre, #configuration code, section:has-text("Configuration") pre, section:has-text("Configuration") code');
      await expect(configCodeBlock.first()).toBeVisible();
    });

    test('should show TOML configuration format', async ({ page }) => {
      // Check for TOML-style configuration
      const pageContent = await page.textContent('body');

      // TOML uses = for assignment
      const hasTomlFormat =
        pageContent.includes('addr = ') ||
        pageContent.includes('work_dir = ') ||
        pageContent.includes('max_level = ');

      expect(hasTomlFormat).toBe(true);
    });

    test('should show how to set custom values', async ({ page }) => {
      // The configuration example should demonstrate customization
      const codeBlocks = page.locator('pre, code');
      const codeContent = await codeBlocks.allTextContents();
      const allCode = codeContent.join(' ');

      // Check for configuration parameters that can be customized
      const showsCustomization =
        allCode.includes('addr') ||
        allCode.includes('work_dir') ||
        allCode.includes('sst_max_size') ||
        allCode.includes('mem_table_max_size');

      expect(showsCustomization).toBe(true);
    });

    test('should include configuration file path or usage example', async ({ page }) => {
      const pageContent = await page.textContent('body');

      // Check for configuration file reference or command line usage
      const hasConfigUsage =
        pageContent.includes('.toml') ||
        pageContent.includes('-c ') ||
        pageContent.includes('config') ||
        pageContent.includes('mirdb.toml');

      expect(hasConfigUsage).toBe(true);
    });
  });

  test.describe('Configuration Section Accessibility', () => {
    test('should have proper heading hierarchy for configuration section', async ({ page }) => {
      // Configuration section should have proper heading
      const configHeading = page.locator('#configuration h2, section:has-text("Configuration") h2, #configuration h3, section:has-text("Configuration") h3');
      await expect(configHeading.first()).toBeVisible();
    });

    test('should have readable code blocks with proper formatting', async ({ page }) => {
      // Check that code blocks exist and have styling
      const codeBlock = page.locator('#configuration pre, section:has-text("Configuration") pre');
      await expect(codeBlock.first()).toBeVisible();

      // Verify code block has some minimum content
      const codeText = await codeBlock.first().textContent();
      expect(codeText.length).toBeGreaterThan(10);
    });
  });
});
