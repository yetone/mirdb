/**
 * Unit Tests for Content Accuracy - Technical Information
 * Scenario: Verify technical information displayed matches actual MirDB specifications
 *
 * These tests verify that the homepage displays accurate technical information
 * about MirDB's configuration defaults and specifications.
 */

const fs = require('fs');
const path = require('path');
const { JSDOM } = require('jsdom');

describe('Content Accuracy - Technical Information', () => {
  let document;
  let configSection;
  let quickStartSection;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../index.html');
    const html = fs.readFileSync(htmlPath, 'utf-8');
    const dom = new JSDOM(html);
    document = dom.window.document;
    configSection = document.querySelector('#config, .config');
    quickStartSection = document.querySelector('#quickstart, .quickstart');
  });

  /**
   * Test Case 1: Check stated default port
   * Expected: Page states default port as 12333
   * Reference: MirDB default configuration - addr = "0.0.0.0:12333"
   */
  test('TC1: Page states default port as 12333', () => {
    // The port should be stated in the configuration section or quick start
    const pageContent = document.body.textContent;

    // Check for port 12333 specifically
    expect(pageContent).toContain('12333');

    // Verify it's mentioned in the config section with the correct address format
    const configContent = configSection.textContent;
    expect(configContent).toContain('0.0.0.0:12333');

    // Also verify in quick start section where connection instructions are
    const quickStartContent = quickStartSection.textContent;
    expect(quickStartContent).toContain('12333');
  });

  /**
   * Test Case 2: Check stated max LSM levels
   * Expected: Page states max LSM levels as 7
   * Reference: MirDB default configuration - max_level = 7
   */
  test('TC2: Page states max LSM levels as 7', () => {
    const configContent = configSection.textContent;

    // Check for LSM levels configuration
    expect(configContent.toLowerCase()).toContain('lsm');
    expect(configContent).toContain('7');

    // Verify the specific row contains both "LSM" and "7"
    const lsmRow = configSection.querySelector('tr:has(td)');
    const tableRows = configSection.querySelectorAll('tbody tr');
    let foundLSMLevels = false;

    tableRows.forEach(row => {
      const rowText = row.textContent.toLowerCase();
      if (rowText.includes('lsm') && rowText.includes('level')) {
        // This row should contain "7" as the value
        expect(row.textContent).toContain('7');
        foundLSMLevels = true;
      }
    });

    expect(foundLSMLevels).toBe(true);
  });

  /**
   * Test Case 3: Check stated SSTable max size
   * Expected: Page states SSTable max size as 100MB
   * Reference: MirDB default configuration - sst_max_size = "100M"
   */
  test('TC3: Page states SSTable max size as 100MB', () => {
    const configContent = configSection.textContent;

    // Check for SSTable size configuration
    expect(configContent.toLowerCase()).toContain('sstable');

    // Verify the value is 100MB
    const tableRows = configSection.querySelectorAll('tbody tr');
    let foundSSTableSize = false;

    tableRows.forEach(row => {
      const rowText = row.textContent.toLowerCase();
      if (rowText.includes('sstable') && rowText.includes('size')) {
        // This row should contain "100MB" as the value
        expect(row.textContent).toMatch(/100\s*MB/i);
        foundSSTableSize = true;
      }
    });

    expect(foundSSTableSize).toBe(true);
  });

  /**
   * Test Case 4: Check stated memtable max size
   * Expected: Page states memtable max size as 4MB
   * Reference: MirDB default configuration - mem_table_max_size = "4M"
   */
  test('TC4: Page states memtable max size as 4MB', () => {
    const configContent = configSection.textContent;

    // Check for Memtable size configuration
    expect(configContent.toLowerCase()).toContain('memtable');

    // Verify the value is 4MB
    const tableRows = configSection.querySelectorAll('tbody tr');
    let foundMemtableSize = false;

    tableRows.forEach(row => {
      const rowText = row.textContent.toLowerCase();
      if (rowText.includes('memtable') && rowText.includes('size')) {
        // This row should contain "4MB" as the value
        expect(row.textContent).toMatch(/4\s*MB/i);
        foundMemtableSize = true;
      }
    });

    expect(foundMemtableSize).toBe(true);
  });

  /**
   * Test Case 5: Check stated block size
   * Expected: Page states block size as 4KB
   * Reference: MirDB default configuration - block_size = "4K"
   */
  test('TC5: Page states block size as 4KB', () => {
    const configContent = configSection.textContent;

    // Check for Block size configuration
    expect(configContent.toLowerCase()).toContain('block');

    // Verify the value is 4KB
    const tableRows = configSection.querySelectorAll('tbody tr');
    let foundBlockSize = false;

    tableRows.forEach(row => {
      const rowText = row.textContent.toLowerCase();
      if (rowText.includes('block') && rowText.includes('size')) {
        // This row should contain "4KB" as the value
        expect(row.textContent).toMatch(/4\s*KB/i);
        foundBlockSize = true;
      }
    });

    expect(foundBlockSize).toBe(true);
  });

  /**
   * Additional validation: Verify all configuration values match MirDB defaults
   * This provides a comprehensive check of the configuration table
   */
  test('Configuration table contains all expected default values', () => {
    const expectedConfigs = {
      'Listen Address': '0.0.0.0:12333',
      'Max LSM Levels': '7',
      'SSTable Max Size': '100MB',
      'Memtable Max Size': '4MB',
      'Block Size': '4KB',
      'Work Directory': '/tmp/mirdb'
    };

    const tableRows = configSection.querySelectorAll('tbody tr');
    const foundConfigs = {};

    tableRows.forEach(row => {
      const cells = row.querySelectorAll('td');
      if (cells.length >= 2) {
        const paramName = cells[0].textContent.trim();
        const paramValue = cells[1].textContent.trim();
        foundConfigs[paramName] = paramValue;
      }
    });

    // Verify each expected configuration is present with correct value
    Object.entries(expectedConfigs).forEach(([key, expectedValue]) => {
      expect(foundConfigs[key]).toBe(expectedValue);
    });
  });
});
