/**
 * Configuration Reference Unit Tests
 * Owner: Scenario 6 - Configuration Reference
 *
 * Tests for:
 * - Configuration section existence and structure
 * - Listen address configuration with default value
 * - Work directory configuration with default value
 * - SSTable size configuration with default value
 * - Memtable size configuration with default value
 * - Configuration items have descriptions
 *
 * Requirements: REQ-6
 * @jest-environment jsdom
 */

import { readFileSync } from 'fs';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));

describe('Configuration Reference Section', () => {
  let configSection;

  beforeEach(() => {
    // Load the homepage HTML
    const htmlPath = resolve(__dirname, '../../index.html');
    const html = readFileSync(htmlPath, 'utf-8');
    document.documentElement.innerHTML = html;
    configSection = document.getElementById('configuration');
  });

  /**
   * Test Case 1: Query for configuration section
   * Input: Query for configuration section
   * Expected: Section exists with id='configuration' or id='config' or heading containing 'Configuration'
   */
  describe('Test Case 1: Configuration Section Existence', () => {
    test('configuration section exists with id="configuration"', () => {
      expect(configSection).not.toBeNull();
      expect(configSection).toBeInTheDocument();
    });

    test('configuration section is a semantic section element', () => {
      expect(configSection.tagName.toLowerCase()).toBe('section');
    });

    test('configuration section has aria-labelledby for accessibility', () => {
      expect(configSection.getAttribute('aria-labelledby')).toBe('configuration-title');
    });

    test('configuration section contains a heading with "Configuration" text', () => {
      const heading = configSection.querySelector('#configuration-title');
      expect(heading).not.toBeNull();
      expect(heading.textContent.toLowerCase()).toContain('configuration');
    });

    test('configuration heading is H2 element', () => {
      const heading = configSection.querySelector('#configuration-title');
      expect(heading).not.toBeNull();
      expect(heading.tagName.toLowerCase()).toBe('h2');
    });
  });

  /**
   * Test Case 2: Check for listen address configuration
   * Input: Check for listen address configuration
   * Expected: Element contains text about 'listen' or 'address' with a default value
   */
  describe('Test Case 2: Listen Address Configuration', () => {
    test('configuration contains text about listen address', () => {
      const text = configSection.textContent.toLowerCase();
      const hasListenAddress = text.includes('listen') || text.includes('address');
      expect(hasListenAddress).toBe(true);
    });

    test('listen address has a default value displayed', () => {
      const configItems = configSection.querySelectorAll('.config__item, .config-item, [data-config]');
      const listenItem = Array.from(configItems).find(item => {
        const text = item.textContent.toLowerCase();
        return text.includes('listen') || text.includes('address');
      });
      expect(listenItem).not.toBeUndefined();
      // Check for common default values like 127.0.0.1:11211 or localhost:11211
      const text = listenItem.textContent;
      const hasDefaultValue = text.includes('127.0.0.1') ||
                              text.includes('localhost') ||
                              text.includes('11211') ||
                              text.includes('0.0.0.0');
      expect(hasDefaultValue).toBe(true);
    });
  });

  /**
   * Test Case 3: Check for work directory configuration
   * Input: Check for work directory configuration
   * Expected: Element contains text about 'work' and 'directory' or 'path' with a default value
   */
  describe('Test Case 3: Work Directory Configuration', () => {
    test('configuration contains text about work directory or path', () => {
      const text = configSection.textContent.toLowerCase();
      const hasWorkDir = (text.includes('work') && (text.includes('directory') || text.includes('dir'))) ||
                         text.includes('data') && text.includes('path');
      expect(hasWorkDir).toBe(true);
    });

    test('work directory has a default value displayed', () => {
      const configItems = configSection.querySelectorAll('.config__item, .config-item, [data-config]');
      const workDirItem = Array.from(configItems).find(item => {
        const text = item.textContent.toLowerCase();
        return (text.includes('work') && (text.includes('directory') || text.includes('dir'))) ||
               (text.includes('data') && text.includes('path'));
      });
      expect(workDirItem).not.toBeUndefined();
      // Check for path-like default value
      const text = workDirItem.textContent;
      const hasPathValue = text.includes('/') || text.includes('./') || text.includes('data');
      expect(hasPathValue).toBe(true);
    });
  });

  /**
   * Test Case 4: Check for SSTable size configuration
   * Input: Check for SSTable size configuration
   * Expected: Element contains text about 'SSTable' or 'sstable' and 'size' with a default value
   */
  describe('Test Case 4: SSTable Size Configuration', () => {
    test('configuration contains text about SSTable size', () => {
      const text = configSection.textContent.toLowerCase();
      const hasSSTableSize = text.includes('sstable') && text.includes('size');
      expect(hasSSTableSize).toBe(true);
    });

    test('SSTable size has a default value displayed', () => {
      const configItems = configSection.querySelectorAll('.config__item, .config-item, [data-config]');
      const ssTableItem = Array.from(configItems).find(item => {
        const text = item.textContent.toLowerCase();
        return text.includes('sstable') && text.includes('size');
      });
      expect(ssTableItem).not.toBeUndefined();
      // Check for size value (MB, KB, GB, or numeric)
      const text = ssTableItem.textContent;
      const hasSizeValue = /\d+\s*(mb|kb|gb|bytes|b)/i.test(text) || /\d+/.test(text);
      expect(hasSizeValue).toBe(true);
    });
  });

  /**
   * Test Case 5: Check for memtable size configuration
   * Input: Check for memtable size configuration
   * Expected: Element contains text about 'memtable' and 'size' with a default value
   */
  describe('Test Case 5: Memtable Size Configuration', () => {
    test('configuration contains text about memtable size', () => {
      const text = configSection.textContent.toLowerCase();
      const hasMemtableSize = text.includes('memtable') && text.includes('size');
      expect(hasMemtableSize).toBe(true);
    });

    test('memtable size has a default value displayed', () => {
      const configItems = configSection.querySelectorAll('.config__item, .config-item, [data-config]');
      const memtableItem = Array.from(configItems).find(item => {
        const text = item.textContent.toLowerCase();
        return text.includes('memtable') && text.includes('size');
      });
      expect(memtableItem).not.toBeUndefined();
      // Check for size value (MB, KB, GB, or numeric)
      const text = memtableItem.textContent;
      const hasSizeValue = /\d+\s*(mb|kb|gb|bytes|b)/i.test(text) || /\d+/.test(text);
      expect(hasSizeValue).toBe(true);
    });
  });

  /**
   * Test Case 6: Verify configuration items have descriptions
   * Input: Verify configuration items have descriptions
   * Expected: Each configuration option has a name, default value, and brief description
   */
  describe('Test Case 6: Configuration Items Have Descriptions', () => {
    test('configuration items have name, default value, and description', () => {
      const configItems = configSection.querySelectorAll('.config__item, .config-item, [data-config]');
      expect(configItems.length).toBeGreaterThanOrEqual(4); // At least 4 config options

      configItems.forEach((item) => {
        // Check for name element
        const name = item.querySelector('.config__name, .config-name, dt, th, [data-config-name]');
        expect(name).not.toBeNull();
        expect(name.textContent.trim().length).toBeGreaterThan(0);

        // Check for default value element
        const defaultValue = item.querySelector('.config__default, .config-default, [data-config-default], code');
        expect(defaultValue).not.toBeNull();
        expect(defaultValue.textContent.trim().length).toBeGreaterThan(0);

        // Check for description element
        const description = item.querySelector('.config__description, .config-description, dd, td, [data-config-description]');
        expect(description).not.toBeNull();
        expect(description.textContent.trim().length).toBeGreaterThan(0);
      });
    });

    test('configuration section uses structured format (table or definition list)', () => {
      // Check for table or definition list structure
      const table = configSection.querySelector('table');
      const definitionList = configSection.querySelector('dl');
      const configList = configSection.querySelector('.config__list, .config-list');
      const hasStructuredFormat = table !== null || definitionList !== null || configList !== null;
      expect(hasStructuredFormat).toBe(true);
    });
  });

  /**
   * Additional structural and accessibility tests
   */
  describe('Structure & Accessibility', () => {
    test('configuration section has proper structure with header', () => {
      const header = configSection.querySelector('.config__header, .configuration__header, header');
      const title = configSection.querySelector('#configuration-title, .config__title, .configuration__title');
      expect(title).not.toBeNull();
    });

    test('configuration section has description text', () => {
      const description = configSection.querySelector('.config__description, .configuration__description, p');
      expect(description).not.toBeNull();
      expect(description.textContent.trim().length).toBeGreaterThan(0);
    });

    test('configuration items are contained within the section', () => {
      const container = configSection.querySelector('.container');
      expect(container).not.toBeNull();
      const hasContent = container.children.length > 1; // More than just a placeholder comment
      expect(hasContent).toBe(true);
    });
  });

  /**
   * Test Case 7: Visual inspection of configuration layout (manual test placeholder)
   * Input: Visual inspection of configuration layout
   * Expected: Configuration information is displayed in a clear, scannable format (table or definition list)
   * Note: This is marked as a manual test and will be verified through preview recording
   */
  describe('Test Case 7: Configuration Layout (Visual)', () => {
    test('configuration content is present and non-empty', () => {
      const container = configSection.querySelector('.container');
      const textContent = container.textContent.replace(/\s+/g, ' ').trim();
      expect(textContent.length).toBeGreaterThan(50); // Substantial content
    });

    test('configuration section is visually distinct with proper class', () => {
      expect(configSection.classList.contains('configuration') || configSection.classList.contains('config')).toBe(true);
    });
  });
});
