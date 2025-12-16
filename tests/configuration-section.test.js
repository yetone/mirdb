/**
 * Configuration Section Tests
 * Scenario: Verify that configuration options and example TOML config are displayed (REQ-5)
 */

const fs = require('fs');
const path = require('path');

describe('Configuration Section', () => {
  let document;

  beforeAll(() => {
    // Load the HTML file
    const html = fs.readFileSync(path.join(__dirname, '../index.html'), 'utf8');
    document = new DOMParser().parseFromString(html, 'text/html');
  });

  // Test Case 1: Configuration section exists with heading
  describe('Test Case 1: Configuration section element', () => {
    test('Configuration section exists with heading', () => {
      const configSection = document.getElementById('configuration');
      expect(configSection).not.toBeNull();
      expect(configSection.tagName.toLowerCase()).toBe('section');

      // Check for heading
      const heading = configSection.querySelector('h2');
      expect(heading).not.toBeNull();
      expect(heading.textContent.toLowerCase()).toContain('configuration');
    });
  });

  // Test Case 2: TOML configuration example exists
  describe('Test Case 2: TOML configuration example', () => {
    test('Code block with TOML configuration exists', () => {
      const configSection = document.getElementById('configuration');
      expect(configSection).not.toBeNull();

      // Look for code block with TOML content
      const codeBlocks = configSection.querySelectorAll('pre, code');
      expect(codeBlocks.length).toBeGreaterThan(0);

      // Check that at least one code block contains TOML-like configuration
      let hasTomlConfig = false;
      for (const block of codeBlocks) {
        const content = block.textContent;
        // TOML config should have key = "value" patterns
        if (content.includes('addr') && content.includes('=') && content.includes('work_dir')) {
          hasTomlConfig = true;
          break;
        }
      }
      expect(hasTomlConfig).toBe(true);
    });

    test('TOML example includes key configuration parameters', () => {
      const configSection = document.getElementById('configuration');
      expect(configSection).not.toBeNull();

      const codeBlocks = configSection.querySelectorAll('pre, code');
      let tomlContent = '';

      for (const block of codeBlocks) {
        tomlContent += block.textContent;
      }

      // Check for essential TOML parameters
      expect(tomlContent).toContain('addr');
      expect(tomlContent).toContain('work_dir');
      expect(tomlContent).toContain('max_level');
    });
  });

  // Test Case 3: addr parameter documentation
  describe('Test Case 3: addr parameter documentation', () => {
    test('addr parameter documented with default "0.0.0.0:12333"', () => {
      const configSection = document.getElementById('configuration');
      expect(configSection).not.toBeNull();

      const sectionText = configSection.textContent;

      // Check for addr parameter
      expect(sectionText).toContain('addr');

      // Check for default value
      expect(sectionText).toContain('0.0.0.0:12333');
    });
  });

  // Test Case 4: work_dir parameter documentation
  describe('Test Case 4: work_dir parameter documentation', () => {
    test('work_dir parameter documented with default "/tmp/mirdb"', () => {
      const configSection = document.getElementById('configuration');
      expect(configSection).not.toBeNull();

      const sectionText = configSection.textContent;

      // Check for work_dir parameter
      expect(sectionText).toContain('work_dir');

      // Check for default value
      expect(sectionText).toContain('/tmp/mirdb');
    });
  });

  // Test Case 5: Parameter table or organized list format
  describe('Test Case 5: Parameter table or list', () => {
    test('Configuration parameters displayed in table or organized list format', () => {
      const configSection = document.getElementById('configuration');
      expect(configSection).not.toBeNull();

      // Check for table or organized list structure
      const table = configSection.querySelector('table');
      const list = configSection.querySelector('ul, ol, dl');
      const parameterGrid = configSection.querySelector('.config-params, .parameters, [class*="param"]');

      // Should have at least one organized format (table, list, or grid)
      const hasOrganizedFormat = table !== null || list !== null || parameterGrid !== null;
      expect(hasOrganizedFormat).toBe(true);

      // If table exists, verify it has parameter rows
      if (table) {
        const rows = table.querySelectorAll('tr');
        expect(rows.length).toBeGreaterThan(1); // At least header + 1 data row

        // Check table contains key parameters
        const tableText = table.textContent;
        expect(tableText).toContain('addr');
        expect(tableText).toContain('work_dir');
      }
    });

    test('Parameters include additional configuration options', () => {
      const configSection = document.getElementById('configuration');
      expect(configSection).not.toBeNull();

      const sectionText = configSection.textContent;

      // Check for additional important parameters from PRD
      expect(sectionText).toContain('max_level');
      expect(sectionText).toMatch(/sst_max_size|mem_table_max_size/);
    });
  });

  // Additional validation: TOML code block has proper formatting
  describe('Additional: TOML formatting', () => {
    test('TOML example is copy-paste ready', () => {
      const configSection = document.getElementById('configuration');
      expect(configSection).not.toBeNull();

      // Find the pre/code element containing TOML
      const codeBlock = configSection.querySelector('pre code, pre');
      expect(codeBlock).not.toBeNull();

      const content = codeBlock.textContent;

      // Verify TOML syntax with proper formatting
      expect(content).toMatch(/addr\s*=\s*["']0\.0\.0\.0:12333["']/);
      expect(content).toMatch(/work_dir\s*=\s*["']\/tmp\/mirdb["']/);
    });
  });
});
