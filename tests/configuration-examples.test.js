/**
 * Tests for Configuration Examples Display (REQ-5)
 * Verify configuration examples with common parameters are shown
 */

const fs = require('fs');
const path = require('path');

describe('Configuration Examples Display', () => {
  let document;

  beforeEach(() => {
    const html = fs.readFileSync(path.resolve(__dirname, '../index.html'), 'utf8');
    document = new DOMParser().parseFromString(html, 'text/html');
  });

  // Test Case 1: TOML configuration example is present on the page
  describe('Test Case 1: Configuration Code Block', () => {
    test('should have a configuration code block on the page', () => {
      const codeBlocks = document.querySelectorAll('pre code, code.language-toml, pre.language-toml');
      expect(codeBlocks.length).toBeGreaterThan(0);
    });

    test('should have TOML configuration content', () => {
      const pageContent = document.body.textContent;
      // Check for TOML-like configuration markers
      expect(pageContent).toMatch(/addr\s*=/);
    });

    test('should display configuration in a pre/code element for proper formatting', () => {
      const preElements = document.querySelectorAll('pre');
      const hasConfigPre = Array.from(preElements).some(pre =>
        pre.textContent.includes('addr') && pre.textContent.includes('=')
      );
      expect(hasConfigPre).toBe(true);
    });
  });

  // Test Case 2: Configuration shows 'addr' parameter with example value
  describe('Test Case 2: Addr Configuration Parameter', () => {
    test('should display addr configuration parameter', () => {
      const pageContent = document.body.textContent;
      expect(pageContent).toContain('addr');
    });

    test('should show addr with an IP address and port value', () => {
      const preElements = document.querySelectorAll('pre');
      const configPre = Array.from(preElements).find(pre =>
        pre.textContent.includes('addr')
      );
      expect(configPre).not.toBeNull();
      // Check for IP:port pattern
      expect(configPre.textContent).toMatch(/addr\s*=\s*["']?\d+\.\d+\.\d+\.\d+:\d+["']?/);
    });

    test('should show default addr value of 0.0.0.0:12333', () => {
      const preElements = document.querySelectorAll('pre');
      const configPre = Array.from(preElements).find(pre =>
        pre.textContent.includes('addr')
      );
      expect(configPre).not.toBeNull();
      expect(configPre.textContent).toContain('0.0.0.0:12333');
    });
  });

  // Test Case 3: Configuration includes work_dir, sst_max_size, mem_table_max_size parameters
  describe('Test Case 3: Storage-related Configuration Parameters', () => {
    test('should display work_dir configuration parameter', () => {
      const preElements = document.querySelectorAll('pre');
      const configPre = Array.from(preElements).find(pre =>
        pre.textContent.includes('work_dir')
      );
      expect(configPre).not.toBeNull();
      expect(configPre.textContent).toContain('work_dir');
    });

    test('should display sst_max_size configuration parameter', () => {
      const preElements = document.querySelectorAll('pre');
      const configPre = Array.from(preElements).find(pre =>
        pre.textContent.includes('sst_max_size')
      );
      expect(configPre).not.toBeNull();
      expect(configPre.textContent).toContain('sst_max_size');
    });

    test('should display mem_table_max_size configuration parameter', () => {
      const preElements = document.querySelectorAll('pre');
      const configPre = Array.from(preElements).find(pre =>
        pre.textContent.includes('mem_table_max_size')
      );
      expect(configPre).not.toBeNull();
      expect(configPre.textContent).toContain('mem_table_max_size');
    });

    test('should have work_dir with a path value', () => {
      const preElements = document.querySelectorAll('pre');
      const configPre = Array.from(preElements).find(pre =>
        pre.textContent.includes('work_dir')
      );
      expect(configPre).not.toBeNull();
      expect(configPre.textContent).toMatch(/work_dir\s*=\s*["'][^"']+["']/);
    });

    test('should have sst_max_size with a size value', () => {
      const preElements = document.querySelectorAll('pre');
      const configPre = Array.from(preElements).find(pre =>
        pre.textContent.includes('sst_max_size')
      );
      expect(configPre).not.toBeNull();
      expect(configPre.textContent).toMatch(/sst_max_size\s*=\s*["']\d+[KMGT]?["']/);
    });

    test('should have mem_table_max_size with a size value', () => {
      const preElements = document.querySelectorAll('pre');
      const configPre = Array.from(preElements).find(pre =>
        pre.textContent.includes('mem_table_max_size')
      );
      expect(configPre).not.toBeNull();
      expect(configPre.textContent).toMatch(/mem_table_max_size\s*=\s*["']\d+[KMGT]?["']/);
    });
  });

  // Test Case 4: Configuration code block has syntax highlighting applied
  describe('Test Case 4: Configuration Syntax Highlighting', () => {
    test('should have syntax highlighting class on configuration code block', () => {
      const codeBlocks = document.querySelectorAll('pre code, code[class*="language-"], pre[class*="language-"]');
      const highlightedBlock = Array.from(codeBlocks).find(block =>
        block.textContent.includes('addr') ||
        block.closest('pre')?.textContent.includes('addr')
      );

      // Check if code block or its parent has syntax highlighting class
      const hasHighlightClass = highlightedBlock && (
        highlightedBlock.className.includes('language-') ||
        highlightedBlock.className.includes('highlight') ||
        highlightedBlock.closest('pre')?.className.includes('language-') ||
        highlightedBlock.closest('pre')?.className.includes('highlight')
      );

      expect(hasHighlightClass).toBe(true);
    });

    test('should have language-toml class for TOML configuration', () => {
      const tomlBlocks = document.querySelectorAll('.language-toml, [class*="language-toml"]');
      expect(tomlBlocks.length).toBeGreaterThan(0);
    });

    test('should have code block within pre element', () => {
      const preElements = document.querySelectorAll('pre');
      const configPre = Array.from(preElements).find(pre =>
        pre.textContent.includes('addr')
      );
      expect(configPre).not.toBeNull();

      // Check for code element inside pre
      const codeElement = configPre.querySelector('code');
      expect(codeElement).not.toBeNull();
    });
  });

  // Additional test for configuration section location
  describe('Configuration Section Location', () => {
    test('should have configuration in Getting Started or Features section', () => {
      const gettingStarted = document.querySelector('#getting-started, .getting-started, [data-section="getting-started"]');
      const features = document.querySelector('#features, .features, [data-section="features"]');
      const configuration = document.querySelector('#configuration, .configuration, [data-section="configuration"]');

      // Configuration should be in one of these sections
      const hasConfigSection =
        (gettingStarted && gettingStarted.textContent.includes('addr')) ||
        (features && features.textContent.includes('addr')) ||
        (configuration && configuration.textContent.includes('addr'));

      expect(hasConfigSection).toBe(true);
    });
  });
});
