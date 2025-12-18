import { describe, it, expect, beforeEach } from 'vitest';
import { JSDOM } from 'jsdom';
import fs from 'fs';
import path from 'path';

/**
 * Unit Tests for Configuration Section
 * Tests validate that the configuration section displays default values
 * and tunable parameters as per PRD requirements.
 */

describe('Configuration Section', () => {
  let document: Document;
  let configSection: Element | null;

  beforeEach(() => {
    // Load the actual HTML file
    const htmlPath = path.resolve(__dirname, '../website/index.html');
    const html = fs.readFileSync(htmlPath, 'utf-8');
    const dom = new JSDOM(html);
    document = dom.window.document;
    configSection = document.querySelector('#configuration, [data-testid="configuration-section"]');
  });

  describe('TC1: Check for default port configuration', () => {
    it('should have a configuration section visible', () => {
      expect(configSection).not.toBeNull();
    });

    it('should display default port 12333', () => {
      const configContent = configSection?.textContent || '';
      expect(configContent).toContain('12333');
    });

    it('should have port parameter in the TOML example', () => {
      const tomlCode = configSection?.querySelector('pre code');
      expect(tomlCode).not.toBeNull();
      const codeContent = tomlCode?.textContent || '';
      expect(codeContent).toContain('addr');
      expect(codeContent).toContain('12333');
    });
  });

  describe('TC2: Check for memtable size configuration', () => {
    it('should display default memtable size 4MB', () => {
      const configContent = configSection?.textContent || '';
      expect(configContent).toMatch(/4\s*MB|4M/i);
    });

    it('should have memtable parameter in the TOML example', () => {
      const tomlCode = configSection?.querySelector('pre code');
      const codeContent = tomlCode?.textContent || '';
      expect(codeContent).toContain('mem_table_max_size');
      expect(codeContent).toMatch(/4M/);
    });
  });

  describe('TC3: Check for SSTable size configuration', () => {
    it('should display default SSTable size 100MB', () => {
      const configContent = configSection?.textContent || '';
      expect(configContent).toMatch(/100\s*MB|100M/i);
    });

    it('should have SSTable parameter in the TOML example', () => {
      const tomlCode = configSection?.querySelector('pre code');
      const codeContent = tomlCode?.textContent || '';
      expect(codeContent).toContain('sst_max_size');
      expect(codeContent).toMatch(/100M/);
    });
  });

  describe('TC4: Check for LSM levels configuration', () => {
    it('should document 7 LSM levels', () => {
      const configContent = configSection?.textContent || '';
      expect(configContent).toMatch(/7.*level|level.*7/i);
    });

    it('should have max_level parameter in the TOML example', () => {
      const tomlCode = configSection?.querySelector('pre code');
      const codeContent = tomlCode?.textContent || '';
      expect(codeContent).toContain('max_level');
      expect(codeContent).toContain('7');
    });
  });

  describe('TC5: Verify configuration example format - TOML with syntax highlighting', () => {
    it('should have TOML code block with proper language class', () => {
      const tomlCode = configSection?.querySelector('pre code.language-toml');
      expect(tomlCode).not.toBeNull();
    });

    it('should have TOML format with key = value syntax', () => {
      const tomlCode = configSection?.querySelector('pre code');
      const codeContent = tomlCode?.textContent || '';

      // Verify TOML format - should have key = "value" syntax
      expect(codeContent).toMatch(/addr\s*=\s*"/);
      expect(codeContent).toMatch(/max_level\s*=\s*\d/);
      expect(codeContent).toMatch(/work_dir\s*=\s*"/);
    });

    it('should include multiple configuration parameters', () => {
      const tomlCode = configSection?.querySelector('pre code');
      const codeContent = tomlCode?.textContent || '';

      expect(codeContent).toContain('sst_max_size');
      expect(codeContent).toContain('mem_table_max_size');
      expect(codeContent).toContain('l0_compaction_trigger');
    });
  });

  describe('Verify parameter descriptions are present', () => {
    it('should have a configuration table', () => {
      const configTable = configSection?.querySelector('.config-table, table');
      expect(configTable).not.toBeNull();
    });

    it('should have table headers for Parameter, Description, Default', () => {
      const configTable = configSection?.querySelector('.config-table, table');
      const tableHeaders = Array.from(configTable?.querySelectorAll('th') || [])
        .map(th => th.textContent?.toLowerCase() || '');

      expect(tableHeaders.some(h => h.includes('parameter'))).toBe(true);
      expect(tableHeaders.some(h => h.includes('description'))).toBe(true);
      expect(tableHeaders.some(h => h.includes('default'))).toBe(true);
    });

    it('should have rows with configuration parameters', () => {
      const configTable = configSection?.querySelector('.config-table, table');
      const tableRows = configTable?.querySelectorAll('tbody tr');
      expect(tableRows?.length).toBeGreaterThan(0);
    });
  });

  describe('Navigation to Configuration section', () => {
    it('should have a navigation link to configuration section', () => {
      const navLink = document.querySelector('a[href="#configuration"]');
      expect(navLink).not.toBeNull();
    });

    it('should have Configuration text in the navigation', () => {
      const navLink = document.querySelector('a[href="#configuration"]');
      expect(navLink?.textContent).toContain('Configuration');
    });
  });
});
