/**
 * Unit tests for Configuration component.
 * Owner: Scenario 6 - Configuration Reference
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { renderConfiguration } from '../../../src/components/Configuration';
import { configOptions } from '../../../src/data/config';

describe('Configuration Component', () => {
  let section: HTMLElement;

  beforeEach(() => {
    section = renderConfiguration();
  });

  it('renders a section element with proper id', () => {
    expect(section.tagName).toBe('SECTION');
    expect(section.id).toBe('configuration');
    expect(section.className).toContain('configuration-section');
  });

  it('has proper heading structure', () => {
    const heading = section.querySelector('h2');
    expect(heading).not.toBeNull();
    expect(heading?.id).toBe('configuration-heading');
    expect(heading?.textContent).toBe('Configuration Reference');
  });

  it('has accessible aria attributes', () => {
    expect(section.getAttribute('aria-labelledby')).toBe('configuration-heading');

    const table = section.querySelector('table');
    expect(table?.getAttribute('role')).toBe('table');
    expect(table?.getAttribute('aria-label')).toBe('Configuration options');
  });

  it('displays configuration section with address option', () => {
    const addrOption = configOptions.find(opt => opt.name === 'addr');
    expect(addrOption).toBeDefined();

    const rows = section.querySelectorAll('tbody tr');
    const addrRow = Array.from(rows).find(row => {
      const nameCell = row.querySelector('.config-name code');
      return nameCell?.textContent === 'addr';
    });

    expect(addrRow).not.toBeNull();

    const defaultCell = addrRow?.querySelector('.config-default code');
    expect(defaultCell?.textContent).toBe('0.0.0.0:12333');
  });

  it('displays configuration section with work directory option', () => {
    const workDirOption = configOptions.find(opt => opt.name === 'work_dir');
    expect(workDirOption).toBeDefined();

    const rows = section.querySelectorAll('tbody tr');
    const workDirRow = Array.from(rows).find(row => {
      const nameCell = row.querySelector('.config-name code');
      return nameCell?.textContent === 'work_dir';
    });

    expect(workDirRow).not.toBeNull();

    const defaultCell = workDirRow?.querySelector('.config-default code');
    expect(defaultCell?.textContent).toBe('/tmp/mirdb');
  });

  it('each configuration option has a description', () => {
    const rows = section.querySelectorAll('tbody tr');
    expect(rows.length).toBe(configOptions.length);

    rows.forEach((row, index) => {
      const descCell = row.querySelector('.config-description');
      expect(descCell).not.toBeNull();
      expect(descCell?.textContent?.trim().length).toBeGreaterThan(0);
      expect(descCell?.textContent).toBe(configOptions[index].description);
    });
  });

  it('displays options in a table format', () => {
    const table = section.querySelector('table.config-table');
    expect(table).not.toBeNull();

    const thead = table?.querySelector('thead');
    expect(thead).not.toBeNull();

    const headers = thead?.querySelectorAll('th');
    expect(headers?.length).toBe(3);
    expect(headers?.[0].textContent).toBe('Option');
    expect(headers?.[1].textContent).toBe('Default');
    expect(headers?.[2].textContent).toBe('Description');

    const tbody = table?.querySelector('tbody');
    expect(tbody).not.toBeNull();

    const rows = tbody?.querySelectorAll('tr');
    expect(rows?.length).toBe(configOptions.length);
  });

  it('renders all configuration options from mirdb.toml', () => {
    const rows = section.querySelectorAll('tbody tr');
    expect(rows.length).toBe(configOptions.length);

    configOptions.forEach((option) => {
      const row = Array.from(rows).find(r => {
        const nameCell = r.querySelector('.config-name code');
        return nameCell?.textContent === option.name;
      });

      expect(row).not.toBeNull();

      const defaultCell = row?.querySelector('.config-default code');
      expect(defaultCell?.textContent).toBe(option.default);

      const descCell = row?.querySelector('.config-description');
      expect(descCell?.textContent).toBe(option.description);
    });
  });

  it('includes max_level configuration option', () => {
    const rows = section.querySelectorAll('tbody tr');
    const maxLevelRow = Array.from(rows).find(row => {
      const nameCell = row.querySelector('.config-name code');
      return nameCell?.textContent === 'max_level';
    });

    expect(maxLevelRow).not.toBeNull();
    const defaultCell = maxLevelRow?.querySelector('.config-default code');
    expect(defaultCell?.textContent).toBe('7');
  });

  it('includes mem_table_max_size configuration option', () => {
    const rows = section.querySelectorAll('tbody tr');
    const memTableRow = Array.from(rows).find(row => {
      const nameCell = row.querySelector('.config-name code');
      return nameCell?.textContent === 'mem_table_max_size';
    });

    expect(memTableRow).not.toBeNull();
    const defaultCell = memTableRow?.querySelector('.config-default code');
    expect(defaultCell?.textContent).toBe('4M');
  });

  it('has a table wrapper for responsive scrolling', () => {
    const wrapper = section.querySelector('.config-table-wrapper');
    expect(wrapper).not.toBeNull();

    const table = wrapper?.querySelector('table');
    expect(table).not.toBeNull();
  });

  it('uses semantic table elements', () => {
    const table = section.querySelector('table');
    const thead = table?.querySelector('thead');
    const tbody = table?.querySelector('tbody');

    expect(thead?.tagName).toBe('THEAD');
    expect(tbody?.tagName).toBe('TBODY');

    const headerCells = thead?.querySelectorAll('th');
    headerCells?.forEach(th => {
      expect(th.getAttribute('scope')).toBe('col');
    });
  });

  it('renders introduction text', () => {
    const intro = section.querySelector('.configuration-intro');
    expect(intro).not.toBeNull();
    expect(intro?.textContent).toContain('TOML');
    expect(intro?.textContent).toContain('mirdb.toml');
  });

  it('uses code elements for option names', () => {
    const nameCells = section.querySelectorAll('.config-name');
    nameCells.forEach(cell => {
      const code = cell.querySelector('code');
      expect(code).not.toBeNull();
    });
  });

  it('uses code elements for default values', () => {
    const defaultCells = section.querySelectorAll('.config-default');
    defaultCells.forEach(cell => {
      const code = cell.querySelector('code');
      expect(code).not.toBeNull();
    });
  });
});
