/**
 * Unit tests for Configuration Section (Scenario 6).
 *
 * Tests:
 * - Configuration section uses semantic section element with id='configuration'
 * - h2 heading contains 'Configuration'
 * - Config values are in a table element or definition list for semantic structure
 * - All expected configuration rows are present
 */

import { describe, test, expect, beforeAll } from 'vitest';
import { readFileSync } from 'fs';
import { resolve } from 'path';

describe('Configuration Section HTML Structure', () => {
  let html;
  let parser;
  let doc;

  beforeAll(() => {
    const htmlPath = resolve(process.cwd(), 'index.html');
    html = readFileSync(htmlPath, 'utf-8');
    parser = new DOMParser();
    doc = parser.parseFromString(html, 'text/html');
  });

  test('configuration uses semantic section element with id="configuration"', () => {
    const configSection = doc.querySelector('section#configuration');
    expect(configSection).not.toBeNull();
    expect(configSection.tagName.toLowerCase()).toBe('section');
    expect(configSection.id).toBe('configuration');
  });

  test('configuration section has h2 heading containing "Configuration"', () => {
    const configSection = doc.querySelector('section#configuration');
    const h2 = configSection.querySelector('h2');
    expect(h2).not.toBeNull();
    expect(h2.textContent.trim()).toBe('Configuration');
  });

  test('configuration values are in a table element for semantic structure', () => {
    const configSection = doc.querySelector('section#configuration');
    const table = configSection.querySelector('table.config-table');
    expect(table).not.toBeNull();

    const thead = table.querySelector('thead');
    expect(thead).not.toBeNull();

    const tbody = table.querySelector('tbody');
    expect(tbody).not.toBeNull();

    const rows = tbody.querySelectorAll('tr');
    expect(rows.length).toBeGreaterThanOrEqual(11);
  });

  test('configuration table has correct header columns', () => {
    const configSection = doc.querySelector('section#configuration');
    const table = configSection.querySelector('table.config-table');
    const headers = table.querySelectorAll('thead th');

    expect(headers.length).toBe(3);
    expect(headers[0].textContent.trim()).toBe('Option');
    expect(headers[1].textContent.trim()).toBe('Default Value');
    expect(headers[2].textContent.trim()).toBe('Description');
  });

  test('listen address row is present with correct value', () => {
    const configSection = doc.querySelector('section#configuration');
    const row = configSection.querySelector('[data-testid="config-addr"]');
    expect(row).not.toBeNull();
    expect(row.textContent).toContain('0.0.0.0:12333');
  });

  test('max_level row is present with correct value', () => {
    const configSection = doc.querySelector('section#configuration');
    const row = configSection.querySelector('[data-testid="config-max-level"]');
    expect(row).not.toBeNull();
    expect(row.textContent).toContain('7');
  });

  test('work_dir row is present with correct value', () => {
    const configSection = doc.querySelector('section#configuration');
    const row = configSection.querySelector('[data-testid="config-work-dir"]');
    expect(row).not.toBeNull();
    expect(row.textContent).toContain('/tmp/mirdb');
  });

  test('sst_max_size row is present with correct value', () => {
    const configSection = doc.querySelector('section#configuration');
    const row = configSection.querySelector('[data-testid="config-sst-max-size"]');
    expect(row).not.toBeNull();
    expect(row.textContent).toContain('100MB');
  });

  test('mem_table_max_size row is present with correct value', () => {
    const configSection = doc.querySelector('section#configuration');
    const row = configSection.querySelector('[data-testid="config-mem-table-max-size"]');
    expect(row).not.toBeNull();
    expect(row.textContent).toContain('4MB');
  });

  test('block_size row is present with correct value', () => {
    const configSection = doc.querySelector('section#configuration');
    const row = configSection.querySelector('[data-testid="config-block-size"]');
    expect(row).not.toBeNull();
    expect(row.textContent).toContain('4KB');
  });

  test('all additional config values are present', () => {
    const configSection = doc.querySelector('section#configuration');
    const tableText = configSection.querySelector('table.config-table').textContent;

    expect(tableText).toContain('mem_table_max_height');
    expect(tableText).toContain('32');
    expect(tableText).toContain('imm_mem_table_max_count');
    expect(tableText).toContain('16');
    expect(tableText).toContain('block_restart_interval');
    expect(tableText).toContain('l0_compaction_trigger');
    expect(tableText).toContain('4');
    expect(tableText).toContain('thread_sleep_ms');
    expect(tableText).toContain('500');
  });
});
