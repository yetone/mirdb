/**
 * Comparison Section Unit Tests
 * Owner: Scenario 9 - MirDB vs Memcached Comparison
 *
 * Tests:
 * - Comparison section exists
 * - MirDB has persistence while memcached does not
 * - MirDB uses same protocol as memcached
 */
import { describe, it, expect, beforeAll } from 'vitest';
import { readFileSync } from 'fs';
import { JSDOM } from 'jsdom';
import { resolve } from 'path';

describe('MirDB vs Memcached Comparison (Unit)', () => {
  let document;

  beforeAll(() => {
    const htmlPath = resolve(process.cwd(), 'src/index.html');
    const html = readFileSync(htmlPath, 'utf-8');
    const dom = new JSDOM(html);
    document = dom.window.document;
  });

  it('TC2: Verify persistence comparison - MirDB has persistence while memcached does not', () => {
    // Find the comparison section
    const comparisonSection = document.querySelector('#comparison');
    expect(comparisonSection).not.toBeNull();

    // Find the persistence row
    const persistenceRow = comparisonSection.querySelector('[data-feature="persistence"]');
    expect(persistenceRow).not.toBeNull();

    // Get all cells in the row
    const cells = persistenceRow.querySelectorAll('td');
    expect(cells.length).toBe(3);

    // First cell should mention "Data Persistence"
    expect(cells[0].textContent.toLowerCase()).toContain('persistence');

    // MirDB column (second cell) should show "Yes"
    const mirdbCell = cells[1];
    expect(mirdbCell.textContent.toLowerCase()).toContain('yes');
    expect(mirdbCell.querySelector('.comparison-yes')).not.toBeNull();

    // Memcached column (third cell) should show "No"
    const memcachedCell = cells[2];
    expect(memcachedCell.textContent.toLowerCase()).toContain('no');
    expect(memcachedCell.querySelector('.comparison-no')).not.toBeNull();
  });

  it('TC3: Verify protocol compatibility mention - MirDB uses same protocol as memcached', () => {
    // Find the comparison section
    const comparisonSection = document.querySelector('#comparison');
    expect(comparisonSection).not.toBeNull();

    // Find the protocol row
    const protocolRow = comparisonSection.querySelector('[data-feature="protocol"]');
    expect(protocolRow).not.toBeNull();

    // Get all cells in the row
    const cells = protocolRow.querySelectorAll('td');
    expect(cells.length).toBe(3);

    // First cell should mention "Memcached Protocol"
    expect(cells[0].textContent.toLowerCase()).toContain('memcached');
    expect(cells[0].textContent.toLowerCase()).toContain('protocol');

    // MirDB column (second cell) should show "Yes"
    const mirdbCell = cells[1];
    expect(mirdbCell.textContent.toLowerCase()).toContain('yes');
    expect(mirdbCell.querySelector('.comparison-yes')).not.toBeNull();

    // Memcached column (third cell) should also show "Yes" (same protocol)
    const memcachedCell = cells[2];
    expect(memcachedCell.textContent.toLowerCase()).toContain('yes');
    expect(memcachedCell.querySelector('.comparison-yes')).not.toBeNull();
  });

  it('Comparison section has proper heading', () => {
    const comparisonSection = document.querySelector('#comparison');
    expect(comparisonSection).not.toBeNull();

    const heading = comparisonSection.querySelector('h2');
    expect(heading).not.toBeNull();
    expect(heading.textContent.toLowerCase()).toContain('mirdb');
    expect(heading.textContent.toLowerCase()).toContain('memcached');
  });

  it('Comparison mentions protocol compatibility in description', () => {
    const comparisonSection = document.querySelector('#comparison');
    expect(comparisonSection).not.toBeNull();

    // Check that the section mentions protocol compatibility
    const sectionText = comparisonSection.textContent.toLowerCase();
    expect(sectionText).toContain('protocol');
    expect(sectionText).toContain('memcached');
  });

  it('Key differentiator callout mentions persistence', () => {
    const comparisonSection = document.querySelector('#comparison');
    expect(comparisonSection).not.toBeNull();

    // Find the key differentiator section
    const keyDifferentiator = comparisonSection.querySelector('h3');
    expect(keyDifferentiator).not.toBeNull();
    expect(keyDifferentiator.textContent.toLowerCase()).toContain('persistence');
  });

  it('Comparison table has MirDB and Memcached columns', () => {
    const comparisonSection = document.querySelector('#comparison');
    expect(comparisonSection).not.toBeNull();

    // Find the table header
    const tableHead = comparisonSection.querySelector('thead');
    expect(tableHead).not.toBeNull();

    const headerCells = tableHead.querySelectorAll('th');
    expect(headerCells.length).toBe(3);

    // Check column headers
    expect(headerCells[0].textContent.toLowerCase()).toContain('feature');
    expect(headerCells[1].textContent.toLowerCase()).toContain('mirdb');
    expect(headerCells[2].textContent.toLowerCase()).toContain('memcached');
  });
});
