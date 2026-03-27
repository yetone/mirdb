/**
 * Architecture Section Unit Tests
 * Owner: Scenario 8 - Architecture Overview Section
 *
 * Tests:
 * - LSM tree explanation content
 * - Memtable explanation content
 * - SSTable explanation content
 * - Architecture diagram presence
 */
import { describe, it, expect, beforeAll } from 'vitest';
import { readFileSync } from 'fs';
import { JSDOM } from 'jsdom';
import { resolve } from 'path';

describe('Architecture Overview Section', () => {
  let document;
  let architectureSection;

  beforeAll(() => {
    const htmlPath = resolve(process.cwd(), 'src/index.html');
    const html = readFileSync(htmlPath, 'utf-8');
    const dom = new JSDOM(html);
    document = dom.window.document;
    architectureSection = document.querySelector('#architecture');
  });

  it('TC1: Architecture section exists with heading', () => {
    expect(architectureSection).not.toBeNull();

    const heading = architectureSection.querySelector('h2');
    expect(heading).not.toBeNull();
    expect(heading.textContent.toLowerCase()).toContain('architecture');
  });

  it('TC2: LSM tree explanation exists and explains Log-Structured Merge-tree', () => {
    const lsmComponent = architectureSection.querySelector('[data-component="lsm-tree"]');
    expect(lsmComponent).not.toBeNull();

    const explanation = lsmComponent.querySelector('[data-content="lsm-tree-explanation"]');
    expect(explanation).not.toBeNull();

    const text = explanation.textContent.toLowerCase();
    // Should explain LSM tree concept
    expect(text).toContain('log-structured merge-tree');
    // Should mention write optimization
    expect(text).toMatch(/write|buffer|flush/i);
  });

  it('TC3: Memtable explanation exists and describes in-memory write buffer', () => {
    const memtableComponent = architectureSection.querySelector('[data-component="memtable"]');
    expect(memtableComponent).not.toBeNull();

    const explanation = memtableComponent.querySelector('[data-content="memtable-explanation"]');
    expect(explanation).not.toBeNull();

    const text = explanation.textContent.toLowerCase();
    // Should describe memtable as in-memory buffer
    expect(text).toContain('in-memory');
    expect(text).toMatch(/write|buffer/i);
    // Should mention skip list or similar data structure
    expect(text).toMatch(/skip list|data structure/i);
  });

  it('TC4: SSTable explanation exists and describes sorted string table for persistence', () => {
    const sstableComponent = architectureSection.querySelector('[data-component="sstable"]');
    expect(sstableComponent).not.toBeNull();

    const explanation = sstableComponent.querySelector('[data-content="sstable-explanation"]');
    expect(explanation).not.toBeNull();

    const text = explanation.textContent.toLowerCase();
    // Should explain SSTable as sorted string table
    expect(text).toMatch(/sorted string table|sstable/i);
    // Should mention disk/persistence
    expect(text).toMatch(/disk|persist|immutable|file/i);
  });

  it('TC5: Architecture diagram exists showing data flow', () => {
    const diagram = architectureSection.querySelector('.architecture-diagram');
    expect(diagram).not.toBeNull();

    // Should have role="img" for accessibility
    expect(diagram.getAttribute('role')).toBe('img');

    // Should have aria-label
    const ariaLabel = diagram.getAttribute('aria-label');
    expect(ariaLabel).not.toBeNull();
    expect(ariaLabel.toLowerCase()).toContain('architecture');

    // Should contain key components in the diagram
    const diagramText = diagram.textContent;
    expect(diagramText).toContain('WRITE REQUEST');
    expect(diagramText).toContain('MEMTABLE');
    expect(diagramText).toContain('SSTABLE');
    expect(diagramText).toContain('WAL');
  });

  it('WAL explanation exists and describes durability', () => {
    const walComponent = architectureSection.querySelector('[data-component="wal"]');
    expect(walComponent).not.toBeNull();

    const explanation = walComponent.querySelector('[data-content="wal-explanation"]');
    expect(explanation).not.toBeNull();

    const text = explanation.textContent.toLowerCase();
    // Should explain WAL role in durability
    expect(text).toMatch(/durability|recovery|crash/i);
    // Should mention persistence before acknowledgment
    expect(text).toMatch(/persist|write.*disk|log/i);
  });

  it('Architecture section has section-level aria-labelledby', () => {
    const ariaLabelledBy = architectureSection.getAttribute('aria-labelledby');
    expect(ariaLabelledBy).toBe('architecture-heading');

    const heading = document.querySelector('#architecture-heading');
    expect(heading).not.toBeNull();
  });

  it('Architecture diagram shows read and write paths', () => {
    // Check for data paths section
    const dataPathsSection = architectureSection.querySelector('.grid.md\\:grid-cols-2');
    expect(dataPathsSection).not.toBeNull();

    const sectionText = architectureSection.textContent;
    // Should explain write path
    expect(sectionText).toMatch(/write path/i);
    // Should explain read path
    expect(sectionText).toMatch(/read path/i);
  });
});
