/**
 * Unit tests for Architecture component.
 * Owner: Scenario 4 - Architecture Visualization
 *
 * Tests the LSM tree diagram rendering including:
 * - WAL, Memtable, Immutable Memtables, SSTable levels
 * - Data flow arrows
 * - Component labels
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { renderArchitecture } from '../../../src/components/Architecture';

describe('Architecture Component', () => {
  let section: HTMLElement;

  beforeEach(() => {
    section = renderArchitecture();
  });

  it('renders a section element with proper id', () => {
    expect(section.tagName).toBe('SECTION');
    expect(section.id).toBe('architecture');
    expect(section.className).toContain('architecture-section');
  });

  it('contains LSM tree diagram', () => {
    const diagram = section.querySelector('.arch-diagram');
    expect(diagram).not.toBeNull();
    expect(diagram?.getAttribute('role')).toBe('figure');
    expect(diagram?.getAttribute('aria-label')).toContain('LSM Tree');
  });

  it('shows WAL (Write-Ahead Log) component', () => {
    const walNode = section.querySelector('[data-component="wal"]');
    expect(walNode).not.toBeNull();
    expect(walNode?.className).toContain('arch-wal');

    const label = walNode?.querySelector('.arch-label');
    expect(label?.textContent).toBe('WAL');

    const sublabel = walNode?.querySelector('.arch-sublabel');
    expect(sublabel?.textContent).toContain('Write-Ahead Log');
  });

  it('shows Memtable component', () => {
    const memtableNode = section.querySelector('[data-component="memtable"]');
    expect(memtableNode).not.toBeNull();
    expect(memtableNode?.className).toContain('arch-memtable');

    const label = memtableNode?.querySelector('.arch-label');
    expect(label?.textContent).toBe('Memtable');

    const sublabel = memtableNode?.querySelector('.arch-sublabel');
    expect(sublabel?.textContent).toContain('Skip List');
  });

  it('shows Immutable Memtables component', () => {
    const immutableNode = section.querySelector('[data-component="immutable"]');
    expect(immutableNode).not.toBeNull();
    expect(immutableNode?.className).toContain('arch-immutable');

    const label = immutableNode?.querySelector('.arch-label');
    expect(label?.textContent).toContain('Immutable');
  });

  it('shows SSTable levels (L0, L1, L2)', () => {
    const l0Node = section.querySelector('[data-component="sstable-l0"]');
    const l1Node = section.querySelector('[data-component="sstable-l1"]');
    const l2Node = section.querySelector('[data-component="sstable-l2"]');

    expect(l0Node).not.toBeNull();
    expect(l1Node).not.toBeNull();
    expect(l2Node).not.toBeNull();

    // Check labels
    expect(l0Node?.querySelector('.arch-label')?.textContent).toBe('L0');
    expect(l1Node?.querySelector('.arch-label')?.textContent).toBe('L1');
    expect(l2Node?.querySelector('.arch-label')?.textContent).toBe('L2');

    // Check sublabels contain SSTable
    expect(l0Node?.querySelector('.arch-sublabel')?.textContent).toContain('SSTable');
    expect(l1Node?.querySelector('.arch-sublabel')?.textContent).toContain('SSTable');
    expect(l2Node?.querySelector('.arch-sublabel')?.textContent).toContain('SSTable');

    // Check CSS classes
    expect(l0Node?.className).toContain('arch-sstable');
    expect(l1Node?.className).toContain('arch-sstable');
    expect(l2Node?.className).toContain('arch-sstable');
  });

  it('shows data flow arrows between components', () => {
    const arrows = section.querySelectorAll('.arch-arrow');
    expect(arrows.length).toBeGreaterThanOrEqual(4); // Write->WAL, WAL->Memtable, Memtable->Immutable, Immutable->L0, L0->L1, L1->L2

    // Check for specific arrows
    const walMemtableArrow = section.querySelector('.arch-arrow-wal-memtable');
    expect(walMemtableArrow).not.toBeNull();

    const memtableImmutableArrow = section.querySelector('.arch-arrow-memtable-immutable');
    expect(memtableImmutableArrow).not.toBeNull();

    // Check flush to disk arrow
    const flushDiskArrow = section.querySelector('.arch-arrow-flush-disk');
    expect(flushDiskArrow).not.toBeNull();
  });

  it('all components have clear labels', () => {
    const nodes = section.querySelectorAll('.arch-node');
    expect(nodes.length).toBeGreaterThanOrEqual(6); // WAL, Memtable, Immutable, L0, L1, L2

    nodes.forEach((node) => {
      const label = node.querySelector('.arch-label');
      expect(label).not.toBeNull();
      expect(label?.textContent?.trim().length).toBeGreaterThan(0);

      // Each node should have a sublabel for additional context
      const sublabel = node.querySelector('.arch-sublabel');
      expect(sublabel).not.toBeNull();
    });
  });

  it('has proper heading structure', () => {
    const heading = section.querySelector('h2');
    expect(heading).not.toBeNull();
    expect(heading?.id).toBe('architecture-heading');
    expect(heading?.textContent).toBe('Architecture');
  });

  it('has accessible aria attributes', () => {
    expect(section.getAttribute('aria-labelledby')).toBe('architecture-heading');

    // Diagram has role and aria-label
    const diagram = section.querySelector('.arch-diagram');
    expect(diagram?.getAttribute('role')).toBe('figure');
    expect(diagram?.getAttribute('aria-label')).toBeTruthy();

    // Nodes have role and aria-label
    const nodes = section.querySelectorAll('.arch-node');
    nodes.forEach((node) => {
      expect(node.getAttribute('role')).toBe('img');
      expect(node.getAttribute('aria-label')).toBeTruthy();
    });
  });

  it('includes a description of the architecture', () => {
    const description = section.querySelector('.architecture-description');
    expect(description).not.toBeNull();
    expect(description?.textContent).toContain('LSM');
  });

  it('has memory and disk sections', () => {
    const memorySection = section.querySelector('.arch-memory-section');
    const diskSection = section.querySelector('.arch-disk-section');

    expect(memorySection).not.toBeNull();
    expect(diskSection).not.toBeNull();

    // Memory section should contain WAL, Memtable, and Immutable
    expect(memorySection?.querySelector('[data-component="wal"]')).not.toBeNull();
    expect(memorySection?.querySelector('[data-component="memtable"]')).not.toBeNull();
    expect(memorySection?.querySelector('[data-component="immutable"]')).not.toBeNull();

    // Disk section should contain SSTables
    expect(diskSection?.querySelector('[data-component="sstable-l0"]')).not.toBeNull();
    expect(diskSection?.querySelector('[data-component="sstable-l1"]')).not.toBeNull();
    expect(diskSection?.querySelector('[data-component="sstable-l2"]')).not.toBeNull();
  });

  it('has a legend explaining the diagram', () => {
    const legend = section.querySelector('.arch-legend');
    expect(legend).not.toBeNull();

    const legendItems = legend?.querySelectorAll('.arch-legend-item');
    expect(legendItems?.length).toBeGreaterThanOrEqual(2);

    // Check for memory and disk legend items
    const legendLabels = Array.from(legend?.querySelectorAll('.arch-legend-label') || [])
      .map((el) => el.textContent);
    expect(legendLabels.some((label) => label?.includes('Memory'))).toBe(true);
    expect(legendLabels.some((label) => label?.includes('Disk'))).toBe(true);
  });

  it('shows write entry point', () => {
    const writeEntry = section.querySelector('.arch-write-entry');
    expect(writeEntry).not.toBeNull();
    expect(writeEntry?.textContent).toContain('Write');
  });

  it('shows compaction labels on arrows', () => {
    const arrowLabels = Array.from(section.querySelectorAll('.arch-arrow-label'))
      .map((el) => el.textContent);

    // Should have flush and compaction labels
    expect(arrowLabels.some((label) => label?.includes('Flush'))).toBe(true);
    expect(arrowLabels.some((label) => label?.includes('Compaction'))).toBe(true);
  });

  it('uses container class for proper layout', () => {
    const container = section.querySelector('.container');
    expect(container).not.toBeNull();
    expect(container?.contains(section.querySelector('h2'))).toBe(true);
    expect(container?.contains(section.querySelector('.arch-diagram'))).toBe(true);
  });
});
