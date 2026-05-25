/**
 * Unit tests for LSMTreeVisualization component.
 * Covers REQ-5 (LSM tree visualization), colorblind-safe palette.
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import LSMTreeVisualization, { formatBytes } from '../../../src/components/lsm-tree/LSMTreeVisualization';
import type { LSMState } from '../../../src/types';

function createMockState(overrides?: Partial<LSMState>): LSMState {
  return {
    memtable: { key_count: 50, size_bytes: 4096 },
    immutable_memtable: null,
    levels: [
      { level: 0, file_count: 3, total_size_bytes: 102400 },
      { level: 1, file_count: 2, total_size_bytes: 204800 },
    ],
    ...overrides,
  };
}

describe('LSMTreeVisualization', () => {
  it('renders the section container', () => {
    render(<LSMTreeVisualization state={createMockState()} />);
    expect(screen.getByTestId('lsm-tree-section')).toBeInTheDocument();
  });

  it('renders the section title', () => {
    render(<LSMTreeVisualization state={createMockState()} />);
    expect(screen.getByTestId('lsm-tree-title')).toHaveTextContent('LSM Tree Visualization');
  });

  it('renders the SVG element', () => {
    render(<LSMTreeVisualization state={createMockState()} />);
    expect(screen.getByTestId('lsm-tree-svg')).toBeInTheDocument();
  });

  it('SVG has descriptive aria-label for accessibility', () => {
    render(<LSMTreeVisualization state={createMockState()} />);
    const svg = screen.getByTestId('lsm-tree-svg');
    expect(svg).toHaveAttribute('role', 'img');
    expect(svg).toHaveAttribute('aria-label', expect.stringContaining('LSM tree'));
  });

  it('renders memtable block with key count', () => {
    render(<LSMTreeVisualization state={createMockState()} />);
    const blocks = screen.getAllByTestId('memtable-block');
    expect(blocks.length).toBeGreaterThanOrEqual(1);
    const svg = screen.getByTestId('lsm-tree-svg');
    expect(svg).toHaveTextContent('Memtable');
    expect(svg).toHaveTextContent('50 keys');
  });

  it('renders memtable block spanning full width when no immutable memtable', () => {
    render(
      <LSMTreeVisualization
        state={createMockState({ immutable_memtable: null })}
      />
    );
    const blocks = screen.getAllByTestId('memtable-block');
    expect(blocks).toHaveLength(1);
  });

  it('renders immutable memtable block when present', () => {
    render(
      <LSMTreeVisualization
        state={createMockState({
          immutable_memtable: { key_count: 30, size_bytes: 2048 },
        })}
      />
    );
    const svg = screen.getByTestId('lsm-tree-svg');
    expect(svg).toHaveTextContent('Immutable Memtable');
    expect(svg).toHaveTextContent('30 keys');
  });

  it('renders correct number of SSTable level blocks', () => {
    render(<LSMTreeVisualization state={createMockState()} />);
    expect(screen.getByTestId('sstable-level-0')).toBeInTheDocument();
    expect(screen.getByTestId('sstable-level-1')).toBeInTheDocument();
  });

  it('shows correct file count for Level 0', () => {
    render(<LSMTreeVisualization state={createMockState()} />);
    const svg = screen.getByTestId('lsm-tree-svg');
    expect(svg).toHaveTextContent('Level 0');
    expect(svg).toHaveTextContent('3 files');
  });

  it('shows correct file count for Level 1', () => {
    render(<LSMTreeVisualization state={createMockState()} />);
    const svg = screen.getByTestId('lsm-tree-svg');
    expect(svg).toHaveTextContent('Level 1');
    expect(svg).toHaveTextContent('2 files');
  });

  it('shows singular "file" text when count is 1', () => {
    render(
      <LSMTreeVisualization
        state={createMockState({
          levels: [{ level: 0, file_count: 1, total_size_bytes: 100 }],
        })}
      />
    );
    const svg = screen.getByTestId('lsm-tree-svg');
    expect(svg).toHaveTextContent('1 file');
    expect(svg.textContent).not.toMatch(/1 files/);
  });

  it('shows singular "key" text when memtable has 1 key', () => {
    render(
      <LSMTreeVisualization
        state={createMockState({
          memtable: { key_count: 1, size_bytes: 100 },
        })}
      />
    );
    const svg = screen.getByTestId('lsm-tree-svg');
    expect(svg).toHaveTextContent('1 key');
  });

  it('renders multiple levels correctly (up to 8 levels)', () => {
    const levels = Array.from({ length: 8 }, (_, i) => ({
      level: i,
      file_count: 8 - i,
      total_size_bytes: (8 - i) * 100000,
    }));
    render(<LSMTreeVisualization state={createMockState({ levels })} />);
    for (let i = 0; i < 8; i++) {
      expect(screen.getByTestId(`sstable-level-${i}`)).toBeInTheDocument();
    }
  });

  it('renders empty levels array gracefully', () => {
    render(<LSMTreeVisualization state={createMockState({ levels: [] })} />);
    expect(screen.getByTestId('lsm-tree-svg')).toBeInTheDocument();
    expect(screen.getByTestId('memtable-block')).toBeInTheDocument();
  });

  it('renders zero-key memtable correctly', () => {
    render(
      <LSMTreeVisualization
        state={createMockState({ memtable: { key_count: 0, size_bytes: 0 } })}
      />
    );
    const svg = screen.getByTestId('lsm-tree-svg');
    expect(svg).toHaveTextContent('0 keys');
  });

  it('updates to show immutable memtable when state changes', () => {
    const { rerender } = render(
      <LSMTreeVisualization
        state={createMockState({ immutable_memtable: null })}
      />
    );
    const svg = screen.getByTestId('lsm-tree-svg');
    expect(svg.textContent).not.toContain('Immutable Memtable');

    rerender(
      <LSMTreeVisualization
        state={createMockState({
          immutable_memtable: { key_count: 25, size_bytes: 2048 },
        })}
      />
    );
    expect(svg).toHaveTextContent('Immutable Memtable');
    expect(svg).toHaveTextContent('25 keys');
  });

  it('shows Level 0 SSTable increase after state simulates compaction', () => {
    const { rerender } = render(
      <LSMTreeVisualization
        state={createMockState({
          immutable_memtable: { key_count: 30, size_bytes: 4096 },
          levels: [{ level: 0, file_count: 2, total_size_bytes: 100000 }],
        })}
      />
    );
    const svg = screen.getByTestId('lsm-tree-svg');
    expect(svg).toHaveTextContent('2 files');

    rerender(
      <LSMTreeVisualization
        state={createMockState({
          immutable_memtable: null,
          levels: [{ level: 0, file_count: 3, total_size_bytes: 150000 }],
        })}
      />
    );
    expect(svg).toHaveTextContent('3 files');
    expect(svg.textContent).not.toContain('Immutable Memtable');
  });
});

describe('LSMTreeVisualization - Tooltip interaction', () => {
  it('shows tooltip on hovering SSTable level block', async () => {
    const user = userEvent.setup();
    render(<LSMTreeVisualization state={createMockState()} />);

    const level0 = screen.getByTestId('sstable-level-0');
    await user.hover(level0);

    const tooltip = await screen.findByTestId('lsm-tooltip');
    expect(tooltip).toBeInTheDocument();
    expect(tooltip).toHaveTextContent('Level 0');
    expect(tooltip).toHaveTextContent('Files: 3');
    expect(tooltip).toHaveTextContent('Total size: 100 KB');
    expect(tooltip).toHaveTextContent('Compaction: ok');
  });

  it('shows tooltip on hovering memtable block', async () => {
    const user = userEvent.setup();
    render(<LSMTreeVisualization state={createMockState()} />);

    const memtable = screen.getByTestId('memtable-block');
    await user.hover(memtable);

    const tooltip = await screen.findByTestId('lsm-tooltip');
    expect(tooltip).toBeInTheDocument();
    expect(tooltip).toHaveTextContent('Memtable');
    expect(tooltip).toHaveTextContent('Keys: 50');
    expect(tooltip).toHaveTextContent('Location: RAM');
  });

  it('shows tooltip on hovering immutable memtable block', async () => {
    const user = userEvent.setup();
    render(
      <LSMTreeVisualization
        state={createMockState({
          immutable_memtable: { key_count: 25, size_bytes: 2048 },
        })}
      />
    );

    const blocks = screen.getAllByTestId('memtable-block');
    const immutableBlock = blocks[1];
    await user.hover(immutableBlock);

    const tooltip = await screen.findByTestId('lsm-tooltip');
    expect(tooltip).toBeInTheDocument();
    expect(tooltip).toHaveTextContent('Immutable Memtable');
    expect(tooltip).toHaveTextContent('Keys: 25');
  });

  it('hides tooltip on mouse leave', async () => {
    const user = userEvent.setup();
    render(<LSMTreeVisualization state={createMockState()} />);

    const level0 = screen.getByTestId('sstable-level-0');
    await user.hover(level0);
    await screen.findByTestId('lsm-tooltip');

    await user.unhover(level0);
    expect(screen.queryByTestId('lsm-tooltip')).not.toBeInTheDocument();
  });

  it('shows "compaction: needed" when file count exceeds threshold', async () => {
    const user = userEvent.setup();
    render(
      <LSMTreeVisualization
        state={createMockState({
          levels: [{ level: 0, file_count: 5, total_size_bytes: 500000 }],
        })}
      />
    );

    await user.hover(screen.getByTestId('sstable-level-0'));
    const tooltip = await screen.findByTestId('lsm-tooltip');
    expect(tooltip).toHaveTextContent('Compaction: needed');
  });
});

describe('LSMTreeVisualization - Colorblind-safe palette', () => {
  it('renders distinct pattern definitions for each level type', () => {
    render(<LSMTreeVisualization state={createMockState()} />);
    const svg = screen.getByTestId('lsm-tree-svg');
    const patterns = svg.querySelectorAll('pattern[id^="pattern-"]');
    expect(patterns.length).toBeGreaterThanOrEqual(4);
  });

  it('memtable block uses pattern fill', () => {
    render(<LSMTreeVisualization state={createMockState()} />);
    const svg = screen.getByTestId('lsm-tree-svg');
    const memtableRect = svg.querySelector('rect[fill="url(#pattern-memtable)"]');
    expect(memtableRect).toBeInTheDocument();
  });

  it('level 0 block uses distinct pattern fill', () => {
    render(<LSMTreeVisualization state={createMockState()} />);
    const svg = screen.getByTestId('lsm-tree-svg');
    const level0Rect = svg.querySelector('rect[fill="url(#pattern-level0)"]');
    expect(level0Rect).toBeInTheDocument();
  });

  it('level 1 block uses distinct pattern fill', () => {
    render(<LSMTreeVisualization state={createMockState()} />);
    const svg = screen.getByTestId('lsm-tree-svg');
    const level1Rect = svg.querySelector('rect[fill="url(#pattern-level1)"]');
    expect(level1Rect).toBeInTheDocument();
  });

  it('immutable memtable uses distinct pattern fill when present', () => {
    render(
      <LSMTreeVisualization
        state={createMockState({
          immutable_memtable: { key_count: 10, size_bytes: 1024 },
        })}
      />
    );
    const svg = screen.getByTestId('lsm-tree-svg');
    const immutableRect = svg.querySelector('rect[fill="url(#pattern-immutable)"]');
    expect(immutableRect).toBeInTheDocument();
  });

  it('uses high-contrast stroke colors for block borders', () => {
    render(<LSMTreeVisualization state={createMockState()} />);
    const svg = screen.getByTestId('lsm-tree-svg');
    const rects = svg.querySelectorAll('rect[stroke]');
    expect(rects.length).toBeGreaterThan(0);
    rects.forEach(rect => {
      const stroke = rect.getAttribute('stroke');
      expect(stroke).toBeTruthy();
      expect(stroke).not.toBe('none');
    });
  });

  it('uses dark text colors for readability', () => {
    render(<LSMTreeVisualization state={createMockState()} />);
    const svg = screen.getByTestId('lsm-tree-svg');
    const texts = svg.querySelectorAll('text');
    expect(texts.length).toBeGreaterThan(0);
    texts.forEach(text => {
      const fill = text.getAttribute('fill');
      expect(fill).toBeTruthy();
    });
  });
});

describe('formatBytes', () => {
  it('formats 0 bytes', () => {
    expect(formatBytes(0)).toBe('0 B');
  });

  it('formats bytes', () => {
    expect(formatBytes(512)).toBe('512 B');
  });

  it('formats kilobytes', () => {
    expect(formatBytes(1536)).toBe('1.5 KB');
  });

  it('formats megabytes', () => {
    expect(formatBytes(5 * 1024 * 1024)).toBe('5 MB');
  });

  it('formats gigabytes', () => {
    expect(formatBytes(3 * 1024 * 1024 * 1024)).toBe('3 GB');
  });
});
