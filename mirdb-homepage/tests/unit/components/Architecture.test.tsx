/**
 * Unit tests for Architecture section components.
 * Owner: Scenario 4 - Architecture Visualization
 *
 * Tests cover:
 * - Test Case 1: Diagram (Mermaid or SVG) is displayed showing LSM-tree architecture
 * - Test Case 2: Diagram shows WAL → Memtable → Immutable Memtable → SSTable flow (write path)
 * - Test Case 3: Diagram shows Memtable → SSTable levels (L0-L7) read path
 * - Test Case 5: Diagram lazy loading (integration test pattern - unit verification)
 * - Component descriptions and accessibility
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, waitFor, act } from '@testing-library/react';
import { ArchitectureDiagram, LSMTreeSVG } from '../../../src/components/Architecture/ArchitectureDiagram';

describe('LSMTreeSVG', () => {
  describe('Test Case 1: Diagram (SVG) is displayed showing LSM-tree architecture', () => {
    it('renders the SVG diagram with correct structure', () => {
      render(<LSMTreeSVG />);

      // Check that the SVG container is rendered
      const svgContainer = screen.getByTestId('lsm-tree-svg');
      expect(svgContainer).toBeInTheDocument();

      // Check that the SVG element is present
      const svg = screen.getByTestId('architecture-svg');
      expect(svg).toBeInTheDocument();
      expect(svg.tagName.toLowerCase()).toBe('svg');
    });

    it('has proper accessibility attributes', () => {
      render(<LSMTreeSVG />);

      const svg = screen.getByTestId('architecture-svg');
      expect(svg).toHaveAttribute('role', 'img');

      // Check for title and description for screen readers
      const title = document.getElementById('lsm-tree-title');
      expect(title).toBeInTheDocument();
      expect(title).toHaveTextContent('LSM-Tree Architecture Diagram');

      const desc = document.getElementById('lsm-tree-desc');
      expect(desc).toBeInTheDocument();
      expect(desc?.textContent).toContain('write path');
      expect(desc?.textContent).toContain('read path');
      expect(desc?.textContent).toContain('L0-L7');
    });

    it('displays all LSM-tree components', () => {
      render(<LSMTreeSVG />);

      // Check for WAL component
      const wal = screen.getByTestId('component-wal');
      expect(wal).toBeInTheDocument();

      // Check for Memtable component
      const memtable = screen.getByTestId('component-memtable');
      expect(memtable).toBeInTheDocument();

      // Check for Immutable Memtable component
      const immutableMemtable = screen.getByTestId('component-immutable-memtable');
      expect(immutableMemtable).toBeInTheDocument();

      // Check for SSTable levels
      const l0 = screen.getByTestId('component-sstable-l0');
      expect(l0).toBeInTheDocument();

      const l1 = screen.getByTestId('component-sstable-l1');
      expect(l1).toBeInTheDocument();

      const l2l7 = screen.getByTestId('component-sstable-l2-l7');
      expect(l2l7).toBeInTheDocument();
    });

    it('displays diagram legend', () => {
      render(<LSMTreeSVG />);

      const legend = screen.getByTestId('diagram-legend');
      expect(legend).toBeInTheDocument();
    });
  });

  describe('Test Case 2: Diagram shows WAL → Memtable → Immutable Memtable → SSTable flow (write path)', () => {
    it('shows write path label', () => {
      render(<LSMTreeSVG />);

      const writePathLabel = screen.getByTestId('write-path-label');
      expect(writePathLabel).toBeInTheDocument();
      expect(writePathLabel).toHaveTextContent('Write Request');
    });

    it('shows arrow from WAL to Memtable', () => {
      render(<LSMTreeSVG />);

      const walToMemtable = screen.getByTestId('wal-to-memtable-arrow');
      expect(walToMemtable).toBeInTheDocument();
      expect(walToMemtable.tagName.toLowerCase()).toBe('line');
    });

    it('shows arrow from Memtable to Immutable Memtable', () => {
      render(<LSMTreeSVG />);

      const memtableToImmutable = screen.getByTestId('memtable-to-immutable-arrow');
      expect(memtableToImmutable).toBeInTheDocument();
    });

    it('shows arrow from Immutable Memtable to SSTable', () => {
      render(<LSMTreeSVG />);

      const immutableToSstable = screen.getByTestId('immutable-to-sstable-arrow');
      expect(immutableToSstable).toBeInTheDocument();
    });

    it('write path elements have correct color (green)', () => {
      render(<LSMTreeSVG />);

      const walToMemtable = screen.getByTestId('wal-to-memtable-arrow');
      expect(walToMemtable).toHaveAttribute('stroke', '#3fb950');

      const writeLabel = screen.getByTestId('write-path-label');
      expect(writeLabel).toHaveAttribute('fill', '#3fb950');
    });
  });

  describe('Test Case 3: Diagram shows Memtable → SSTable levels (L0-L7) read path', () => {
    it('shows read path elements', () => {
      render(<LSMTreeSVG />);

      const readPath = screen.getByTestId('read-path');
      expect(readPath).toBeInTheDocument();
    });

    it('shows SSTable levels label', () => {
      render(<LSMTreeSVG />);

      const sstableLevelsLabel = screen.getByTestId('sstable-levels-label');
      expect(sstableLevelsLabel).toBeInTheDocument();
      expect(sstableLevelsLabel).toHaveTextContent('SSTable Levels');
    });

    it('shows compaction arrows between levels', () => {
      render(<LSMTreeSVG />);

      const compactionL0L1 = screen.getByTestId('compaction-l0-l1');
      expect(compactionL0L1).toBeInTheDocument();

      const compactionL1L2 = screen.getByTestId('compaction-l1-l2');
      expect(compactionL1L2).toBeInTheDocument();
    });

    it('displays size indicator showing level size progression', () => {
      render(<LSMTreeSVG />);

      const sizeIndicator = screen.getByTestId('size-indicator');
      expect(sizeIndicator).toBeInTheDocument();
    });
  });
});

describe('ArchitectureDiagram', () => {
  let mockIntersectionObserver: ReturnType<typeof vi.fn>;
  let observerCallback: (entries: Array<{ isIntersecting: boolean }>) => void;

  beforeEach(() => {
    // Mock IntersectionObserver
    mockIntersectionObserver = vi.fn().mockImplementation((callback: (entries: Array<{ isIntersecting: boolean }>) => void) => {
      observerCallback = callback;
      return {
        observe: vi.fn(),
        disconnect: vi.fn(),
        unobserve: vi.fn(),
      };
    });
    window.IntersectionObserver = mockIntersectionObserver as unknown as typeof IntersectionObserver;
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('Test Case 1: Render Architecture component', () => {
    it('renders the architecture section', async () => {
      render(<ArchitectureDiagram />);

      // Simulate intersection
      act(() => {
        observerCallback([{ isIntersecting: true }]);
      });

      const section = screen.getByTestId('architecture-section');
      expect(section).toBeInTheDocument();
    });

    it('has correct section heading', async () => {
      render(<ArchitectureDiagram />);

      act(() => {
        observerCallback([{ isIntersecting: true }]);
      });

      const heading = screen.getByTestId('architecture-heading');
      expect(heading).toBeInTheDocument();
      expect(heading).toHaveTextContent('Architecture');
      expect(heading.tagName.toLowerCase()).toBe('h2');
    });

    it('has intro paragraph describing LSM-tree', async () => {
      render(<ArchitectureDiagram />);

      act(() => {
        observerCallback([{ isIntersecting: true }]);
      });

      const intro = screen.getByTestId('architecture-intro');
      expect(intro).toBeInTheDocument();
      expect(intro.textContent).toContain('LSM-tree');
    });

    it('renders the SVG diagram when loaded', async () => {
      render(<ArchitectureDiagram />);

      // Initially should show placeholder or be loading
      const container = screen.getByTestId('diagram-container');
      expect(container).toHaveAttribute('data-loaded', 'false');

      // Simulate intersection
      act(() => {
        observerCallback([{ isIntersecting: true }]);
      });

      await waitFor(() => {
        expect(container).toHaveAttribute('data-loaded', 'true');
      });

      // SVG should now be visible
      const diagramContent = screen.getByTestId('diagram-content');
      expect(diagramContent).toBeInTheDocument();
    });
  });

  describe('Test Case 4 (Component Descriptions): Check text descriptions accompany the diagram', () => {
    it('renders all component description cards', async () => {
      render(<ArchitectureDiagram />);

      act(() => {
        observerCallback([{ isIntersecting: true }]);
      });

      const descriptionsContainer = screen.getByTestId('component-descriptions');
      expect(descriptionsContainer).toBeInTheDocument();

      // Check for WAL description
      const walDesc = screen.getByTestId('description-wal');
      expect(walDesc).toBeInTheDocument();

      // Check for Memtable description
      const memtableDesc = screen.getByTestId('description-memtable');
      expect(memtableDesc).toBeInTheDocument();

      // Check for Immutable Memtable description
      const immutableDesc = screen.getByTestId('description-immutable-memtable');
      expect(immutableDesc).toBeInTheDocument();

      // Check for SSTable description
      const sstableDesc = screen.getByTestId('description-sstable');
      expect(sstableDesc).toBeInTheDocument();

      // Check for Compaction description
      const compactionDesc = screen.getByTestId('description-compaction');
      expect(compactionDesc).toBeInTheDocument();

      // Check for Read/Write paths description
      const pathsDesc = screen.getByTestId('description-paths');
      expect(pathsDesc).toBeInTheDocument();
    });

    it('WAL description explains write-ahead logging', async () => {
      render(<ArchitectureDiagram />);

      act(() => {
        observerCallback([{ isIntersecting: true }]);
      });

      const walTitle = screen.getByTestId('description-wal-title');
      expect(walTitle).toHaveTextContent('Write-Ahead Log (WAL)');

      const walDescription = screen.getByTestId('description-wal-description');
      expect(walDescription.textContent?.toLowerCase()).toContain('durability');
    });

    it('Memtable description explains skip list and in-memory operations', async () => {
      render(<ArchitectureDiagram />);

      act(() => {
        observerCallback([{ isIntersecting: true }]);
      });

      const memtableDescription = screen.getByTestId('description-memtable-description');
      expect(memtableDescription.textContent?.toLowerCase()).toContain('skip list');
      expect(memtableDescription.textContent?.toLowerCase()).toContain('in-memory');
    });

    it('Compaction description explains minor and major compaction', async () => {
      render(<ArchitectureDiagram />);

      act(() => {
        observerCallback([{ isIntersecting: true }]);
      });

      const compactionDescription = screen.getByTestId('description-compaction-description');
      expect(compactionDescription.textContent?.toLowerCase()).toContain('minor compaction');
      expect(compactionDescription.textContent?.toLowerCase()).toContain('major compaction');
    });

    it('Paths description explains write and read paths', async () => {
      render(<ArchitectureDiagram />);

      act(() => {
        observerCallback([{ isIntersecting: true }]);
      });

      const pathsDescription = screen.getByTestId('description-paths-description');
      const text = pathsDescription.textContent?.toLowerCase() || '';
      expect(text).toContain('wal');
      expect(text).toContain('memtable');
      expect(text).toContain('sstable');
    });
  });

  describe('SSTable levels explanation', () => {
    it('renders levels explanation section', async () => {
      render(<ArchitectureDiagram />);

      act(() => {
        observerCallback([{ isIntersecting: true }]);
      });

      const levelsExplanation = screen.getByTestId('levels-explanation');
      expect(levelsExplanation).toBeInTheDocument();

      const levelsHeading = screen.getByTestId('levels-heading');
      expect(levelsHeading).toHaveTextContent('SSTable Levels (L0-L7)');
    });

    it('explains L0 level characteristics', async () => {
      render(<ArchitectureDiagram />);

      act(() => {
        observerCallback([{ isIntersecting: true }]);
      });

      const l0Desc = screen.getByTestId('levels-l0-desc');
      expect(l0Desc.textContent?.toLowerCase()).toContain('level 0');
      expect(l0Desc.textContent?.toLowerCase()).toContain('overlapping');
    });

    it('explains size ratio between levels', async () => {
      render(<ArchitectureDiagram />);

      act(() => {
        observerCallback([{ isIntersecting: true }]);
      });

      const sizeRatio = screen.getByTestId('levels-size-ratio');
      expect(sizeRatio.textContent?.toLowerCase()).toContain('10x larger');
    });
  });

  describe('Test Case 5: Diagram lazy loading (unit verification)', () => {
    it('shows placeholder before intersection', () => {
      render(<ArchitectureDiagram />);

      const container = screen.getByTestId('diagram-container');
      expect(container).toHaveAttribute('data-loaded', 'false');

      // Placeholder should be visible
      const placeholder = screen.getByTestId('diagram-placeholder');
      expect(placeholder).toBeInTheDocument();
    });

    it('loads diagram content after intersection', async () => {
      render(<ArchitectureDiagram />);

      // Initially not loaded
      const container = screen.getByTestId('diagram-container');
      expect(container).toHaveAttribute('data-loaded', 'false');

      // Simulate intersection
      act(() => {
        observerCallback([{ isIntersecting: true }]);
      });

      await waitFor(() => {
        expect(container).toHaveAttribute('data-loaded', 'true');
      });

      // Diagram content should now be visible
      const diagramContent = screen.getByTestId('diagram-content');
      expect(diagramContent).toBeInTheDocument();
    });

    it('IntersectionObserver is initialized with correct options', () => {
      render(<ArchitectureDiagram />);

      expect(mockIntersectionObserver).toHaveBeenCalledWith(
        expect.any(Function),
        {
          rootMargin: '100px 0px',
          threshold: 0.1,
        }
      );
    });

    it('disconnects observer after intersection', async () => {
      const disconnectMock = vi.fn();
      mockIntersectionObserver.mockImplementation((callback: (entries: Array<{ isIntersecting: boolean }>) => void) => {
        observerCallback = callback;
        return {
          observe: vi.fn(),
          disconnect: disconnectMock,
          unobserve: vi.fn(),
        };
      });

      render(<ArchitectureDiagram />);

      act(() => {
        observerCallback([{ isIntersecting: true }]);
      });

      expect(disconnectMock).toHaveBeenCalled();
    });
  });

  describe('Accessibility', () => {
    it('has proper section aria attributes', async () => {
      render(<ArchitectureDiagram />);

      act(() => {
        observerCallback([{ isIntersecting: true }]);
      });

      const section = screen.getByTestId('architecture-section');
      expect(section).toHaveAttribute('aria-labelledby', 'architecture-heading');
    });

    it('has proper heading hierarchy', async () => {
      render(<ArchitectureDiagram />);

      act(() => {
        observerCallback([{ isIntersecting: true }]);
      });

      const h2 = screen.getByRole('heading', { level: 2 });
      expect(h2).toHaveTextContent('Architecture');

      const h3s = screen.getAllByRole('heading', { level: 3 });
      expect(h3s.length).toBeGreaterThan(0);
    });
  });

  describe('Fallback behavior', () => {
    it('shows diagram immediately when IntersectionObserver is not supported', () => {
      // Remove IntersectionObserver from window
      const originalIO = window.IntersectionObserver;
      // @ts-expect-error - intentionally removing for test
      delete window.IntersectionObserver;

      render(<ArchitectureDiagram />);

      const container = screen.getByTestId('diagram-container');
      expect(container).toHaveAttribute('data-loaded', 'true');

      // Restore
      window.IntersectionObserver = originalIO;
    });
  });
});
