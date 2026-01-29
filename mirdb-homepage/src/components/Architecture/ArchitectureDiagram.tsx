/**
 * React version of ArchitectureDiagram for testing purposes.
 * This mirrors the ArchitectureDiagram.astro component structure.
 * Owner: Scenario 4 - Architecture Visualization
 */

import { useEffect, useRef, useState } from 'react';

interface LSMTreeSVGProps {
  className?: string;
}

export function LSMTreeSVG({ className = '' }: LSMTreeSVGProps) {
  return (
    <div className={`lsm-tree-svg ${className}`} data-testid="lsm-tree-svg">
      <svg
        viewBox="0 0 800 600"
        xmlns="http://www.w3.org/2000/svg"
        className="w-full h-auto max-w-4xl mx-auto"
        role="img"
        aria-labelledby="lsm-tree-title lsm-tree-desc"
        data-testid="architecture-svg"
      >
        <title id="lsm-tree-title">LSM-Tree Architecture Diagram</title>
        <desc id="lsm-tree-desc">
          Diagram showing the LSM-tree architecture with write path from WAL through Memtable to SSTables, and read path across SSTable levels L0-L7.
        </desc>

        <defs>
          {/* Arrow markers */}
          <marker
            id="arrowhead"
            markerWidth="10"
            markerHeight="7"
            refX="9"
            refY="3.5"
            orient="auto"
          >
            <polygon points="0 0, 10 3.5, 0 7" fill="#58a6ff" />
          </marker>
          <marker
            id="arrowhead-write"
            markerWidth="10"
            markerHeight="7"
            refX="9"
            refY="3.5"
            orient="auto"
          >
            <polygon points="0 0, 10 3.5, 0 7" fill="#3fb950" />
          </marker>
          <marker
            id="arrowhead-read"
            markerWidth="10"
            markerHeight="7"
            refX="9"
            refY="3.5"
            orient="auto"
          >
            <polygon points="0 0, 10 3.5, 0 7" fill="#d29922" />
          </marker>
          <marker
            id="arrowhead-compact"
            markerWidth="10"
            markerHeight="7"
            refX="9"
            refY="3.5"
            orient="auto"
          >
            <polygon points="0 0, 10 3.5, 0 7" fill="#8b949e" />
          </marker>
        </defs>

        {/* Background */}
        <rect width="800" height="600" fill="transparent" />

        {/* Legend */}
        <g transform="translate(600, 20)" className="legend" data-testid="diagram-legend">
          <text x="0" y="0" fill="#c9d1d9" fontSize="12" fontWeight="bold">Legend</text>
          <line x1="0" y1="15" x2="30" y2="15" stroke="#3fb950" strokeWidth="2" markerEnd="url(#arrowhead-write)" />
          <text x="40" y="20" fill="#c9d1d9" fontSize="11">Write Path</text>
          <line x1="0" y1="35" x2="30" y2="35" stroke="#d29922" strokeWidth="2" markerEnd="url(#arrowhead-read)" />
          <text x="40" y="40" fill="#c9d1d9" fontSize="11">Read Path</text>
          <line x1="0" y1="55" x2="30" y2="55" stroke="#8b949e" strokeWidth="2" strokeDasharray="4,2" markerEnd="url(#arrowhead-compact)" />
          <text x="40" y="60" fill="#c9d1d9" fontSize="11">Compaction</text>
        </g>

        {/* Write Request Label */}
        <text x="40" y="50" fill="#3fb950" fontSize="14" fontWeight="bold" data-write-path="true" data-testid="write-path-label">
          Write Request
        </text>
        <line x1="40" y1="60" x2="40" y2="90" stroke="#3fb950" strokeWidth="2" markerEnd="url(#arrowhead-write)" data-write-path="true" />

        {/* WAL (Write-Ahead Log) */}
        <g transform="translate(20, 100)" data-component="wal" data-testid="component-wal">
          <rect
            width="120"
            height="60"
            rx="8"
            fill="#161b22"
            stroke="#3fb950"
            strokeWidth="2"
          />
          <text x="60" y="25" fill="#c9d1d9" fontSize="14" fontWeight="bold" textAnchor="middle">
            WAL
          </text>
          <text x="60" y="45" fill="#8b949e" fontSize="10" textAnchor="middle">
            Write-Ahead Log
          </text>
        </g>

        {/* Arrow from WAL to Memtable */}
        <line x1="140" y1="130" x2="190" y2="130" stroke="#3fb950" strokeWidth="2" markerEnd="url(#arrowhead-write)" data-write-path="true" data-testid="wal-to-memtable-arrow" />

        {/* Memtable */}
        <g transform="translate(200, 100)" data-component="memtable" data-testid="component-memtable">
          <rect
            width="140"
            height="60"
            rx="8"
            fill="#161b22"
            stroke="#3fb950"
            strokeWidth="2"
          />
          <text x="70" y="25" fill="#c9d1d9" fontSize="14" fontWeight="bold" textAnchor="middle">
            Memtable
          </text>
          <text x="70" y="45" fill="#8b949e" fontSize="10" textAnchor="middle">
            Skip List (in-memory)
          </text>
        </g>

        {/* Arrow from Memtable to Immutable Memtable */}
        <line x1="340" y1="130" x2="390" y2="130" stroke="#3fb950" strokeWidth="2" markerEnd="url(#arrowhead-write)" data-write-path="true" data-testid="memtable-to-immutable-arrow" />
        <text x="365" y="120" fill="#8b949e" fontSize="9" textAnchor="middle">
          freeze
        </text>

        {/* Immutable Memtable */}
        <g transform="translate(400, 100)" data-component="immutable-memtable" data-testid="component-immutable-memtable">
          <rect
            width="160"
            height="60"
            rx="8"
            fill="#161b22"
            stroke="#58a6ff"
            strokeWidth="2"
            strokeDasharray="5,3"
          />
          <text x="80" y="25" fill="#c9d1d9" fontSize="14" fontWeight="bold" textAnchor="middle">
            Immutable
          </text>
          <text x="80" y="45" fill="#8b949e" fontSize="10" textAnchor="middle">
            Memtable (frozen)
          </text>
        </g>

        {/* Arrow from Immutable Memtable to SSTable L0 */}
        <path d="M 480 160 L 480 200 L 200 200 L 200 240" fill="none" stroke="#3fb950" strokeWidth="2" markerEnd="url(#arrowhead-write)" data-write-path="true" data-testid="immutable-to-sstable-arrow" />
        <text x="350" y="215" fill="#8b949e" fontSize="9" textAnchor="middle">
          flush to disk
        </text>

        {/* SSTable Levels Section */}
        <text x="400" y="270" fill="#c9d1d9" fontSize="16" fontWeight="bold" textAnchor="middle" data-sstable-levels="true" data-testid="sstable-levels-label">
          SSTable Levels
        </text>

        {/* Level L0 */}
        <g transform="translate(50, 290)" data-component="sstable-l0" data-testid="component-sstable-l0">
          <rect
            width="700"
            height="45"
            rx="6"
            fill="#161b22"
            stroke="#30363d"
            strokeWidth="1"
          />
          <text x="35" y="28" fill="#58a6ff" fontSize="13" fontWeight="bold">
            L0
          </text>
          {/* SSTable files in L0 */}
          <rect x="70" y="10" width="80" height="25" rx="4" fill="#1f2937" stroke="#3fb950" strokeWidth="1" />
          <text x="110" y="27" fill="#c9d1d9" fontSize="10" textAnchor="middle">SST-001</text>
          <rect x="160" y="10" width="80" height="25" rx="4" fill="#1f2937" stroke="#58a6ff" strokeWidth="1" />
          <text x="200" y="27" fill="#c9d1d9" fontSize="10" textAnchor="middle">SST-002</text>
          <rect x="250" y="10" width="80" height="25" rx="4" fill="#1f2937" stroke="#58a6ff" strokeWidth="1" />
          <text x="290" y="27" fill="#c9d1d9" fontSize="10" textAnchor="middle">SST-003</text>
          <text x="360" y="27" fill="#8b949e" fontSize="10">...</text>
        </g>

        {/* Compaction arrow from L0 to L1 */}
        <path d="M 400 340 L 400 360" fill="none" stroke="#8b949e" strokeWidth="2" strokeDasharray="4,2" markerEnd="url(#arrowhead-compact)" data-testid="compaction-l0-l1" />
        <text x="430" y="355" fill="#8b949e" fontSize="9">compaction</text>

        {/* Level L1 */}
        <g transform="translate(50, 370)" data-component="sstable-l1" data-testid="component-sstable-l1">
          <rect
            width="700"
            height="45"
            rx="6"
            fill="#161b22"
            stroke="#30363d"
            strokeWidth="1"
          />
          <text x="35" y="28" fill="#58a6ff" fontSize="13" fontWeight="bold">
            L1
          </text>
          <rect x="70" y="10" width="100" height="25" rx="4" fill="#1f2937" stroke="#58a6ff" strokeWidth="1" />
          <text x="120" y="27" fill="#c9d1d9" fontSize="10" textAnchor="middle">SST-010</text>
          <rect x="180" y="10" width="100" height="25" rx="4" fill="#1f2937" stroke="#58a6ff" strokeWidth="1" />
          <text x="230" y="27" fill="#c9d1d9" fontSize="10" textAnchor="middle">SST-011</text>
          <text x="300" y="27" fill="#8b949e" fontSize="10">...</text>
        </g>

        {/* Compaction arrow from L1 to L2+ */}
        <path d="M 400 420 L 400 440" fill="none" stroke="#8b949e" strokeWidth="2" strokeDasharray="4,2" markerEnd="url(#arrowhead-compact)" data-testid="compaction-l1-l2" />

        {/* Levels L2-L7 */}
        <g transform="translate(50, 450)" data-component="sstable-l2-l7" data-testid="component-sstable-l2-l7">
          <rect
            width="700"
            height="45"
            rx="6"
            fill="#161b22"
            stroke="#30363d"
            strokeWidth="1"
          />
          <text x="30" y="28" fill="#58a6ff" fontSize="13" fontWeight="bold">
            L2-L7
          </text>
          <rect x="90" y="10" width="120" height="25" rx="4" fill="#1f2937" stroke="#58a6ff" strokeWidth="1" />
          <text x="150" y="27" fill="#c9d1d9" fontSize="10" textAnchor="middle">Larger SSTables</text>
          <rect x="220" y="10" width="120" height="25" rx="4" fill="#1f2937" stroke="#58a6ff" strokeWidth="1" />
          <text x="280" y="27" fill="#c9d1d9" fontSize="10" textAnchor="middle">Sorted Runs</text>
          <text x="370" y="27" fill="#8b949e" fontSize="10">... (exponentially larger)</text>
        </g>

        {/* Read Path Overlay */}
        <g className="read-path" data-read-path="true" data-testid="read-path">
          <text x="700" y="100" fill="#d29922" fontSize="14" fontWeight="bold">
            Read
          </text>
          {/* Read starts at Memtable */}
          <path d="M 680 110 L 680 130 L 350 130" fill="none" stroke="#d29922" strokeWidth="2" markerEnd="url(#arrowhead-read)" />
          {/* If not found, check L0 */}
          <path d="M 270 160 L 270 180 L 270 290 L 300 310" fill="none" stroke="#d29922" strokeWidth="2" strokeDasharray="6,3" markerEnd="url(#arrowhead-read)" />
          <text x="235" y="230" fill="#d29922" fontSize="9" transform="rotate(-90, 235, 230)">if not found</text>
          {/* Continue to L1 */}
          <path d="M 300 340 L 300 380 L 320 395" fill="none" stroke="#d29922" strokeWidth="2" strokeDasharray="6,3" markerEnd="url(#arrowhead-read)" />
          {/* Continue to L2-L7 */}
          <path d="M 300 420 L 300 460 L 320 475" fill="none" stroke="#d29922" strokeWidth="2" strokeDasharray="6,3" markerEnd="url(#arrowhead-read)" />
        </g>

        {/* Size indicators */}
        <g transform="translate(760, 310)" className="size-indicator" data-testid="size-indicator">
          <text x="0" y="0" fill="#8b949e" fontSize="10" textAnchor="end">Smaller</text>
          <line x1="5" y1="10" x2="5" y2="170" stroke="#8b949e" strokeWidth="1" />
          <polygon points="5,10 0,20 10,20" fill="#8b949e" />
          <polygon points="5,170 0,160 10,160" fill="#8b949e" />
          <text x="0" y="185" fill="#8b949e" fontSize="10" textAnchor="end">Larger</text>
        </g>
      </svg>
    </div>
  );
}

interface ComponentDescriptionProps {
  title: string;
  description: string;
  colorClass?: string;
  testId: string;
}

function ComponentDescription({ title, description, colorClass = 'text-accent', testId }: ComponentDescriptionProps) {
  return (
    <article className="bg-surface rounded-lg p-6 border border-border" data-testid={testId}>
      <h3 className={`text-lg font-bold font-mono mb-2 ${colorClass}`} data-testid={`${testId}-title`}>
        {title}
      </h3>
      <p className="text-text-secondary text-sm" data-testid={`${testId}-description`}>
        {description}
      </p>
    </article>
  );
}

interface ArchitectureDiagramProps {
  className?: string;
}

export function ArchitectureDiagram({ className = '' }: ArchitectureDiagramProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const [isLoaded, setIsLoaded] = useState(false);

  useEffect(() => {
    const container = containerRef.current;
    if (!container) return;

    // Check if IntersectionObserver is supported
    if ('IntersectionObserver' in window) {
      const observer = new IntersectionObserver(
        (entries) => {
          entries.forEach((entry) => {
            if (entry.isIntersecting) {
              setIsLoaded(true);
              observer.disconnect();
            }
          });
        },
        {
          rootMargin: '100px 0px',
          threshold: 0.1,
        }
      );

      observer.observe(container);

      return () => observer.disconnect();
    } else {
      // Fallback: show immediately if IntersectionObserver not supported
      setIsLoaded(true);
    }
  }, []);

  const componentDescriptions = [
    {
      title: 'Write-Ahead Log (WAL)',
      description: 'All write operations are first appended to the WAL for durability. In case of a crash, uncommitted data can be recovered from the log, ensuring no data loss.',
      colorClass: 'text-accent',
      testId: 'description-wal',
    },
    {
      title: 'Memtable',
      description: 'An in-memory skip list that buffers recent writes. Provides fast O(log n) lookups and insertions. When it reaches capacity, it becomes immutable and is flushed to disk.',
      colorClass: 'text-success',
      testId: 'description-memtable',
    },
    {
      title: 'Immutable Memtable',
      description: "A frozen memtable awaiting flush to disk. While it's being flushed, a new active memtable accepts incoming writes, ensuring continuous write availability.",
      colorClass: 'text-accent',
      testId: 'description-immutable-memtable',
    },
    {
      title: 'SSTable (Sorted String Table)',
      description: 'Immutable, sorted files stored on disk. Each SSTable contains a block index for efficient binary search. Data is compressed for optimal storage efficiency.',
      colorClass: 'text-accent',
      testId: 'description-sstable',
    },
    {
      title: 'Compaction',
      description: 'Background process that merges and sorts SSTables across levels. Minor compaction moves L0 files to L1; major compaction merges files into deeper levels (L2-L7), removing deleted entries and duplicates.',
      colorClass: 'text-warning',
      testId: 'description-compaction',
    },
    {
      title: 'Read & Write Paths',
      description: 'Writes: WAL → Memtable → Immutable → SSTable (L0). Reads: Check Memtable first, then traverse SSTable levels (L0→L7) until found.',
      colorClass: 'text-accent',
      testId: 'description-paths',
    },
  ];

  return (
    <section
      id="architecture"
      className={`py-16 px-4 ${className}`}
      aria-labelledby="architecture-heading"
      data-testid="architecture-section"
    >
      <div className="max-w-6xl mx-auto">
        <h2
          id="architecture-heading"
          className="text-3xl font-bold font-mono text-text-primary mb-4 text-center"
          data-testid="architecture-heading"
        >
          Architecture
        </h2>
        <p className="text-text-secondary text-center mb-12 max-w-2xl mx-auto" data-testid="architecture-intro">
          MirDB uses an LSM-tree (Log-Structured Merge-tree) architecture optimized for high write throughput and efficient storage.
        </p>

        {/* Lazy-loaded diagram wrapper */}
        <div
          ref={containerRef}
          className="architecture-diagram-container mb-12"
          data-lazy-diagram="true"
          data-loaded={isLoaded}
          data-testid="diagram-container"
        >
          {!isLoaded ? (
            <div className="lazy-placeholder" aria-hidden="true" data-testid="diagram-placeholder">
              <div className="flex items-center justify-center h-64 bg-surface rounded-lg border border-border">
                <p className="text-text-secondary">Loading architecture diagram...</p>
              </div>
            </div>
          ) : (
            <div
              className="lazy-content"
              style={{ opacity: isLoaded ? 1 : 0, transition: 'opacity 0.3s ease-in-out' }}
              data-testid="diagram-content"
            >
              <LSMTreeSVG />
            </div>
          )}
        </div>

        {/* Component Descriptions */}
        <div className="grid md:grid-cols-2 lg:grid-cols-3 gap-6" data-testid="component-descriptions">
          {componentDescriptions.map((desc) => (
            <ComponentDescription key={desc.testId} {...desc} />
          ))}
        </div>

        {/* SSTable Levels explanation */}
        <div className="mt-8 bg-surface rounded-lg p-6 border border-border" data-testid="levels-explanation">
          <h3 className="text-lg font-bold font-mono text-text-primary mb-4" data-testid="levels-heading">
            SSTable Levels (L0-L7)
          </h3>
          <div className="grid md:grid-cols-2 gap-4 text-sm text-text-secondary">
            <div>
              <p className="mb-2" data-testid="levels-l0-desc">
                <strong className="text-accent">Level 0:</strong> Freshly flushed SSTables. Files may have overlapping key ranges.
              </p>
              <p className="mb-2" data-testid="levels-l1-plus-desc">
                <strong className="text-accent">Level 1+:</strong> SSTables with non-overlapping key ranges within each level.
              </p>
            </div>
            <div>
              <p className="mb-2" data-testid="levels-size-ratio">
                <strong className="text-accent">Size Ratio:</strong> Each level is ~10x larger than the previous level.
              </p>
              <p data-testid="levels-compaction-trigger">
                <strong className="text-accent">Compaction Trigger:</strong> When a level exceeds its size threshold, compaction moves data to the next level.
              </p>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}

export default ArchitectureDiagram;
