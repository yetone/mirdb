/**
 * Architecture Overview Component
 * Owner: Scenario 6 - Architecture Overview Section
 *
 * Displays the LSM Tree architecture with a visual diagram and detailed explanations
 * of memtable, SSTables, and compaction processes.
 */

/**
 * Renders the LSM Tree diagram as an inline SVG
 * Shows data flow: Write -> Memtable -> Immutable Memtable -> Level 0 SSTables -> Level N SSTables
 * @returns {string} HTML string for the LSM Tree SVG diagram
 */
export function LSMTreeDiagram() {
  return `
    <svg
      viewBox="0 0 800 600"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      role="img"
      aria-label="LSM Tree Architecture Diagram showing data flow from Memtable through Immutable Memtable to SSTables across multiple levels"
      class="w-full max-w-3xl mx-auto dark:text-white text-gray-900"
      data-testid="lsm-tree-diagram"
    >
      <title>LSM Tree Architecture</title>
      <desc>A diagram showing the Log-Structured Merge Tree architecture used by MirDB. Data flows from the active Memtable (in-memory skip list) to Immutable Memtables, then to Level 0 SSTables, and finally compacted into Level N SSTables on disk.</desc>

      <!-- Background -->
      <rect width="800" height="600" fill="transparent"/>

      <!-- Title -->
      <text x="400" y="40" font-family="system-ui, sans-serif" font-size="24" font-weight="bold" fill="currentColor" text-anchor="middle">LSM Tree Architecture</text>

      <!-- Write Request Arrow -->
      <path d="M400 70 L400 95" stroke="currentColor" stroke-width="2" marker-end="url(#arrowhead-arch)"/>
      <text x="400" y="65" font-family="system-ui, sans-serif" font-size="12" fill="currentColor" text-anchor="middle" opacity="0.8">Write Request</text>

      <!-- Memtable Box -->
      <rect x="275" y="100" width="250" height="70" rx="8" fill="#3b82f6" fill-opacity="0.2" stroke="#3b82f6" stroke-width="2"/>
      <text x="400" y="130" font-family="system-ui, sans-serif" font-size="16" font-weight="600" fill="currentColor" text-anchor="middle">Memtable</text>
      <text x="400" y="152" font-family="system-ui, sans-serif" font-size="12" fill="currentColor" text-anchor="middle" opacity="0.8">(Active Skip List)</text>

      <!-- Arrow to Immutable -->
      <path d="M400 170 L400 205" stroke="currentColor" stroke-width="2" marker-end="url(#arrowhead-arch)"/>
      <text x="470" y="192" font-family="system-ui, sans-serif" font-size="11" fill="currentColor" opacity="0.7">when full</text>

      <!-- Immutable Memtable Box -->
      <rect x="275" y="210" width="250" height="70" rx="8" fill="#8b5cf6" fill-opacity="0.2" stroke="#8b5cf6" stroke-width="2"/>
      <text x="400" y="240" font-family="system-ui, sans-serif" font-size="16" font-weight="600" fill="currentColor" text-anchor="middle">Immutable Memtable</text>
      <text x="400" y="262" font-family="system-ui, sans-serif" font-size="12" fill="currentColor" text-anchor="middle" opacity="0.8">(Read-only, awaiting flush)</text>

      <!-- Arrow to Level 0 -->
      <path d="M400 280 L400 315" stroke="currentColor" stroke-width="2" marker-end="url(#arrowhead-arch)"/>
      <text x="470" y="302" font-family="system-ui, sans-serif" font-size="11" fill="currentColor" opacity="0.7">minor compaction</text>

      <!-- Disk Storage Label -->
      <rect x="80" y="320" width="640" height="250" rx="12" fill="currentColor" fill-opacity="0.05" stroke="currentColor" stroke-width="1" stroke-dasharray="4 2"/>
      <text x="120" y="345" font-family="system-ui, sans-serif" font-size="12" fill="currentColor" opacity="0.6">On Disk (SSTables)</text>

      <!-- Level 0 SSTables -->
      <rect x="120" y="360" width="560" height="60" rx="6" fill="#10b981" fill-opacity="0.2" stroke="#10b981" stroke-width="2"/>
      <text x="400" y="385" font-family="system-ui, sans-serif" font-size="14" font-weight="600" fill="currentColor" text-anchor="middle">Level 0 SSTables</text>
      <text x="400" y="405" font-family="system-ui, sans-serif" font-size="11" fill="currentColor" text-anchor="middle" opacity="0.8">(Recently flushed, may overlap)</text>

      <!-- Level 0 SSTable blocks -->
      <rect x="150" y="365" width="80" height="25" rx="4" fill="#10b981" fill-opacity="0.4" stroke="#10b981"/>
      <rect x="250" y="365" width="80" height="25" rx="4" fill="#10b981" fill-opacity="0.4" stroke="#10b981"/>
      <rect x="350" y="365" width="80" height="25" rx="4" fill="#10b981" fill-opacity="0.4" stroke="#10b981"/>
      <rect x="450" y="365" width="80" height="25" rx="4" fill="#10b981" fill-opacity="0.4" stroke="#10b981"/>
      <rect x="550" y="365" width="80" height="25" rx="4" fill="#10b981" fill-opacity="0.4" stroke="#10b981"/>

      <!-- Arrow to Level 1 -->
      <path d="M400 420 L400 450" stroke="currentColor" stroke-width="2" marker-end="url(#arrowhead-arch)"/>
      <text x="470" y="440" font-family="system-ui, sans-serif" font-size="11" fill="currentColor" opacity="0.7">major compaction</text>

      <!-- Level 1+ SSTables -->
      <rect x="120" y="455" width="560" height="60" rx="6" fill="#f59e0b" fill-opacity="0.2" stroke="#f59e0b" stroke-width="2"/>
      <text x="400" y="480" font-family="system-ui, sans-serif" font-size="14" font-weight="600" fill="currentColor" text-anchor="middle">Level 1+ SSTables</text>
      <text x="400" y="500" font-family="system-ui, sans-serif" font-size="11" fill="currentColor" text-anchor="middle" opacity="0.8">(Sorted, non-overlapping within level)</text>

      <!-- Level 1 SSTable blocks -->
      <rect x="130" y="460" width="160" height="25" rx="4" fill="#f59e0b" fill-opacity="0.4" stroke="#f59e0b"/>
      <rect x="310" y="460" width="180" height="25" rx="4" fill="#f59e0b" fill-opacity="0.4" stroke="#f59e0b"/>
      <rect x="510" y="460" width="160" height="25" rx="4" fill="#f59e0b" fill-opacity="0.4" stroke="#f59e0b"/>

      <!-- Arrow to Level N -->
      <path d="M400 515 L400 535" stroke="currentColor" stroke-width="2" stroke-dasharray="4 2"/>
      <text x="400" y="555" font-family="system-ui, sans-serif" font-size="12" fill="currentColor" text-anchor="middle" opacity="0.6">... Level N ...</text>

      <!-- Arrowhead marker definition -->
      <defs>
        <marker id="arrowhead-arch" markerWidth="10" markerHeight="7" refX="9" refY="3.5" orient="auto">
          <polygon points="0 0, 10 3.5, 0 7" fill="currentColor"/>
        </marker>
      </defs>
    </svg>
  `
}

/**
 * Renders an explanation card for architecture concepts
 * @param {Object} props - Card properties
 * @param {string} props.id - Unique identifier for the card
 * @param {string} props.title - Card title
 * @param {string} props.icon - SVG icon markup
 * @param {string} props.content - Card content HTML
 * @returns {string} HTML string for the explanation card
 */
function ExplanationCard({ id, title, icon, content }) {
  return `
    <article
      class="explanation-card bg-white dark:bg-gray-800 rounded-xl shadow-md p-6"
      data-testid="explanation-${id}"
    >
      <div class="flex items-start gap-4">
        <div class="flex-shrink-0 w-12 h-12 flex items-center justify-center rounded-lg bg-blue-100 dark:bg-blue-900/30">
          ${icon}
        </div>
        <div class="flex-1">
          <h3 class="text-lg font-semibold text-gray-900 dark:text-white mb-2">${title}</h3>
          <div class="text-gray-600 dark:text-gray-300 text-sm leading-relaxed">
            ${content}
          </div>
        </div>
      </div>
    </article>
  `
}

/**
 * Renders the complete architecture overview section
 * Includes the LSM Tree diagram and explanations for memtable, SSTables, and compaction
 * @returns {string} HTML string for the architecture overview section
 */
export function ArchitectureOverview() {
  const memtableIcon = `<svg class="w-6 h-6 text-blue-600 dark:text-blue-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 3v2m6-2v2M9 19v2m6-2v2M5 9H3m2 6H3m18-6h-2m2 6h-2M7 19h10a2 2 0 002-2V7a2 2 0 00-2-2H7a2 2 0 00-2 2v10a2 2 0 002 2zM9 9h6v6H9V9z"></path>
  </svg>`

  const sstableIcon = `<svg class="w-6 h-6 text-green-600 dark:text-green-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4m0 5c0 2.21-3.582 4-8 4s-8-1.79-8-4"></path>
  </svg>`

  const compactionIcon = `<svg class="w-6 h-6 text-amber-600 dark:text-amber-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"></path>
  </svg>`

  const memtableContent = `
    <p class="mb-2">
      The <strong>memtable</strong> is an <strong>in-memory</strong> data structure that holds all recent writes.
      MirDB implements it as a <strong>skip list</strong>, providing O(log n) insert and lookup performance.
    </p>
    <p>
      Incoming write operations are first appended to the Write-Ahead Log (WAL) for durability,
      then inserted into the active memtable. When the memtable reaches its size threshold (default 4MB),
      it becomes immutable and a new active memtable is created.
    </p>
  `

  const sstableContent = `
    <p class="mb-2">
      <strong>SSTables</strong> (Sorted String Tables) are immutable files stored on <strong>disk</strong>.
      Each SSTable contains key-value pairs <strong>sorted</strong> by key, organized in a <strong>block</strong> structure.
    </p>
    <p class="mb-2">
      The file format includes data blocks (4KB default), a meta block with min/max keys and bloom filter,
      an index block for fast lookups, and a footer for metadata.
    </p>
    <p>
      SSTables are organized into levels. Level 0 files may have overlapping key ranges,
      while Level 1+ files are non-overlapping within each level for efficient range queries.
    </p>
  `

  const compactionContent = `
    <p class="mb-2">
      <strong>Minor compaction</strong> flushes immutable memtables to disk as new Level 0 SSTables.
      This happens automatically when memtables reach their size limit.
    </p>
    <p class="mb-2">
      <strong>Major compaction</strong> merges SSTables from one level into the next.
      This process combines overlapping files, removes deleted keys, and optimizes storage efficiency.
    </p>
    <p>
      Compaction runs in background threads to minimize impact on read/write performance,
      ensuring the database remains responsive while maintaining optimal disk usage.
    </p>
  `

  return `
    <div
      class="architecture-section container-custom max-w-7xl mx-auto px-4 sm:px-6 lg:px-8"
      data-testid="architecture-section"
      aria-label="Architecture Overview Section"
    >
      <!-- Section Header -->
      <div class="text-center mb-12">
        <h2 class="section-title text-3xl md:text-4xl font-bold text-gray-900 dark:text-white mb-4">
          Architecture Overview
        </h2>
        <p class="section-subtitle text-lg text-gray-600 dark:text-gray-400 max-w-2xl mx-auto">
          MirDB uses a Log-Structured Merge (LSM) tree architecture optimized for write-heavy workloads
          while maintaining excellent read performance.
        </p>
      </div>

      <!-- LSM Tree Diagram -->
      <div class="diagram-container mb-12 bg-gray-50 dark:bg-gray-800/50 rounded-2xl p-6 overflow-x-auto">
        ${LSMTreeDiagram()}
      </div>

      <!-- Explanations Grid -->
      <div class="explanations-grid grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
        ${ExplanationCard({
          id: 'memtable',
          title: 'Memtable',
          icon: memtableIcon,
          content: memtableContent
        })}
        ${ExplanationCard({
          id: 'sstable',
          title: 'SSTables',
          icon: sstableIcon,
          content: sstableContent
        })}
        ${ExplanationCard({
          id: 'compaction',
          title: 'Compaction',
          icon: compactionIcon,
          content: compactionContent
        })}
      </div>
    </div>
  `
}

export default ArchitectureOverview
