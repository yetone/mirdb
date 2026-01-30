/**
 * Architecture Section Component.
 * Owner: Scenario 4 - Architecture Diagram Section
 *
 * Displays:
 * - Visual diagram showing data flow:
 *   Client -> Memcached Protocol -> Memtable -> SSTable -> Storage
 * - Brief explanation of LSM-tree implementation
 * - Compaction explanation (minor and major)
 *
 * Expected exports:
 * - renderArchitecture(): HTMLElement
 */

import architectureDiagramUrl from '../assets/images/architecture-diagram.svg';

/**
 * Renders the Architecture section with LSM-tree diagram and explanations
 * @returns HTMLElement - The complete architecture section
 */
export function renderArchitecture(): HTMLElement {
  const section = document.createElement('section');
  section.id = 'architecture';
  section.className = 'architecture-section py-16 px-4 bg-gray-50';
  section.setAttribute('aria-labelledby', 'architecture-heading');

  section.innerHTML = `
    <div class="architecture-container max-w-6xl mx-auto">
      <header class="architecture-header text-center mb-12">
        <h2 id="architecture-heading" class="text-3xl font-bold text-gray-900 mb-4">
          Technical Architecture
        </h2>
        <p class="text-lg text-gray-600 max-w-2xl mx-auto">
          MirDB implements a Log-Structured Merge-tree (LSM-tree) storage engine
          for high-performance persistent key-value storage.
        </p>
      </header>

      <div class="architecture-diagram-container mb-12 bg-white rounded-lg shadow-lg p-6 overflow-x-auto">
        <img
          src="${architectureDiagramUrl}"
          alt="MirDB LSM-Tree Architecture: Data flows from Client through Memcached Protocol to Memtable, then to SSTable levels via compaction, and finally to persistent Storage. The Write-Ahead Log ensures durability."
          class="architecture-diagram w-full h-auto max-w-4xl mx-auto"
          loading="lazy"
        />
      </div>

      <div class="architecture-explanations grid md:grid-cols-2 gap-8">
        <article class="explanation-card bg-white rounded-lg shadow p-6">
          <h3 class="text-xl font-semibold text-gray-900 mb-4 flex items-center">
            <span class="inline-block w-8 h-8 bg-purple-100 text-purple-600 rounded-full mr-3 flex items-center justify-center text-sm font-bold">1</span>
            LSM-Tree Architecture
          </h3>
          <div class="lsm-tree-explanation text-gray-700 space-y-3">
            <p>
              <strong>Log-Structured Merge-tree</strong> is a data structure optimized for
              write-heavy workloads. MirDB uses this approach to achieve high write throughput
              while maintaining efficient reads.
            </p>
            <p>
              <strong>Memtable:</strong> Incoming writes are first stored in an in-memory
              sorted data structure (skip list). This allows for fast writes and maintains
              sorted order for efficient reads.
            </p>
            <p>
              <strong>SSTables (Sorted String Tables):</strong> When the memtable reaches
              its size threshold, it's flushed to disk as an immutable SSTable. These files
              are organized in levels, with newer data in lower levels.
            </p>
          </div>
        </article>

        <article class="explanation-card bg-white rounded-lg shadow p-6">
          <h3 class="text-xl font-semibold text-gray-900 mb-4 flex items-center">
            <span class="inline-block w-8 h-8 bg-blue-100 text-blue-600 rounded-full mr-3 flex items-center justify-center text-sm font-bold">2</span>
            Compaction Process
          </h3>
          <div class="compaction-explanation text-gray-700 space-y-3">
            <p>
              <strong>Minor Compaction:</strong> When the memtable is full (default 4MB),
              it becomes immutable and is flushed to disk as a Level 0 SSTable. This process
              converts in-memory data to persistent storage.
            </p>
            <p>
              <strong>Major Compaction (Level Compaction):</strong> When a level accumulates
              too many SSTables, they are merged and pushed to the next level. This process
              removes duplicate keys, reclaims space from deleted entries, and maintains
              read performance.
            </p>
            <p>
              <strong>Background Processing:</strong> Both compaction types run in dedicated
              background threads, ensuring write operations aren't blocked during compaction.
            </p>
          </div>
        </article>
      </div>

      <div class="architecture-highlights mt-8 bg-white rounded-lg shadow p-6">
        <h3 class="text-lg font-semibold text-gray-900 mb-4">Key Benefits</h3>
        <ul class="grid sm:grid-cols-2 lg:grid-cols-4 gap-4">
          <li class="flex items-start">
            <svg class="w-5 h-5 text-green-500 mr-2 mt-0.5 flex-shrink-0" fill="currentColor" viewBox="0 0 20 20">
              <path fill-rule="evenodd" d="M16.707 5.293a1 1 0 010 1.414l-8 8a1 1 0 01-1.414 0l-4-4a1 1 0 011.414-1.414L8 12.586l7.293-7.293a1 1 0 011.414 0z" clip-rule="evenodd"/>
            </svg>
            <span class="text-gray-700">High write throughput</span>
          </li>
          <li class="flex items-start">
            <svg class="w-5 h-5 text-green-500 mr-2 mt-0.5 flex-shrink-0" fill="currentColor" viewBox="0 0 20 20">
              <path fill-rule="evenodd" d="M16.707 5.293a1 1 0 010 1.414l-8 8a1 1 0 01-1.414 0l-4-4a1 1 0 011.414-1.414L8 12.586l7.293-7.293a1 1 0 011.414 0z" clip-rule="evenodd"/>
            </svg>
            <span class="text-gray-700">Efficient disk usage</span>
          </li>
          <li class="flex items-start">
            <svg class="w-5 h-5 text-green-500 mr-2 mt-0.5 flex-shrink-0" fill="currentColor" viewBox="0 0 20 20">
              <path fill-rule="evenodd" d="M16.707 5.293a1 1 0 010 1.414l-8 8a1 1 0 01-1.414 0l-4-4a1 1 0 011.414-1.414L8 12.586l7.293-7.293a1 1 0 011.414 0z" clip-rule="evenodd"/>
            </svg>
            <span class="text-gray-700">Crash recovery via WAL</span>
          </li>
          <li class="flex items-start">
            <svg class="w-5 h-5 text-green-500 mr-2 mt-0.5 flex-shrink-0" fill="currentColor" viewBox="0 0 20 20">
              <path fill-rule="evenodd" d="M16.707 5.293a1 1 0 010 1.414l-8 8a1 1 0 01-1.414 0l-4-4a1 1 0 011.414-1.414L8 12.586l7.293-7.293a1 1 0 011.414 0z" clip-rule="evenodd"/>
            </svg>
            <span class="text-gray-700">Sorted key ordering</span>
          </li>
        </ul>
      </div>
    </div>
  `;

  return section;
}
