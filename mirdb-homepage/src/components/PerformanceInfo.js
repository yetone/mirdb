/**
 * Performance Information Component
 * Owner: Scenario 7 - Performance Information Section
 *
 * Displays performance characteristics, benchmarks, and configuration options
 * with tuning guidelines for optimizing MirDB.
 */

import configOptions from '../data/config-options.json'

/**
 * Performance characteristic icons mapping
 */
const icons = {
  write_performance: `<svg class="config-icon w-6 h-6 text-green-600 dark:text-green-400" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 10V3L4 14h7v7l9-11h-7z"></path>
  </svg>`,
  storage_efficiency: `<svg class="config-icon w-6 h-6 text-blue-600 dark:text-blue-400" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4"></path>
  </svg>`,
  read_write_balance: `<svg class="config-icon w-6 h-6 text-purple-600 dark:text-purple-400" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 6l3 1m0 0l-3 9a5.002 5.002 0 006.001 0M6 7l3 9M6 7l6-2m6 2l3-1m-3 1l-3 9a5.002 5.002 0 006.001 0M18 7l3 9m-3-9l-6-2m0-2v2m0 16V5m0 16H9m3 0h3"></path>
  </svg>`,
  read_performance: `<svg class="config-icon w-6 h-6 text-orange-600 dark:text-orange-400" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z"></path>
  </svg>`,
  compaction_frequency: `<svg class="config-icon w-6 h-6 text-indigo-600 dark:text-indigo-400" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"></path>
  </svg>`
}

/**
 * Renders a configuration option card with name, default value, description, and tuning guidance
 * @param {Object} props - Configuration option properties
 * @param {string} props.id - Unique identifier
 * @param {string} props.name - Configuration parameter name
 * @param {string} props.default - Default value
 * @param {string} props.description - Parameter description
 * @param {string} props.tuning - Tuning guidance
 * @param {string} props.impact - Impact category for icon selection
 * @returns {string} HTML string for the configuration option card
 */
export function ConfigOption({ id, name, default: defaultValue, description, tuning, impact }) {
  const iconSvg = icons[impact] || icons.write_performance

  return `
    <article
      class="config-option bg-white dark:bg-gray-800 rounded-lg shadow-md p-6 border-l-4 border-primary-500"
      data-config-id="${id}"
      data-testid="config-option-${id}"
    >
      <div class="flex items-start gap-4">
        <div class="config-icon-wrapper flex-shrink-0 mt-1">
          ${iconSvg}
        </div>
        <div class="flex-grow">
          <div class="flex flex-wrap items-center gap-2 mb-2">
            <h3 class="config-name text-lg font-mono font-semibold text-gray-900 dark:text-white">
              ${name}
            </h3>
            <span class="config-default inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-primary-100 dark:bg-primary-900 text-primary-800 dark:text-primary-200" data-testid="config-default-${id}">
              Default: ${defaultValue}
            </span>
          </div>
          <p class="config-description text-gray-600 dark:text-gray-300 text-sm mb-3">
            ${description}
          </p>
          <div class="config-tuning bg-gray-50 dark:bg-gray-700 rounded-md p-3">
            <p class="tuning-label text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wide mb-1">
              Tuning Guidance
            </p>
            <p class="tuning-text text-sm text-gray-700 dark:text-gray-300">
              ${tuning}
            </p>
          </div>
        </div>
      </div>
    </article>
  `
}

/**
 * Renders performance characteristics section with benchmarks
 * @returns {string} HTML string for performance characteristics
 */
export function PerformanceCharacteristics() {
  return `
    <div class="performance-characteristics bg-gradient-to-r from-primary-50 to-primary-100 dark:from-gray-800 dark:to-gray-700 rounded-xl p-6 mb-12" data-testid="performance-characteristics">
      <h3 class="text-xl font-bold text-gray-900 dark:text-white mb-4">
        Performance Characteristics
      </h3>
      <div class="grid grid-cols-1 md:grid-cols-3 gap-6">
        <div class="perf-metric text-center" data-testid="perf-metric-writes">
          <div class="metric-icon mb-2">
            <svg class="w-10 h-10 mx-auto text-green-600 dark:text-green-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 7h8m0 0v8m0-8l-8 8-4-4-6 6"></path>
            </svg>
          </div>
          <p class="metric-title text-sm font-semibold text-gray-600 dark:text-gray-400 uppercase">Write Performance</p>
          <p class="metric-value text-2xl font-bold text-gray-900 dark:text-white">Optimized</p>
          <p class="metric-description text-xs text-gray-500 dark:text-gray-400 mt-1">Sequential writes to memtable with O(log n) insert time</p>
        </div>
        <div class="perf-metric text-center" data-testid="perf-metric-reads">
          <div class="metric-icon mb-2">
            <svg class="w-10 h-10 mx-auto text-blue-600 dark:text-blue-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z"></path>
            </svg>
          </div>
          <p class="metric-title text-sm font-semibold text-gray-600 dark:text-gray-400 uppercase">Read Performance</p>
          <p class="metric-value text-2xl font-bold text-gray-900 dark:text-white">Efficient</p>
          <p class="metric-description text-xs text-gray-500 dark:text-gray-400 mt-1">Bloom filters and block indices minimize disk reads</p>
        </div>
        <div class="perf-metric text-center" data-testid="perf-metric-durability">
          <div class="metric-icon mb-2">
            <svg class="w-10 h-10 mx-auto text-purple-600 dark:text-purple-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m5.618-4.016A11.955 11.955 0 0112 2.944a11.955 11.955 0 01-8.618 3.04A12.02 12.02 0 003 9c0 5.591 3.824 10.29 9 11.622 5.176-1.332 9-6.03 9-11.622 0-1.042-.133-2.052-.382-3.016z"></path>
            </svg>
          </div>
          <p class="metric-title text-sm font-semibold text-gray-600 dark:text-gray-400 uppercase">Durability</p>
          <p class="metric-value text-2xl font-bold text-gray-900 dark:text-white">Persistent</p>
          <p class="metric-description text-xs text-gray-500 dark:text-gray-400 mt-1">Data survives restarts with SSTable persistence</p>
        </div>
      </div>
    </div>
  `
}

/**
 * Renders the complete performance information section
 * @returns {string} HTML string for the performance information section
 */
export function PerformanceInfo() {
  const configCards = configOptions.map(option => ConfigOption(option)).join('')

  return `
    <div class="container-custom max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
      <div class="text-center mb-12">
        <h2 class="section-title text-3xl md:text-4xl font-bold text-gray-900 dark:text-white mb-4">
          Performance & Configuration
        </h2>
        <p class="section-subtitle text-lg text-gray-600 dark:text-gray-400 max-w-2xl mx-auto">
          MirDB's LSM tree architecture is optimized for write-heavy workloads while maintaining efficient reads. Fine-tune these parameters to match your specific use case.
        </p>
      </div>

      ${PerformanceCharacteristics()}

      <div class="config-section">
        <h3 class="config-section-title text-2xl font-bold text-gray-900 dark:text-white mb-6">
          Configuration Options
        </h3>
        <div
          class="config-grid space-y-4"
          data-testid="config-grid"
          role="list"
          aria-label="MirDB Configuration Options"
        >
          ${configCards}
        </div>
      </div>

      <div class="tuning-summary bg-gray-100 dark:bg-gray-800 rounded-lg p-6 mt-8" data-testid="tuning-summary">
        <h4 class="text-lg font-semibold text-gray-900 dark:text-white mb-3">
          General Tuning Guidelines
        </h4>
        <ul class="tuning-guidelines space-y-2 text-sm text-gray-600 dark:text-gray-300">
          <li class="flex items-start gap-2">
            <span class="text-green-500 mt-0.5">&#10003;</span>
            <span><strong>Write-heavy workloads:</strong> Increase memtable_max_size and l0_compaction_trigger for better write throughput.</span>
          </li>
          <li class="flex items-start gap-2">
            <span class="text-green-500 mt-0.5">&#10003;</span>
            <span><strong>Read-heavy workloads:</strong> Use smaller block_size and fewer max_levels to reduce read amplification.</span>
          </li>
          <li class="flex items-start gap-2">
            <span class="text-green-500 mt-0.5">&#10003;</span>
            <span><strong>Large datasets:</strong> Increase sstable_max_size and max_levels for efficient storage of more data.</span>
          </li>
          <li class="flex items-start gap-2">
            <span class="text-green-500 mt-0.5">&#10003;</span>
            <span><strong>Memory-constrained:</strong> Reduce memtable_max_size to limit memory usage at the cost of more frequent flushes.</span>
          </li>
        </ul>
      </div>
    </div>
  `
}

export default PerformanceInfo
