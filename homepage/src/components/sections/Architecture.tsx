/**
 * Architecture Overview Section Component.
 * Owner: Scenario 4 - Architecture Overview Section
 *
 * Responsibilities:
 * - Display high-level architecture diagram
 * - Explain LSM tree implementation
 * - Describe skip-list memtable
 * - Cover compaction processes
 *
 * Requirements:
 * - REQ-7: Architecture Overview section
 * - US-4: High-level architecture understanding
 */

import React from 'react'

interface ArchitectureLayerProps {
  title: string
  description: string
  color: string
}

function ArchitectureLayer({ title, description, color }: ArchitectureLayerProps) {
  return (
    <div
      className={`p-4 rounded-lg ${color} border border-gray-200 dark:border-gray-700`}
      data-testid="architecture-layer"
    >
      <h4 className="font-semibold text-gray-900 dark:text-white mb-1">{title}</h4>
      <p className="text-sm text-gray-600 dark:text-gray-300">{description}</p>
    </div>
  )
}

export function Architecture() {
  return (
    <section
      id="architecture"
      className="py-16 sm:py-24 bg-white dark:bg-gray-800"
      aria-labelledby="architecture-heading"
    >
      <div className="section-container">
        <div className="text-center max-w-3xl mx-auto mb-12">
          <h2
            id="architecture-heading"
            className="section-heading"
          >
            Architecture Overview
          </h2>
          <p className="section-description">
            MirDB uses a Log-Structured Merge-tree (LSM tree) architecture for high write throughput
            and efficient storage management.
          </p>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-12 items-start">
          {/* Architecture Diagram */}
          <div
            className="bg-gray-50 dark:bg-gray-900 rounded-xl p-6 border border-gray-200 dark:border-gray-700"
            data-testid="architecture-diagram"
            role="img"
            aria-label="MirDB LSM tree architecture diagram showing data flow from client writes through memtable, WAL, and SSTable levels with compaction"
          >
            <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-6 text-center">
              LSM Tree Data Flow
            </h3>

            {/* SVG Architecture Diagram */}
            <svg
              viewBox="0 0 400 320"
              className="w-full h-auto"
              data-testid="architecture-svg"
              role="presentation"
            >
              {/* Client Writes */}
              <rect x="150" y="10" width="100" height="40" rx="4" className="fill-blue-500" />
              <text x="200" y="35" textAnchor="middle" className="fill-white text-xs font-medium">
                Client Writes
              </text>

              {/* Arrow down */}
              <path d="M200 50 L200 70 L195 65 M200 70 L205 65" className="stroke-gray-400 dark:stroke-gray-500 fill-none stroke-2" />

              {/* Memtable (Skip-List) */}
              <rect x="120" y="75" width="160" height="50" rx="4" className="fill-green-500" />
              <text x="200" y="95" textAnchor="middle" className="fill-white text-xs font-medium">
                Skip-List Memtable
              </text>
              <text x="200" y="115" textAnchor="middle" className="fill-green-100 text-[10px]">
                (In-Memory, O(log n))
              </text>

              {/* WAL side */}
              <rect x="300" y="75" width="80" height="50" rx="4" className="fill-yellow-500" />
              <text x="340" y="100" textAnchor="middle" className="fill-white text-xs font-medium">
                WAL
              </text>

              {/* Arrow to WAL */}
              <path d="M280 100 L300 100 L295 95 M300 100 L295 105" className="stroke-gray-400 dark:stroke-gray-500 fill-none stroke-2" />

              {/* Arrow down */}
              <path d="M200 125 L200 145 L195 140 M200 145 L205 140" className="stroke-gray-400 dark:stroke-gray-500 fill-none stroke-2" />
              <text x="220" y="138" className="fill-gray-500 dark:fill-gray-400 text-[9px]">flush</text>

              {/* Level 0 SSTables */}
              <rect x="100" y="150" width="200" height="40" rx="4" className="fill-purple-500" />
              <text x="200" y="175" textAnchor="middle" className="fill-white text-xs font-medium">
                Level 0 SSTables (Immutable)
              </text>

              {/* Minor Compaction Arrow */}
              <path d="M200 190 L200 210 L195 205 M200 210 L205 205" className="stroke-orange-500 fill-none stroke-2" />
              <text x="260" y="203" className="fill-orange-500 text-[9px] font-medium">minor compaction</text>

              {/* Level 1+ SSTables */}
              <rect x="80" y="215" width="240" height="40" rx="4" className="fill-indigo-500" />
              <text x="200" y="240" textAnchor="middle" className="fill-white text-xs font-medium">
                Level 1+ SSTables (Sorted)
              </text>

              {/* Major Compaction Arrow */}
              <path d="M200 255 L200 275 L195 270 M200 275 L205 270" className="stroke-red-500 fill-none stroke-2" />
              <text x="260" y="268" className="fill-red-500 text-[9px] font-medium">major compaction</text>

              {/* Disk */}
              <rect x="60" y="280" width="280" height="35" rx="4" className="fill-gray-600 dark:fill-gray-700" />
              <text x="200" y="302" textAnchor="middle" className="fill-white text-xs font-medium">
                Persistent Disk Storage
              </text>
            </svg>
          </div>

          {/* Architecture Description */}
          <div className="space-y-6">
            <ArchitectureLayer
              title="Skip-List Memtable"
              description="Incoming writes are first stored in an in-memory skip-list data structure. Skip-lists provide O(log n) insert, delete, and lookup operations while maintaining sorted order, making them ideal for the memtable implementation."
              color="bg-green-50 dark:bg-green-900/20"
            />

            <ArchitectureLayer
              title="Log-Structured Merge Tree (LSM)"
              description="MirDB implements an LSM tree architecture where data flows from the memtable to increasingly larger sorted string tables (SSTables) on disk. This design optimizes for write performance while maintaining efficient reads."
              color="bg-purple-50 dark:bg-purple-900/20"
            />

            <ArchitectureLayer
              title="Compaction Process"
              description="Minor compaction flushes the memtable to Level 0 SSTables. Major compaction merges SSTables across levels, removing duplicates and deleted entries. This process maintains storage efficiency and consistent read performance."
              color="bg-orange-50 dark:bg-orange-900/20"
            />

            <ArchitectureLayer
              title="Write-Ahead Log (WAL)"
              description="All writes are first recorded in a write-ahead log for durability. This ensures data can be recovered after crashes before being flushed to the memtable."
              color="bg-yellow-50 dark:bg-yellow-900/20"
            />
          </div>
        </div>

        {/* Key Benefits */}
        <div className="mt-12 grid grid-cols-1 md:grid-cols-3 gap-6">
          <div
            className="text-center p-6 bg-gray-50 dark:bg-gray-900 rounded-lg"
            data-testid="architecture-benefit"
          >
            <div className="text-3xl font-bold text-blue-600 dark:text-blue-400 mb-2">O(1)</div>
            <div className="text-sm text-gray-600 dark:text-gray-300">
              Write Performance
            </div>
          </div>
          <div
            className="text-center p-6 bg-gray-50 dark:bg-gray-900 rounded-lg"
            data-testid="architecture-benefit"
          >
            <div className="text-3xl font-bold text-green-600 dark:text-green-400 mb-2">O(log n)</div>
            <div className="text-sm text-gray-600 dark:text-gray-300">
              Memtable Operations
            </div>
          </div>
          <div
            className="text-center p-6 bg-gray-50 dark:bg-gray-900 rounded-lg"
            data-testid="architecture-benefit"
          >
            <div className="text-3xl font-bold text-purple-600 dark:text-purple-400 mb-2">Durable</div>
            <div className="text-sm text-gray-600 dark:text-gray-300">
              Persistent Storage
            </div>
          </div>
        </div>
      </div>
    </section>
  )
}

export default Architecture
