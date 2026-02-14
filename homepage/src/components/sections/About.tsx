import React from 'react';

const ArchitectureDiagram: React.FC = () => {
  return (
    <svg
      role="img"
      aria-label="MirDB Architecture Diagram"
      viewBox="0 0 600 400"
      className="w-full max-w-2xl mx-auto"
    >
      <title>MirDB Architecture Diagram</title>
      <defs>
        <linearGradient id="memtableGradient" x1="0%" y1="0%" x2="100%" y2="100%">
          <stop offset="0%" className="[stop-color:theme(colors.blue.400)]" />
          <stop offset="100%" className="[stop-color:theme(colors.blue.600)]" />
        </linearGradient>
        <linearGradient id="sstableGradient" x1="0%" y1="0%" x2="100%" y2="100%">
          <stop offset="0%" className="[stop-color:theme(colors.green.400)]" />
          <stop offset="100%" className="[stop-color:theme(colors.green.600)]" />
        </linearGradient>
        <linearGradient id="compactionGradient" x1="0%" y1="0%" x2="100%" y2="100%">
          <stop offset="0%" className="[stop-color:theme(colors.purple.400)]" />
          <stop offset="100%" className="[stop-color:theme(colors.purple.600)]" />
        </linearGradient>
      </defs>

      {/* Write Path Arrow */}
      <path
        d="M 80 60 L 80 120"
        fill="none"
        className="stroke-gray-600 dark:stroke-gray-400"
        strokeWidth="2"
        markerEnd="url(#arrowhead)"
      />
      <text x="40" y="50" className="fill-gray-700 dark:fill-gray-300 text-sm font-medium">
        Writes
      </text>

      {/* Memtable Box */}
      <rect
        x="20"
        y="130"
        width="160"
        height="80"
        rx="8"
        fill="url(#memtableGradient)"
        className="opacity-90"
      />
      <text x="100" y="165" textAnchor="middle" className="fill-white font-bold text-base">
        Memtable
      </text>
      <text x="100" y="190" textAnchor="middle" className="fill-white text-xs opacity-90">
        (Skip List)
      </text>

      {/* Flush Arrow */}
      <path
        d="M 180 170 L 250 170"
        fill="none"
        className="stroke-gray-600 dark:stroke-gray-400"
        strokeWidth="2"
        strokeDasharray="5,5"
      />
      <text x="200" y="160" className="fill-gray-600 dark:fill-gray-400 text-xs">
        Flush
      </text>

      {/* SSTable Stack */}
      <rect
        x="260"
        y="100"
        width="140"
        height="40"
        rx="6"
        fill="url(#sstableGradient)"
        className="opacity-70"
      />
      <text x="330" y="125" textAnchor="middle" className="fill-white font-semibold text-sm">
        SSTable L0
      </text>

      <rect
        x="260"
        y="150"
        width="140"
        height="40"
        rx="6"
        fill="url(#sstableGradient)"
        className="opacity-80"
      />
      <text x="330" y="175" textAnchor="middle" className="fill-white font-semibold text-sm">
        SSTable L1
      </text>

      <rect
        x="260"
        y="200"
        width="140"
        height="40"
        rx="6"
        fill="url(#sstableGradient)"
        className="opacity-90"
      />
      <text x="330" y="225" textAnchor="middle" className="fill-white font-semibold text-sm">
        SSTable L2
      </text>

      {/* Compaction Box */}
      <rect
        x="440"
        y="130"
        width="140"
        height="80"
        rx="8"
        fill="url(#compactionGradient)"
        className="opacity-90"
      />
      <text x="510" y="165" textAnchor="middle" className="fill-white font-bold text-base">
        Compaction
      </text>
      <text x="510" y="190" textAnchor="middle" className="fill-white text-xs opacity-90">
        (Merge & Cleanup)
      </text>

      {/* Compaction Arrows */}
      <path
        d="M 400 170 L 440 170"
        fill="none"
        className="stroke-gray-600 dark:stroke-gray-400"
        strokeWidth="2"
      />
      <path
        d="M 510 210 Q 510 280 330 280 Q 150 280 150 240"
        fill="none"
        className="stroke-gray-600 dark:stroke-gray-400"
        strokeWidth="2"
        strokeDasharray="5,5"
      />

      {/* Read Path */}
      <text x="330" y="320" textAnchor="middle" className="fill-gray-700 dark:fill-gray-300 text-sm font-medium">
        Read Path: Memtable → SSTable (newest to oldest)
      </text>

      {/* Disk Storage Label */}
      <rect
        x="240"
        y="80"
        width="180"
        height="180"
        rx="10"
        fill="none"
        className="stroke-gray-400 dark:stroke-gray-600"
        strokeWidth="1"
        strokeDasharray="4,4"
      />
      <text x="330" y="72" textAnchor="middle" className="fill-gray-500 dark:fill-gray-400 text-xs">
        Persistent Storage
      </text>

      {/* Legend */}
      <g transform="translate(20, 340)">
        <rect width="12" height="12" fill="url(#memtableGradient)" rx="2" />
        <text x="18" y="10" className="fill-gray-700 dark:fill-gray-300 text-xs">
          In-Memory
        </text>
        <rect x="100" width="12" height="12" fill="url(#sstableGradient)" rx="2" />
        <text x="118" y="10" className="fill-gray-700 dark:fill-gray-300 text-xs">
          On-Disk
        </text>
        <rect x="200" width="12" height="12" fill="url(#compactionGradient)" rx="2" />
        <text x="218" y="10" className="fill-gray-700 dark:fill-gray-300 text-xs">
          Background Process
        </text>
      </g>
    </svg>
  );
};

export const About: React.FC = () => {
  return (
    <section
      id="about"
      className="py-16 px-4 bg-white dark:bg-gray-900"
      aria-labelledby="about-heading"
    >
      <div className="max-w-4xl mx-auto">
        <h2
          id="about-heading"
          className="text-3xl md:text-4xl font-bold text-gray-900 dark:text-white mb-8 text-center"
        >
          About MirDB
        </h2>

        <div className="space-y-6 text-gray-600 dark:text-gray-300 text-lg leading-relaxed">
          <p>
            MirDB is a high-performance, persistent key-value store that speaks the Memcached protocol.
            Built in Rust, it combines the simplicity of Memcached's well-known interface with durable
            storage that survives restarts, giving you the best of both worlds: blazing-fast in-memory
            operations with the reliability of persistent data.
          </p>

          <p>
            Under the hood, MirDB leverages an LSM tree architecture (Log-Structured Merge-tree) for
            efficient write operations and intelligent storage management. Data flows through an in-memory
            Memtable implemented as a skip list, then flushes to immutable SSTable files on disk. Automatic
            compaction runs in the background to optimize read performance and reclaim storage space.
          </p>

          <p>
            Whether you're building a caching layer that needs persistence, a session store that must
            survive deployments, or any application requiring fast key-value access with durability,
            MirDB provides a drop-in solution compatible with existing Memcached clients and libraries.
          </p>
        </div>

        <div className="mt-12">
          <h3 className="text-xl font-semibold text-gray-900 dark:text-white mb-6 text-center">
            Architecture Overview
          </h3>
          <ArchitectureDiagram />
        </div>
      </div>
    </section>
  );
};
