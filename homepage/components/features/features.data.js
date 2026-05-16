/*
 * Feature data — Scenario 2.
 * Exports: FEATURES: Array<{ id: string, icon: string, title: string, description: string }>
 * Constraint: 3–5 entries (PRD REQ-3).
 */

export const FEATURES = [
  {
    id: "speed",
    icon: "<svg viewBox='0 0 24 24' aria-hidden='true' focusable='false' width='32' height='32'><path fill='currentColor' d='M13 2 3 14h7l-1 8 10-12h-7l1-8Z'/></svg>",
    title: "Blazing-fast reads & writes",
    description:
      "In-memory skip-list memtable backed by an LSM tree delivers sub-millisecond GET/SET on commodity hardware.",
  },
  {
    id: "durability",
    icon: "<svg viewBox='0 0 24 24' aria-hidden='true' focusable='false' width='32' height='32'><path fill='currentColor' d='M12 2 3 6v6c0 5 4 9 9 10 5-1 9-5 9-10V6l-9-4Z'/></svg>",
    title: "Crash-safe durability",
    description:
      "Write-ahead log plus immutable SSTables guarantee zero data loss across restarts without sacrificing throughput.",
  },
  {
    id: "protocol",
    icon: "<svg viewBox='0 0 24 24' aria-hidden='true' focusable='false' width='32' height='32'><path fill='currentColor' d='M4 4h16v4H4V4Zm0 6h16v4H4v-4Zm0 6h16v4H4v-4Z'/></svg>",
    title: "Memcached-compatible protocol",
    description:
      "Drop-in replacement for memcached clients — point your existing app at MirDB and gain persistence for free.",
  },
  {
    id: "rust",
    icon: "<svg viewBox='0 0 24 24' aria-hidden='true' focusable='false' width='32' height='32'><path fill='currentColor' d='M12 2 2 7l10 5 10-5-10-5Zm0 7L2 14l10 5 10-5-10-5Z'/></svg>",
    title: "Written in safe Rust",
    description:
      "Built with Rust's ownership model, MirDB avoids whole classes of memory bugs while staying close to C performance.",
  },
];
