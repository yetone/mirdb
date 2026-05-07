import { cn } from '@/lib/utils';

export default function ArchitectureSection() {
  return (
    <section
      id="architecture"
      className="py-20 px-4 sm:px-6 lg:px-8"
      data-testid="architecture-section"
    >
      <div className="max-w-7xl mx-auto">
        <div className="text-center mb-16">
          <h2 className={cn('text-3xl sm:text-4xl font-bold text-[var(--foreground)] mb-4')}>
            Architecture
          </h2>
          <p className="text-lg text-[var(--muted-foreground)] max-w-2xl mx-auto">
            LSM-tree based storage engine with efficient compaction and crash recovery.
          </p>
        </div>

        <div
          className="grid grid-cols-1 lg:grid-cols-3 gap-8"
          data-testid="architecture-grid"
        >
          <div className="p-6 rounded-xl border border-[var(--border)] bg-[var(--card)]">
            <h3 className="text-xl font-semibold text-[var(--foreground)] mb-3">Memtable</h3>
            <p className="text-[var(--muted-foreground)]">
              In-memory skip-list data structure for fast writes. All mutations are first written to the memtable and WAL for durability.
            </p>
          </div>
          <div className="p-6 rounded-xl border border-[var(--border)] bg-[var(--card)]">
            <h3 className="text-xl font-semibold text-[var(--foreground)] mb-3">SSTables</h3>
            <p className="text-[var(--muted-foreground)]">
              Immutable sorted string tables stored on disk across multiple levels. Each level has a target size threshold.
            </p>
          </div>
          <div className="p-6 rounded-xl border border-[var(--border)] bg-[var(--card)]">
            <h3 className="text-xl font-semibold text-[var(--foreground)] mb-3">Compaction</h3>
            <p className="text-[var(--muted-foreground)]">
              Background process merges overlapping SSTables to reclaim space and maintain read performance.
            </p>
          </div>
        </div>
      </div>
    </section>
  );
}
