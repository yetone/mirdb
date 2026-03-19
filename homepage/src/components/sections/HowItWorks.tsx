/**
 * How It Works Section component.
 * Owner: Scenario 3 - How It Works Section
 *
 * Features:
 * - Architecture diagram (memtable → immutable → SSTable)
 * - Brief LSM tree explanation
 * - Comparison table: MirDB vs memcached
 *
 * Requirements: REQ-2, REQ-4
 */

import type { ComparisonRow } from '@/types';

const comparisonData: ComparisonRow[] = [
  {
    feature: 'Data Persistence',
    mirdb: 'Yes - LSM tree with WAL',
    memcached: 'No - Memory only',
  },
  {
    feature: 'Protocol',
    mirdb: 'Memcached compatible',
    memcached: 'Native Memcached',
  },
  {
    feature: 'Language',
    mirdb: 'Rust',
    memcached: 'C',
  },
  {
    feature: 'Data Survives Restart',
    mirdb: 'Yes',
    memcached: 'No',
  },
  {
    feature: 'Use Case',
    mirdb: 'Persistent caching, primary storage',
    memcached: 'Volatile caching only',
  },
];

export function HowItWorks() {
  return (
    <section id="how-it-works" className="py-16">
      <div className="max-w-6xl mx-auto px-4">
        <h2 className="text-3xl font-bold text-center mb-4 text-slate-900 dark:text-slate-100">
          How It Works
        </h2>
        <p className="text-center text-slate-600 dark:text-slate-300 mb-12 max-w-2xl mx-auto">
          MirDB uses a Log-Structured Merge (LSM) tree architecture to provide
          persistent storage while maintaining high write throughput.
        </p>

        {/* Architecture Diagram */}
        <div className="mb-16">
          <h3 className="text-xl font-semibold text-center mb-6 text-slate-800 dark:text-slate-200">
            LSM Tree Architecture
          </h3>
          <div className="flex justify-center">
            <img
              src="/images/architecture-diagram.svg"
              alt="MirDB LSM Tree Architecture: Write requests flow through Write-Ahead Log to Memtable, then to Immutable Memtables, and finally to SSTable levels through compaction"
              className="max-w-full h-auto"
              width={600}
              height={400}
              loading="lazy"
            />
          </div>
        </div>

        {/* Comparison Table */}
        <div className="mb-8">
          <h3 className="text-xl font-semibold text-center mb-6 text-slate-800 dark:text-slate-200">
            MirDB vs Memcached
          </h3>
          <div className="overflow-x-auto">
            <table className="w-full max-w-3xl mx-auto border-collapse">
              <thead>
                <tr className="bg-primary-100 dark:bg-primary-900/30">
                  <th
                    scope="col"
                    className="px-4 py-3 text-left font-semibold text-slate-900 dark:text-slate-100 border border-slate-300 dark:border-slate-600"
                  >
                    Feature
                  </th>
                  <th
                    scope="col"
                    className="px-4 py-3 text-left font-semibold text-slate-900 dark:text-slate-100 border border-slate-300 dark:border-slate-600"
                  >
                    MirDB
                  </th>
                  <th
                    scope="col"
                    className="px-4 py-3 text-left font-semibold text-slate-900 dark:text-slate-100 border border-slate-300 dark:border-slate-600"
                  >
                    Memcached
                  </th>
                </tr>
              </thead>
              <tbody>
                {comparisonData.map((row, index) => (
                  <tr
                    key={row.feature}
                    className={
                      index % 2 === 0
                        ? 'bg-white dark:bg-slate-800'
                        : 'bg-slate-50 dark:bg-slate-800/50'
                    }
                  >
                    <td className="px-4 py-3 font-medium text-slate-900 dark:text-slate-100 border border-slate-300 dark:border-slate-600">
                      {row.feature}
                    </td>
                    <td className="px-4 py-3 text-slate-700 dark:text-slate-300 border border-slate-300 dark:border-slate-600">
                      {row.mirdb}
                    </td>
                    <td className="px-4 py-3 text-slate-700 dark:text-slate-300 border border-slate-300 dark:border-slate-600">
                      {row.memcached}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </section>
  );
}
