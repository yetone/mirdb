import { COMPARISON_DATA } from '../../utils/constants';

function Cell({ value }: { value: string | boolean }) {
  if (typeof value === 'boolean') {
    if (value) {
      return (
        <span className="inline-flex items-center gap-1 text-green-600 dark:text-green-400 font-semibold">
          <svg aria-hidden="true" className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2.5}>
            <path strokeLinecap="round" strokeLinejoin="round" d="M5 13l4 4L19 7" />
          </svg>
          <span>Yes</span>
        </span>
      );
    }
    return (
      <span className="inline-flex items-center gap-1 text-gray-400 dark:text-gray-500">
        <svg aria-hidden="true" className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
          <path strokeLinecap="round" d="M6 18L18 6M6 6l12 12" />
        </svg>
        <span>No</span>
      </span>
    );
  }
  return <span className="text-sm text-gray-700 dark:text-gray-300">{value}</span>;
}

/**
 * Comparison table component.
 * Owner: Scenario 6 - Comparison Table
 *
 * Displays a feature comparison table contrasting MirDB with memcached and Redis.
 * Visual indicators for positive/negative features (not color-only).
 * Responsive: horizontal scroll on mobile.
 *
 * Requirements: REQ-8
 * Accessibility: Table has caption, proper th/td associations, color is not sole differentiator
 */
export default function Comparison() {
  return (
    <section
      id="comparison"
      aria-label="Feature Comparison"
      className="py-16 bg-gray-50 dark:bg-gray-800/50"
    >
      <div className="container mx-auto px-4">
        <h2 className="text-3xl font-bold text-center mb-8 text-gray-900 dark:text-gray-100">
          How MirDB Compares
        </h2>

        <div className="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700 shadow-sm">
          <table className="w-full min-w-[600px] border-collapse bg-white dark:bg-gray-900">
            <caption className="sr-only">
              Feature comparison of MirDB, memcached, and Redis across key dimensions
            </caption>
            <thead>
              <tr className="border-b border-gray-200 dark:border-gray-700 bg-gray-100 dark:bg-gray-800">
                <th
                  scope="col"
                  className="px-4 py-4 text-left text-sm font-semibold text-gray-700 dark:text-gray-200"
                >
                  Feature
                </th>
                <th
                  scope="col"
                  className="px-4 py-4 text-center text-sm font-semibold text-primary-600 dark:text-primary-400 bg-primary-50/50 dark:bg-primary-900/20"
                >
                  MirDB
                </th>
                <th
                  scope="col"
                  className="px-4 py-4 text-center text-sm font-semibold text-gray-700 dark:text-gray-200"
                >
                  memcached
                </th>
                <th
                  scope="col"
                  className="px-4 py-4 text-center text-sm font-semibold text-gray-700 dark:text-gray-200"
                >
                  Redis
                </th>
              </tr>
            </thead>
            <tbody>
              {COMPARISON_DATA.map((row, idx) => (
                <tr
                  key={row.dimension}
                  className={
                    idx % 2 === 0
                      ? 'bg-white dark:bg-gray-900'
                      : 'bg-gray-50/50 dark:bg-gray-800/30'
                  }
                >
                  <th
                    scope="row"
                    className="px-4 py-3 text-left text-sm font-medium text-gray-900 dark:text-gray-100"
                  >
                    {row.dimension}
                  </th>
                  <td className="px-4 py-3 text-center bg-primary-50/30 dark:bg-primary-900/10">
                    <Cell value={row.mirdb} />
                  </td>
                  <td className="px-4 py-3 text-center">
                    <Cell value={row.memcached} />
                  </td>
                  <td className="px-4 py-3 text-center">
                    <Cell value={row.redis} />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <p className="mt-4 text-xs text-center text-gray-400 dark:text-gray-500">
          Scroll horizontally to view full table on small screens
        </p>
      </div>
    </section>
  );
}
