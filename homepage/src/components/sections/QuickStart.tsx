/**
 * Quick Start Section Component
 * Owner: Scenario 5 - Quick Start Section
 *
 * Expected exports:
 * - QuickStart: Section with code example and copy functionality
 *
 * Requirements:
 * - Display MirDB command examples
 * - Syntax highlighting for code
 * - Copy to clipboard button
 * - Visual feedback on successful copy
 */

export function QuickStart() {
  return (
    <section data-testid="quickstart" className="py-20 bg-white dark:bg-gray-900">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <h2 className="text-3xl font-bold text-center text-gray-900 dark:text-white mb-12">
          Quick Start
        </h2>
        <div className="bg-gray-900 rounded-lg p-6 max-w-2xl mx-auto">
          <pre className="text-green-400">
            <code>$ mirdb-server --port 11211</code>
          </pre>
        </div>
      </div>
    </section>
  )
}
