/**
 * Features Section Component
 * Owner: Scenario 4 - Features Section Display
 *
 * Expected exports:
 * - Features: Grid of feature cards
 *
 * Required features (minimum 4):
 * - Persistent Key-Value Storage
 * - Memcached Protocol Compatibility
 * - LSM-tree Implementation (with memtable/SSTable)
 * - Write-Ahead Logging (durability)
 * - Atomic Compaction (minor and major)
 */

export function Features() {
  return (
    <section data-testid="features" className="py-20 bg-gray-50 dark:bg-gray-800">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <h2 className="text-3xl font-bold text-center text-gray-900 dark:text-white mb-12">
          Features
        </h2>
        <div className="grid md:grid-cols-2 lg:grid-cols-3 gap-8">
          <div className="bg-white dark:bg-gray-700 p-6 rounded-lg shadow">
            <h3 className="text-lg font-semibold text-gray-900 dark:text-white">Persistent Storage</h3>
            <p className="text-gray-600 dark:text-gray-300">Durable key-value storage</p>
          </div>
        </div>
      </div>
    </section>
  )
}
