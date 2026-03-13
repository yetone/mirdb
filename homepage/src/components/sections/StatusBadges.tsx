/**
 * Status Badges Section Component
 * Owner: Scenario 6 - Status Badges Display
 *
 * Expected exports:
 * - StatusBadges: Section displaying CI/CD and project badges
 *
 * Requirements:
 * - CircleCI build status badge
 * - Badge links to CI dashboard
 * - Proper alt text for accessibility
 * - rel="noopener noreferrer" on external links
 */

export function StatusBadges() {
  return (
    <section data-testid="status-badges" className="py-12 bg-gray-50 dark:bg-gray-800">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 text-center">
        <h2 className="text-2xl font-bold text-gray-900 dark:text-white mb-6">
          Build Status
        </h2>
        <a
          href="https://circleci.com/gh/yetone/mirdb"
          target="_blank"
          rel="noopener noreferrer"
        >
          <img
            src="https://circleci.com/gh/yetone/mirdb.svg?style=shield"
            alt="CircleCI Build Status"
          />
        </a>
      </div>
    </section>
  )
}
