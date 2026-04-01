/**
 * Status Badges Component.
 * Owner: Scenario 5 - Project Status Badges
 * Error Handling: Scenario 16 - Error Handling Missing Assets
 *
 * Displays CircleCI build status badge with:
 * - Badge image from CircleCI
 * - Link to CI/CD status page
 * - Accessible alt text
 * - Graceful fallback when badge fails to load
 */

import { useState, useCallback } from 'react'
import { CIRCLECI_BADGE_URL, CIRCLECI_STATUS_URL } from '../../utils/constants'

export interface StatusBadgesProps {
  className?: string
}

export const StatusBadges: React.FC<StatusBadgesProps> = ({ className = '' }) => {
  const [badgeError, setBadgeError] = useState(false)

  const handleBadgeError = useCallback(() => {
    setBadgeError(true)
  }, [])

  return (
    <div className={`flex items-center gap-2 ${className}`} data-testid="status-badges">
      <a
        href={CIRCLECI_STATUS_URL}
        target="_blank"
        rel="noopener noreferrer"
        aria-label="View CircleCI build status"
        data-testid="circleci-badge-link"
      >
        {badgeError ? (
          <span
            className="inline-flex items-center px-2.5 py-0.5 rounded text-xs font-medium bg-gray-200 dark:bg-gray-700 text-gray-600 dark:text-gray-300"
            data-testid="circleci-badge-fallback"
          >
            Build Status
          </span>
        ) : (
          <img
            src={CIRCLECI_BADGE_URL}
            alt="Build Status"
            className="h-5"
            data-testid="circleci-badge-image"
            onError={handleBadgeError}
          />
        )}
      </a>
    </div>
  )
}

export default StatusBadges
