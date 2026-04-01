/**
 * Status Badges Component.
 * Owner: Scenario 5 - Project Status Badges
 *
 * Displays CircleCI build status badge with:
 * - Badge image from CircleCI
 * - Link to CI/CD status page
 * - Accessible alt text
 */

import { CIRCLECI_BADGE_URL, CIRCLECI_STATUS_URL } from '../../utils/constants'

export interface StatusBadgesProps {
  className?: string
}

export const StatusBadges: React.FC<StatusBadgesProps> = ({ className = '' }) => {
  return (
    <div className={`flex items-center gap-2 ${className}`} data-testid="status-badges">
      <a
        href={CIRCLECI_STATUS_URL}
        target="_blank"
        rel="noopener noreferrer"
        aria-label="View CircleCI build status"
        data-testid="circleci-badge-link"
      >
        <img
          src={CIRCLECI_BADGE_URL}
          alt="Build Status"
          className="h-5"
          data-testid="circleci-badge-image"
        />
      </a>
    </div>
  )
}

export default StatusBadges
