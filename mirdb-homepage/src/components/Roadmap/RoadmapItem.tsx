/**
 * Roadmap Item Component.
 * Owner: Scenario 4 - Roadmap Section
 */

import { RoadmapStatus } from '../../types/roadmap'

interface RoadmapItemProps {
  title: string
  description?: string
  status: RoadmapStatus
}

function StatusIndicator({ status }: { status: RoadmapStatus }) {
  if (status === 'complete') {
    return (
      <span
        className="flex items-center justify-center w-6 h-6 rounded-full bg-green-100 dark:bg-green-900"
        aria-label="Completed"
        data-testid="status-complete"
      >
        <span className="text-green-600 dark:text-green-400 font-bold text-sm">✓</span>
      </span>
    )
  }
  if (status === 'in-progress') {
    return (
      <span
        className="flex items-center justify-center w-6 h-6 rounded-full bg-yellow-100 dark:bg-yellow-900"
        aria-label="In Progress"
        data-testid="status-in-progress"
      >
        <span className="text-yellow-600 dark:text-yellow-400 text-sm">○</span>
      </span>
    )
  }
  return (
    <span
      className="flex items-center justify-center w-6 h-6 rounded-full bg-gray-100 dark:bg-gray-700"
      aria-label="Planned"
      data-testid="status-planned"
    >
      <span className="text-gray-500 dark:text-gray-400 text-sm">◇</span>
    </span>
  )
}

function StatusBadge({ status }: { status: RoadmapStatus }) {
  const baseClasses = 'px-2 py-0.5 text-xs font-medium rounded-full'

  if (status === 'complete') {
    return (
      <span className={`${baseClasses} bg-green-100 dark:bg-green-900 text-green-700 dark:text-green-300`}>
        Complete
      </span>
    )
  }
  if (status === 'in-progress') {
    return (
      <span className={`${baseClasses} bg-yellow-100 dark:bg-yellow-900 text-yellow-700 dark:text-yellow-300`}>
        In Progress
      </span>
    )
  }
  return (
    <span className={`${baseClasses} bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-300`}>
      Planned
    </span>
  )
}

export function RoadmapItem({ title, description, status }: RoadmapItemProps) {
  return (
    <div
      className="flex items-start gap-4 p-4 bg-white dark:bg-gray-800 rounded-lg border border-gray-100 dark:border-gray-700"
      data-testid="roadmap-item"
    >
      <StatusIndicator status={status} />
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-3 flex-wrap">
          <h3
            className="text-base font-semibold text-gray-900 dark:text-white"
            data-testid="roadmap-item-title"
          >
            {title}
          </h3>
          <StatusBadge status={status} />
        </div>
        {description && (
          <p
            className="mt-1 text-sm text-gray-600 dark:text-gray-300"
            data-testid="roadmap-item-description"
          >
            {description}
          </p>
        )}
      </div>
    </div>
  )
}
