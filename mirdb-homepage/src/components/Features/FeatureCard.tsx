/**
 * Feature Card Component.
 * Owner: Scenario 3 - Features Section
 */

import { FeatureStatus } from '../../types/features'

interface FeatureCardProps {
  icon: string
  title: string
  description: string
  status: FeatureStatus
}

function StatusIndicator({ status }: { status: FeatureStatus }) {
  if (status === 'complete') {
    return (
      <span
        className="text-green-500 font-bold"
        aria-label="Completed"
        data-testid="status-complete"
      >
        ✓
      </span>
    )
  }
  if (status === 'in-progress') {
    return (
      <span
        className="text-yellow-500"
        aria-label="In Progress"
        data-testid="status-in-progress"
      >
        ○
      </span>
    )
  }
  return (
    <span
      className="text-gray-400"
      aria-label="Planned"
      data-testid="status-planned"
    >
      ◇
    </span>
  )
}

export function FeatureCard({ icon, title, description, status }: FeatureCardProps) {
  return (
    <div
      className="p-6 bg-white dark:bg-gray-800 rounded-lg shadow-md border border-gray-100 dark:border-gray-700 hover:shadow-lg transition-shadow"
      data-testid="feature-card"
    >
      <div className="flex items-start justify-between mb-4">
        <span className="text-3xl" data-testid="feature-icon" role="img" aria-hidden="true">
          {icon}
        </span>
        <StatusIndicator status={status} />
      </div>
      <h3
        className="text-lg font-semibold text-gray-900 dark:text-white mb-2"
        data-testid="feature-title"
      >
        {title}
      </h3>
      <p
        className="text-gray-600 dark:text-gray-300 text-sm"
        data-testid="feature-description"
      >
        {description}
      </p>
    </div>
  )
}
