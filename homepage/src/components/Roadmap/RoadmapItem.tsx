/**
 * RoadmapItem component for displaying individual roadmap features.
 * Owner: Scenario 8 - Roadmap and Status Section
 */

import type { RoadmapItem as RoadmapItemType } from '../../types'

interface RoadmapItemProps {
  item: RoadmapItemType
}

export function RoadmapItem({ item }: RoadmapItemProps) {
  const isComplete = item.status === 'complete'
  const isPlanned = item.status === 'planned'

  return (
    <article
      className={`
        flex items-start gap-4 p-4 rounded-lg border transition-colors duration-200
        ${isComplete
          ? 'bg-green-900/20 border-green-700/50 hover:border-green-600/70'
          : isPlanned
            ? 'bg-blue-900/20 border-blue-700/50 hover:border-blue-600/70'
            : 'bg-yellow-900/20 border-yellow-700/50 hover:border-yellow-600/70'
        }
      `}
      data-testid="roadmap-item"
      data-status={item.status}
    >
      <div className="flex-shrink-0 mt-1" aria-hidden="true">
        {isComplete ? (
          <svg
            className="w-6 h-6 text-green-400"
            fill="currentColor"
            viewBox="0 0 20 20"
            data-testid="complete-icon"
          >
            <path
              fillRule="evenodd"
              d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z"
              clipRule="evenodd"
            />
          </svg>
        ) : isPlanned ? (
          <svg
            className="w-6 h-6 text-blue-400"
            fill="currentColor"
            viewBox="0 0 20 20"
            data-testid="planned-icon"
          >
            <path
              fillRule="evenodd"
              d="M10 18a8 8 0 100-16 8 8 0 000 16zm1-12a1 1 0 10-2 0v4a1 1 0 00.293.707l2.828 2.829a1 1 0 101.415-1.415L11 9.586V6z"
              clipRule="evenodd"
            />
          </svg>
        ) : (
          <svg
            className="w-6 h-6 text-yellow-400"
            fill="currentColor"
            viewBox="0 0 20 20"
            data-testid="in-progress-icon"
          >
            <path
              fillRule="evenodd"
              d="M10 18a8 8 0 100-16 8 8 0 000 16zM9.555 7.168A1 1 0 008 8v4a1 1 0 001.555.832l3-2a1 1 0 000-1.664l-3-2z"
              clipRule="evenodd"
            />
          </svg>
        )}
      </div>

      <div className="flex-grow">
        <div className="flex items-center gap-3 mb-1">
          <h3 className="text-lg font-semibold text-white">
            {item.title}
          </h3>
          <span
            className={`
              px-2 py-0.5 text-xs font-medium rounded-full uppercase tracking-wide
              ${isComplete
                ? 'bg-green-900/50 text-green-300'
                : isPlanned
                  ? 'bg-blue-900/50 text-blue-300'
                  : 'bg-yellow-900/50 text-yellow-300'
              }
            `}
            data-testid="status-badge"
          >
            {isComplete ? 'Done' : isPlanned ? 'Planned' : 'In Progress'}
          </span>
        </div>
        <p className="text-gray-400 text-sm leading-relaxed">
          {item.description}
        </p>
      </div>
    </article>
  )
}
