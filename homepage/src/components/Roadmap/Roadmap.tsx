/**
 * Roadmap section component for MirDB homepage.
 * Owner: Scenario 8 - Roadmap and Status Section
 *
 * Requirements:
 * - Display implemented features marked complete (REQ-8)
 * - Display planned features (e.g., Raft consensus)
 * - Visual distinction between complete and planned
 */

import { roadmapItems } from '../../data/roadmap'
import { RoadmapItem } from './RoadmapItem'

export function Roadmap() {
  const completedItems = roadmapItems.filter((item) => item.status === 'complete')
  const plannedItems = roadmapItems.filter((item) => item.status === 'planned')
  const inProgressItems = roadmapItems.filter((item) => item.status === 'in-progress')

  return (
    <section
      id="roadmap"
      className="py-20 px-4 sm:px-6 lg:px-8 bg-gray-900/50"
      aria-labelledby="roadmap-heading"
    >
      <div className="max-w-4xl mx-auto">
        <div className="text-center mb-12">
          <h2
            id="roadmap-heading"
            className="text-3xl sm:text-4xl font-bold text-white mb-4"
          >
            Roadmap & Status
          </h2>
          <p className="text-gray-400 max-w-2xl mx-auto text-lg">
            Track the progress of MirDB development and upcoming features
          </p>
        </div>

        {/* Implemented Features */}
        <div className="mb-10" data-testid="completed-section">
          <h3 className="text-xl font-semibold text-green-400 mb-4 flex items-center gap-2">
            <svg
              className="w-5 h-5"
              fill="currentColor"
              viewBox="0 0 20 20"
              aria-hidden="true"
            >
              <path
                fillRule="evenodd"
                d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z"
                clipRule="evenodd"
              />
            </svg>
            Implemented Features
          </h3>
          <div className="space-y-3" data-testid="completed-items">
            {completedItems.map((item) => (
              <RoadmapItem key={item.title} item={item} />
            ))}
          </div>
        </div>

        {/* In Progress Features (if any) */}
        {inProgressItems.length > 0 && (
          <div className="mb-10" data-testid="in-progress-section">
            <h3 className="text-xl font-semibold text-yellow-400 mb-4 flex items-center gap-2">
              <svg
                className="w-5 h-5"
                fill="currentColor"
                viewBox="0 0 20 20"
                aria-hidden="true"
              >
                <path
                  fillRule="evenodd"
                  d="M10 18a8 8 0 100-16 8 8 0 000 16zM9.555 7.168A1 1 0 008 8v4a1 1 0 001.555.832l3-2a1 1 0 000-1.664l-3-2z"
                  clipRule="evenodd"
                />
              </svg>
              In Progress
            </h3>
            <div className="space-y-3" data-testid="in-progress-items">
              {inProgressItems.map((item) => (
                <RoadmapItem key={item.title} item={item} />
              ))}
            </div>
          </div>
        )}

        {/* Planned Features */}
        <div data-testid="planned-section">
          <h3 className="text-xl font-semibold text-blue-400 mb-4 flex items-center gap-2">
            <svg
              className="w-5 h-5"
              fill="currentColor"
              viewBox="0 0 20 20"
              aria-hidden="true"
            >
              <path
                fillRule="evenodd"
                d="M10 18a8 8 0 100-16 8 8 0 000 16zm1-12a1 1 0 10-2 0v4a1 1 0 00.293.707l2.828 2.829a1 1 0 101.415-1.415L11 9.586V6z"
                clipRule="evenodd"
              />
            </svg>
            Planned Features
          </h3>
          <div className="space-y-3" data-testid="planned-items">
            {plannedItems.map((item) => (
              <RoadmapItem key={item.title} item={item} />
            ))}
          </div>
        </div>
      </div>
    </section>
  )
}
