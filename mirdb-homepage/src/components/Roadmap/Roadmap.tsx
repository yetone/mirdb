/**
 * Roadmap Section Component.
 * Owner: Scenario 4 - Roadmap Section
 */

import { roadmapItems } from '../../data/roadmap'
import { RoadmapItem } from './RoadmapItem'

export function Roadmap() {
  const completedItems = roadmapItems.filter((item) => item.status === 'complete')
  const inProgressItems = roadmapItems.filter((item) => item.status === 'in-progress')
  const plannedItems = roadmapItems.filter((item) => item.status === 'planned')

  return (
    <section
      id="roadmap"
      className="py-16 px-4 sm:px-6 lg:px-8 bg-white dark:bg-gray-800"
      aria-labelledby="roadmap-heading"
      data-testid="roadmap-section"
    >
      <div className="max-w-4xl mx-auto">
        <div className="text-center mb-12">
          <h2
            id="roadmap-heading"
            className="text-3xl font-bold text-gray-900 dark:text-white mb-4"
          >
            Roadmap
          </h2>
          <p className="text-gray-600 dark:text-gray-300 max-w-2xl mx-auto">
            Track the progress of MirDB development and upcoming features
          </p>
        </div>

        <div className="space-y-8" data-testid="roadmap-list">
          {/* Completed Items */}
          {completedItems.length > 0 && (
            <div>
              <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4 flex items-center gap-2">
                <span className="text-green-500">✓</span> Completed
              </h3>
              <div className="space-y-3">
                {completedItems.map((item) => (
                  <RoadmapItem
                    key={item.id}
                    title={item.title}
                    description={item.description}
                    status={item.status}
                  />
                ))}
              </div>
            </div>
          )}

          {/* In Progress Items */}
          {inProgressItems.length > 0 && (
            <div>
              <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4 flex items-center gap-2">
                <span className="text-yellow-500">○</span> In Progress
              </h3>
              <div className="space-y-3">
                {inProgressItems.map((item) => (
                  <RoadmapItem
                    key={item.id}
                    title={item.title}
                    description={item.description}
                    status={item.status}
                  />
                ))}
              </div>
            </div>
          )}

          {/* Planned Items */}
          {plannedItems.length > 0 && (
            <div>
              <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4 flex items-center gap-2">
                <span className="text-gray-400">◇</span> Planned
              </h3>
              <div className="space-y-3">
                {plannedItems.map((item) => (
                  <RoadmapItem
                    key={item.id}
                    title={item.title}
                    description={item.description}
                    status={item.status}
                  />
                ))}
              </div>
            </div>
          )}
        </div>
      </div>
    </section>
  )
}
