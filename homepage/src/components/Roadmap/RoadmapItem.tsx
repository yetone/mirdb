/**
 * RoadmapItem Component
 * Owner: Scenario 7 - Planned Features Section
 *
 * Renders individual roadmap items with visual status indicators.
 * Completed items show a checkmark, planned items show a clock/planned icon.
 */
import type { RoadmapItem as RoadmapItemType } from '../../types';

export interface RoadmapItemProps {
  item: RoadmapItemType;
}

export function RoadmapItem({ item }: RoadmapItemProps) {
  const isCompleted = item.status === 'completed';

  return (
    <li
      className={`roadmap-item roadmap-item--${item.status}`}
      data-testid="roadmap-item"
      data-status={item.status}
    >
      <span
        className={`roadmap-status-indicator roadmap-status-indicator--${item.status}`}
        aria-hidden="true"
      >
        {isCompleted ? (
          <svg
            className="roadmap-icon roadmap-icon--check"
            viewBox="0 0 20 20"
            fill="currentColor"
            aria-hidden="true"
          >
            <path
              fillRule="evenodd"
              d="M16.707 5.293a1 1 0 010 1.414l-8 8a1 1 0 01-1.414 0l-4-4a1 1 0 011.414-1.414L8 12.586l7.293-7.293a1 1 0 011.414 0z"
              clipRule="evenodd"
            />
          </svg>
        ) : (
          <svg
            className="roadmap-icon roadmap-icon--planned"
            viewBox="0 0 20 20"
            fill="currentColor"
            aria-hidden="true"
          >
            <circle cx="10" cy="10" r="8" fill="none" stroke="currentColor" strokeWidth="2" />
          </svg>
        )}
      </span>
      <div className="roadmap-item-content">
        <span className="roadmap-item-title">{item.title}</span>
        {item.description && (
          <span className="roadmap-item-description">{item.description}</span>
        )}
        <span className={`roadmap-item-badge roadmap-item-badge--${item.status}`}>
          {isCompleted ? 'Completed' : 'Planned'}
        </span>
      </div>
    </li>
  );
}
