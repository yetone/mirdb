/**
 * Roadmap section component.
 * Owner: Scenario 7 - Roadmap Section
 *
 * Displays a timeline of completed and planned features with visual distinction:
 * - Completed items: checkmark icon, green color
 * - Planned items: circle icon, different styling
 */

import React from 'react';
import { roadmapItems } from '../../data/roadmap';
import type { RoadmapItem } from '../../types';

interface RoadmapItemProps {
  item: RoadmapItem;
  isLast: boolean;
}

const RoadmapItemComponent: React.FC<RoadmapItemProps> = ({ item, isLast }) => {
  return (
    <div className="relative flex items-start" data-testid="roadmap-item">
      {/* Vertical line connecting items */}
      {!isLast && (
        <div
          className="absolute left-4 top-8 w-0.5 h-full bg-gray-200 dark:bg-gray-700"
          aria-hidden="true"
        />
      )}

      {/* Status indicator */}
      <div
        className={`relative z-10 flex items-center justify-center w-8 h-8 rounded-full shrink-0 ${
          item.completed
            ? 'bg-green-100 dark:bg-green-900'
            : 'bg-blue-100 dark:bg-blue-900'
        }`}
        data-testid={item.completed ? 'completed-indicator' : 'planned-indicator'}
      >
        {item.completed ? (
          <svg
            className="w-5 h-5 text-green-600 dark:text-green-400"
            fill="currentColor"
            viewBox="0 0 20 20"
            aria-hidden="true"
            data-testid="checkmark-icon"
          >
            <path
              fillRule="evenodd"
              d="M16.707 5.293a1 1 0 010 1.414l-8 8a1 1 0 01-1.414 0l-4-4a1 1 0 011.414-1.414L8 12.586l7.293-7.293a1 1 0 011.414 0z"
              clipRule="evenodd"
            />
          </svg>
        ) : (
          <svg
            className="w-5 h-5 text-blue-600 dark:text-blue-400"
            fill="currentColor"
            viewBox="0 0 20 20"
            aria-hidden="true"
            data-testid="planned-icon"
          >
            <circle cx="10" cy="10" r="4" />
          </svg>
        )}
      </div>

      {/* Content */}
      <div className="ml-4 pb-8">
        <h3
          className={`text-lg font-semibold ${
            item.completed
              ? 'text-gray-900 dark:text-white'
              : 'text-blue-600 dark:text-blue-400'
          }`}
          data-testid="roadmap-item-name"
        >
          {item.name}
        </h3>
        {item.description && (
          <p className="mt-1 text-gray-600 dark:text-gray-400">{item.description}</p>
        )}
        <span
          className={`inline-block mt-2 px-2 py-0.5 text-xs font-medium rounded ${
            item.completed
              ? 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200'
              : 'bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200'
          }`}
          data-testid="status-badge"
        >
          {item.completed ? 'Completed' : 'Planned'}
        </span>
      </div>
    </div>
  );
};

export const Roadmap: React.FC = () => {
  const completedCount = roadmapItems.filter((item) => item.completed).length;

  return (
    <section
      id="roadmap"
      className="py-20 px-4 bg-white dark:bg-gray-800"
      aria-labelledby="roadmap-heading"
    >
      <div className="max-w-3xl mx-auto">
        <h2
          id="roadmap-heading"
          className="text-3xl md:text-4xl font-bold text-center mb-4 text-gray-900 dark:text-white"
        >
          Roadmap
        </h2>
        <p className="text-center text-gray-600 dark:text-gray-400 mb-12 max-w-2xl mx-auto">
          Track the progress of MirDB's development. {completedCount} features completed,
          with more on the way.
        </p>

        {/* Timeline */}
        <div className="relative">
          {roadmapItems.map((item, index) => (
            <RoadmapItemComponent
              key={item.name}
              item={item}
              isLast={index === roadmapItems.length - 1}
            />
          ))}
        </div>
      </div>
    </section>
  );
};
