import React from 'react';
import { CheckCircle, Circle, Check, Clock } from 'lucide-react';
import type { RoadmapItem } from '../../types';
import {
  CIRCLECI_BADGE_URL,
  CIRCLECI_PROJECT_URL,
  ROADMAP_ITEMS,
} from '../../utils/constants';

function RoadmapStatusIcon({ status }: { status: RoadmapItem['status'] }) {
  if (status === 'completed') {
    return (
      <span className="flex items-center gap-1.5 text-green-600 dark:text-green-400" aria-hidden="true">
        <Check size={18} />
        <span className="text-sm font-medium">Completed</span>
      </span>
    );
  }
  return (
    <span className="flex items-center gap-1.5 text-amber-600 dark:text-amber-400" aria-hidden="true">
      <Clock size={18} />
      <span className="text-sm font-medium">Planned</span>
    </span>
  );
}

export default function Roadmap() {
  const completedItems = ROADMAP_ITEMS.filter((item) => item.status === 'completed');
  const plannedItems = ROADMAP_ITEMS.filter((item) => item.status === 'planned');

  return (
    <section
      id="roadmap"
      aria-label="Project Roadmap and Build Status"
      className="py-16 px-4 max-w-5xl mx-auto"
    >
      <h2 className="text-3xl font-bold text-center mb-10 text-gray-900 dark:text-gray-100">
        Roadmap &amp; Project Status
      </h2>

      {/* CircleCI Build Status Badge */}
      <div className="flex flex-col items-center mb-12">
        <h3 className="text-lg font-semibold mb-3 text-gray-700 dark:text-gray-300">
          Current Build Status
        </h3>
        <a
          href={CIRCLECI_PROJECT_URL}
          target="_blank"
          rel="noopener noreferrer"
          aria-label="View CircleCI build status for MirDB"
          className="inline-block hover:opacity-80 focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500 focus-visible:ring-offset-2 rounded transition-opacity"
        >
          <img
            src={CIRCLECI_BADGE_URL}
            alt="CircleCI build status"
            className="h-5"
          />
        </a>
        <p className="mt-2 text-sm text-gray-500 dark:text-gray-400">
          Click the badge to view build history on CircleCI
        </p>
      </div>

      {/* Completed Items */}
      <div className="mb-10">
        <h3 className="text-xl font-semibold mb-4 text-gray-800 dark:text-gray-200 flex items-center gap-2">
          <CheckCircle className="text-green-600 dark:text-green-400" size={22} aria-hidden="true" />
          Completed
        </h3>
        <ul className="space-y-3" role="list">
          {completedItems.map((item) => (
            <li
              key={item.id}
              className="flex items-start gap-3 p-3 rounded-lg bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800"
            >
              <RoadmapStatusIcon status="completed" />
              <div>
                <span className="font-medium text-gray-900 dark:text-gray-100">
                  {item.title}
                </span>
                <p className="text-sm text-gray-600 dark:text-gray-400 mt-0.5">
                  {item.description}
                </p>
              </div>
            </li>
          ))}
        </ul>
      </div>

      {/* Planned Items */}
      <div>
        <h3 className="text-xl font-semibold mb-4 text-gray-800 dark:text-gray-200 flex items-center gap-2">
          <Circle className="text-amber-600 dark:text-amber-400" size={22} aria-hidden="true" />
          Planned
        </h3>
        <ul className="space-y-3" role="list">
          {plannedItems.map((item) => (
            <li
              key={item.id}
              className="flex items-start gap-3 p-3 rounded-lg bg-amber-50 dark:bg-amber-900/20 border border-amber-200 dark:border-amber-800"
            >
              <RoadmapStatusIcon status="planned" />
              <div>
                <span className="font-medium text-gray-900 dark:text-gray-100">
                  {item.title}
                </span>
                <p className="text-sm text-gray-600 dark:text-gray-400 mt-0.5">
                  {item.description}
                </p>
              </div>
            </li>
          ))}
        </ul>
      </div>
    </section>
  );
}
