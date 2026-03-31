/**
 * Status and Roadmap Section Component
 * Owner: Scenario 5 - Status and Roadmap
 *
 * Displays project status and roadmap:
 * - Checklist of implemented features
 * - Timeline of planned features
 * - Progress indicators
 *
 * Requirements: REQ-5, Story 2 (Feature Understanding)
 */

import { Check, Clock, Target } from 'lucide-react';
import type { RoadmapItem } from '../../types';
import { Card } from '../ui/Card';
import { Badge } from '../ui/Badge';

interface StatusSectionProps {
  items?: RoadmapItem[];
}

const defaultRoadmapItems: RoadmapItem[] = [
  {
    id: 'memcached',
    title: 'Memcached Protocol',
    description: 'Full compatibility with the Memcached text protocol for seamless integration.',
    status: 'completed',
    quarter: 'Q1 2024',
  },
  {
    id: 'persistence',
    title: 'Persistence & SSTable',
    description: 'Durable storage using LSM-tree architecture with SSTable file format.',
    status: 'completed',
    quarter: 'Q2 2024',
  },
  {
    id: 'compaction',
    title: 'Compaction',
    description: 'Automatic background compaction to optimize storage and read performance.',
    status: 'completed',
    quarter: 'Q3 2024',
  },
  {
    id: 'raft',
    title: 'Raft Consensus',
    description: 'Distributed consensus for high availability and fault tolerance.',
    status: 'planned',
    quarter: 'Q1 2025',
  },
];

function RoadmapItemCard({ item }: { item: RoadmapItem }) {
  const isCompleted = item.status === 'completed';
  const isInProgress = item.status === 'in-progress';
  const isPlanned = item.status === 'planned';

  const getStatusIcon = () => {
    if (isCompleted) return <Check className="w-5 h-5" aria-hidden="true" />;
    if (isInProgress) return <Clock className="w-5 h-5" aria-hidden="true" />;
    return <Target className="w-5 h-5" aria-hidden="true" />;
  };

  const getStatusBadge = () => {
    if (isCompleted) {
      return (
        <Badge variant="success" data-testid={`status-${item.id}`}>
          <Check className="w-3 h-3 mr-1" aria-hidden="true" />
          <span>Implemented</span>
        </Badge>
      );
    }
    if (isInProgress) {
      return (
        <Badge variant="info" data-testid={`status-${item.id}`}>
          <Clock className="w-3 h-3 mr-1" aria-hidden="true" />
          <span>In Progress</span>
        </Badge>
      );
    }
    return (
      <Badge variant="warning" data-testid={`status-${item.id}`}>
        <Target className="w-3 h-3 mr-1" aria-hidden="true" />
        <span>Planned</span>
      </Badge>
    );
  };

  const getStatusColor = () => {
    if (isCompleted) return 'bg-green-500';
    if (isInProgress) return 'bg-blue-500';
    return 'bg-yellow-500';
  };

  return (
    <div
      className="relative flex gap-4"
      data-testid={`roadmap-item-${item.id}`}
    >
      {/* Timeline connector */}
      <div className="flex flex-col items-center">
        <div
          className={`w-10 h-10 rounded-full flex items-center justify-center text-white ${getStatusColor()}`}
          data-testid={`timeline-icon-${item.id}`}
        >
          {getStatusIcon()}
        </div>
        <div className="w-0.5 h-full bg-gray-300 dark:bg-gray-600 mt-2" aria-hidden="true" />
      </div>

      {/* Content */}
      <Card className="flex-1 mb-6">
        <div className="flex items-start justify-between mb-2">
          <div>
            <h3 className="text-lg font-semibold text-gray-900 dark:text-gray-100">
              {item.title}
            </h3>
            {item.quarter && (
              <p className="text-sm text-gray-500 dark:text-gray-400">{item.quarter}</p>
            )}
          </div>
          {getStatusBadge()}
        </div>
        <p className="text-gray-600 dark:text-gray-400 text-sm leading-relaxed">
          {item.description}
        </p>
      </Card>
    </div>
  );
}

function ProgressIndicator({ items }: { items: RoadmapItem[] }) {
  const completedCount = items.filter((item) => item.status === 'completed').length;
  const totalCount = items.length;
  const progressPercent = Math.round((completedCount / totalCount) * 100);

  return (
    <div className="mb-12" data-testid="progress-indicator">
      <div className="flex items-center justify-between mb-2">
        <span className="text-sm font-medium text-gray-700 dark:text-gray-300">
          Project Progress
        </span>
        <span className="text-sm font-medium text-gray-700 dark:text-gray-300">
          {completedCount} of {totalCount} milestones completed
        </span>
      </div>
      <div
        className="w-full bg-gray-200 dark:bg-gray-700 rounded-full h-3"
        role="progressbar"
        aria-valuenow={progressPercent}
        aria-valuemin={0}
        aria-valuemax={100}
        aria-label={`${progressPercent}% of milestones completed`}
      >
        <div
          className="bg-gradient-to-r from-green-500 to-green-600 h-3 rounded-full transition-all duration-500"
          style={{ width: `${progressPercent}%` }}
          data-testid="progress-bar-fill"
        />
      </div>
      <div className="flex justify-between mt-2 text-xs text-gray-500 dark:text-gray-400">
        <span>0%</span>
        <span data-testid="progress-percent">{progressPercent}%</span>
        <span>100%</span>
      </div>
    </div>
  );
}

function FeatureChecklist({ items }: { items: RoadmapItem[] }) {
  const implementedItems = items.filter((item) => item.status === 'completed');
  const plannedItems = items.filter((item) => item.status !== 'completed');

  return (
    <div className="grid md:grid-cols-2 gap-8 mb-12" data-testid="feature-checklist">
      {/* Implemented Features */}
      <div>
        <h3 className="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-4 flex items-center gap-2">
          <Check className="w-5 h-5 text-green-500" aria-hidden="true" />
          Implemented Features
        </h3>
        <ul className="space-y-3" data-testid="implemented-features-list">
          {implementedItems.map((item) => (
            <li
              key={item.id}
              className="flex items-center gap-3 text-gray-700 dark:text-gray-300"
              data-testid={`implemented-${item.id}`}
            >
              <div className="w-5 h-5 rounded-full bg-green-500 flex items-center justify-center flex-shrink-0">
                <Check className="w-3 h-3 text-white" aria-hidden="true" />
              </div>
              <span>{item.title}</span>
            </li>
          ))}
        </ul>
      </div>

      {/* Planned Features */}
      <div>
        <h3 className="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-4 flex items-center gap-2">
          <Target className="w-5 h-5 text-yellow-500" aria-hidden="true" />
          Upcoming Features
        </h3>
        <ul className="space-y-3" data-testid="planned-features-list">
          {plannedItems.map((item) => (
            <li
              key={item.id}
              className="flex items-center gap-3 text-gray-700 dark:text-gray-300"
              data-testid={`planned-${item.id}`}
            >
              <div className="w-5 h-5 rounded-full bg-yellow-500 flex items-center justify-center flex-shrink-0">
                <Target className="w-3 h-3 text-white" aria-hidden="true" />
              </div>
              <span>{item.title}</span>
            </li>
          ))}
        </ul>
      </div>
    </div>
  );
}

export function StatusSection({ items = defaultRoadmapItems }: StatusSectionProps) {
  return (
    <section
      id="status"
      className="py-16 px-4 sm:px-6 lg:px-8 bg-white dark:bg-gray-800"
      aria-labelledby="status-heading"
      data-testid="status-section"
    >
      <div className="max-w-4xl mx-auto">
        <div className="text-center mb-12">
          <h2
            id="status-heading"
            className="text-3xl font-bold text-gray-900 dark:text-gray-100 sm:text-4xl"
          >
            Project Status & Roadmap
          </h2>
          <p className="mt-4 text-lg text-gray-600 dark:text-gray-400 max-w-2xl mx-auto">
            Track our progress as we build a robust, feature-complete key-value store.
          </p>
        </div>

        {/* Progress Indicator */}
        <ProgressIndicator items={items} />

        {/* Feature Checklist */}
        <FeatureChecklist items={items} />

        {/* Timeline */}
        <div>
          <h3 className="text-xl font-semibold text-gray-900 dark:text-gray-100 mb-6 text-center">
            Development Timeline
          </h3>
          <div className="relative" data-testid="roadmap-timeline">
            {items.map((item, index) => (
              <RoadmapItemCard key={item.id} item={item} />
            ))}
          </div>
        </div>
      </div>
    </section>
  );
}
