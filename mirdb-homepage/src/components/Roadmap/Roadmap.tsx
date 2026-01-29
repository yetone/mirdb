/**
 * React version of Roadmap for testing purposes.
 * This mirrors the Roadmap.astro component structure.
 * Owner: Scenario 8 - Roadmap Section
 */

import type { RoadmapItem } from '../../types/index';

// Default roadmap items matching content/roadmap.json
const defaultRoadmapItems: RoadmapItem[] = [
  {
    title: 'Tokio networking',
    description: 'Async I/O networking layer built on Tokio runtime for high-performance concurrent connections',
    status: 'completed',
  },
  {
    title: 'Memtable implementation',
    description: 'In-memory skip-list based memtable for fast key-value operations',
    status: 'completed',
  },
  {
    title: 'Minor compaction',
    description: 'Automatic flush of immutable memtables to SSTable files on disk',
    status: 'completed',
  },
  {
    title: 'Major compaction',
    description: 'Background merge of SSTables across levels for storage efficiency and read optimization',
    status: 'completed',
  },
  {
    title: 'Raft consensus',
    description: 'Distributed consensus protocol for high availability and data replication across nodes',
    status: 'planned',
  },
];

interface RoadmapItemComponentProps {
  item: RoadmapItem;
}

function CompletedIcon() {
  return (
    <svg className="w-5 h-5" fill="currentColor" viewBox="0 0 20 20" data-testid="completed-icon">
      <path
        fillRule="evenodd"
        d="M16.707 5.293a1 1 0 010 1.414l-8 8a1 1 0 01-1.414 0l-4-4a1 1 0 011.414-1.414L8 12.586l7.293-7.293a1 1 0 011.414 0z"
        clipRule="evenodd"
      />
    </svg>
  );
}

function PlannedIcon() {
  return (
    <svg className="w-5 h-5" fill="currentColor" viewBox="0 0 20 20" data-testid="planned-icon">
      <path
        fillRule="evenodd"
        d="M10 18a8 8 0 100-16 8 8 0 000 16zm1-12a1 1 0 10-2 0v4a1 1 0 00.293.707l2.828 2.829a1 1 0 101.415-1.415L11 9.586V6z"
        clipRule="evenodd"
      />
    </svg>
  );
}

function RoadmapItemComponent({ item }: RoadmapItemComponentProps) {
  const isCompleted = item.status === 'completed';
  const borderStyle = isCompleted ? 'border-solid' : 'border-dashed';
  const iconColor = isCompleted ? 'text-success' : 'text-warning';
  const testId = isCompleted ? 'roadmap-item-completed' : 'roadmap-item-planned';

  return (
    <li
      className={`flex items-start gap-3 p-4 bg-surface rounded-lg border border-border ${borderStyle}`}
      data-testid={testId}
    >
      <span
        className={`flex-shrink-0 w-6 h-6 flex items-center justify-center ${iconColor}`}
        aria-hidden="true"
      >
        {isCompleted ? <CompletedIcon /> : <PlannedIcon />}
      </span>
      <div>
        <h4 className="font-mono font-medium text-text-primary" data-testid="roadmap-item-title">
          {item.title}
        </h4>
        <p className="text-sm text-text-secondary mt-1" data-testid="roadmap-item-description">
          {item.description}
        </p>
      </div>
    </li>
  );
}

interface RoadmapProps {
  items?: RoadmapItem[];
}

export function Roadmap({ items = defaultRoadmapItems }: RoadmapProps) {
  const completedItems = items.filter((item) => item.status === 'completed');
  const plannedItems = items.filter((item) => item.status === 'planned');

  return (
    <section
      id="roadmap"
      className="roadmap-section py-16 px-4 max-w-4xl mx-auto"
      data-testid="roadmap-section"
    >
      <h2 className="text-3xl font-mono font-bold text-center mb-12 text-text-primary">
        Roadmap
      </h2>

      <div className="space-y-12">
        {/* Completed Features */}
        <div data-testid="completed-features">
          <h3 className="text-xl font-mono font-semibold mb-6 text-success flex items-center gap-2">
            <svg
              className="w-6 h-6"
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
            Completed
          </h3>
          <ul className="space-y-4" role="list" aria-label="Completed features">
            {completedItems.map((item) => (
              <RoadmapItemComponent key={item.title} item={item} />
            ))}
          </ul>
        </div>

        {/* Planned Features */}
        <div data-testid="planned-features">
          <h3 className="text-xl font-mono font-semibold mb-6 text-warning flex items-center gap-2">
            <svg
              className="w-6 h-6"
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
            Planned
          </h3>
          <ul className="space-y-4" role="list" aria-label="Planned features">
            {plannedItems.map((item) => (
              <RoadmapItemComponent key={item.title} item={item} />
            ))}
          </ul>
        </div>
      </div>
    </section>
  );
}

export default Roadmap;
