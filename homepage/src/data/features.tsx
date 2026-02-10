/**
 * Features Data
 * Owner: Scenario 2 - Features Section Display
 *
 * Contains the data for all feature cards.
 */

import type { Feature } from '../types';

/**
 * Array of MirDB core features to display on the homepage.
 * Each feature has an id, title, description, and icon (as JSX/SVG).
 */
export const features: Omit<Feature, 'icon'>[] = [
  {
    id: 'tokio-memcached',
    title: 'Tokio with Memcached Protocol',
    description:
      'Built on Tokio for high-performance async networking. Compatible with standard memcached clients using the text protocol.',
  },
  {
    id: 'memtable-skiplist',
    title: 'Memtable with Skiplist',
    description:
      'In-memory data structure using skip lists for efficient ordered key-value operations with O(log n) complexity.',
  },
  {
    id: 'minor-compaction',
    title: 'Minor Compaction',
    description:
      'Automatically flushes memtable data to disk as SSTables when memory threshold is reached, ensuring durability.',
  },
  {
    id: 'major-compaction',
    title: 'Major Compaction',
    description:
      'LSM-tree level compaction merges and compacts SSTables across levels for optimized storage and read performance.',
  },
];

/**
 * Feature icon components mapped by feature id
 */
export const featureIcons: Record<string, React.ReactNode> = {
  'tokio-memcached': (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      width="32"
      height="32"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      <path d="M22 12h-4l-3 9L9 3l-3 9H2" />
    </svg>
  ),
  'memtable-skiplist': (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      width="32"
      height="32"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      <rect x="3" y="3" width="7" height="7" />
      <rect x="14" y="3" width="7" height="7" />
      <rect x="14" y="14" width="7" height="7" />
      <rect x="3" y="14" width="7" height="7" />
    </svg>
  ),
  'minor-compaction': (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      width="32"
      height="32"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" />
      <polyline points="7 10 12 15 17 10" />
      <line x1="12" y1="15" x2="12" y2="3" />
    </svg>
  ),
  'major-compaction': (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      width="32"
      height="32"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      <polyline points="22 12 18 12 15 21 9 3 6 12 2 12" />
    </svg>
  ),
};

/**
 * Get a feature with its icon included
 */
export const getFeaturesWithIcons = (): Feature[] => {
  return features.map((feature) => ({
    ...feature,
    icon: featureIcons[feature.id] || null,
  }));
};
