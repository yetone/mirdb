/**
 * Comparison data for MirDB vs alternatives.
 * Owner: Scenario 6 - Comparison Section
 */

export interface ComparisonFeature {
  name: string;
  description: string;
}

export interface ComparisonProduct {
  name: string;
  description: string;
  features: Record<string, boolean | string>;
  highlighted?: boolean;
}

export const COMPARISON_FEATURES: ComparisonFeature[] = [
  {
    name: 'persistence',
    description: 'Data Persistence',
  },
  {
    name: 'protocol',
    description: 'Memcached Protocol',
  },
  {
    name: 'lsmTree',
    description: 'LSM Tree Storage',
  },
  {
    name: 'inmemory',
    description: 'In-Memory Speed',
  },
  {
    name: 'clustering',
    description: 'Clustering Support',
  },
];

export const COMPARISON_PRODUCTS: ComparisonProduct[] = [
  {
    name: 'MirDB',
    description: 'Persistent key-value store with Memcached protocol',
    highlighted: true,
    features: {
      persistence: true,
      protocol: true,
      lsmTree: true,
      inmemory: true,
      clustering: 'Planned',
    },
  },
  {
    name: 'Memcached',
    description: 'High-performance distributed memory object caching system',
    highlighted: false,
    features: {
      persistence: false,
      protocol: true,
      lsmTree: false,
      inmemory: true,
      clustering: true,
    },
  },
  {
    name: 'Redis',
    description: 'In-memory data structure store with optional persistence',
    highlighted: false,
    features: {
      persistence: true,
      protocol: false,
      lsmTree: false,
      inmemory: true,
      clustering: true,
    },
  },
];

export const COMPARISON_SECTION_CONTENT = {
  title: 'Why MirDB?',
  subtitle: 'See how MirDB compares to popular alternatives',
  description:
    'MirDB combines the simplicity of Memcached protocol with the reliability of disk persistence using LSM Tree storage.',
} as const;
