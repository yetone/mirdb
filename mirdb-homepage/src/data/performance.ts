/**
 * Performance Metrics Data
 * Owner: Scenario 5 - Performance Metrics Display
 *
 * Contains benchmark data for MirDB performance metrics
 * including throughput, latency, and memory usage with
 * comparisons to other databases.
 */

import { PerformanceMetric } from '../types';

export interface PerformanceData {
  metrics: PerformanceMetric[];
  databases: DatabaseComparison[];
}

export interface DatabaseComparison {
  id: string;
  name: string;
  description: string;
  metrics: {
    throughput: number;
    latency: number;
    memory: number;
  };
}

export const performanceMetrics: PerformanceMetric[] = [
  {
    id: 'throughput',
    label: 'Throughput',
    value: 150000,
    unit: 'ops/sec',
    comparison: {
      name: 'LevelDB',
      value: 85000,
    },
  },
  {
    id: 'latency',
    label: 'Latency',
    value: 0.12,
    unit: 'ms',
    comparison: {
      name: 'LevelDB',
      value: 0.25,
    },
  },
  {
    id: 'memory',
    label: 'Memory Usage',
    value: 128,
    unit: 'MB',
    comparison: {
      name: 'LevelDB',
      value: 256,
    },
  },
];

export const databaseComparisons: DatabaseComparison[] = [
  {
    id: 'mirdb',
    name: 'MirDB',
    description: 'High-performance Rust key-value store',
    metrics: {
      throughput: 150000,
      latency: 0.12,
      memory: 128,
    },
  },
  {
    id: 'leveldb',
    name: 'LevelDB',
    description: 'LSM-tree based key-value store by Google',
    metrics: {
      throughput: 85000,
      latency: 0.25,
      memory: 256,
    },
  },
];

export const performanceData: PerformanceData = {
  metrics: performanceMetrics,
  databases: databaseComparisons,
};
