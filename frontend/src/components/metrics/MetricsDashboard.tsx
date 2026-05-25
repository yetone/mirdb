/**
 * Real-time Metrics Dashboard component.
 *
 * Owner: Scenario 7 - Real-time Metrics Dashboard
 *
 * Displays total keys in memory, total disk storage used,
 * active compactions, and request latency with automatic updates.
 * Polls /api/metrics at 2-second intervals.
 */

import React from 'react';
import { useMetrics } from './useMetrics';
import MetricCard from './MetricCard';

/**
 * Format a number with comma separators for thousands.
 */
export function formatNumber(value: number): string {
  return value.toLocaleString('en-US');
}

/**
 * Format disk usage in MB to a human-readable string.
 */
export function formatDiskUsage(mb: number): string {
  return `${mb} MB`;
}

/**
 * Format latency in milliseconds with "avg" suffix.
 */
export function formatLatency(ms: number): string {
  return `${ms}ms avg`;
}

/**
 * Format active compactions count.
 */
export function formatCompactions(count: number): string {
  if (count === 1) return '1 active';
  return `${count} active`;
}

const MetricsDashboard: React.FC = () => {
  const { metrics, loading, error, refresh } = useMetrics();

  if (loading) {
    return (
      <section
        data-testid="metrics-dashboard"
        className="py-8 px-4"
      >
        <h2 className="text-2xl font-bold text-gray-900 dark:text-white mb-6">
          Real-time Metrics
        </h2>
        <div
          data-testid="metrics-loading"
          className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4"
        >
          {Array.from({ length: 4 }).map((_, i) => (
            <div
              key={i}
              className="bg-gray-50 dark:bg-gray-800 rounded-lg p-6 shadow-sm border border-gray-200 dark:border-gray-700 animate-pulse"
            >
              <div className="h-4 bg-gray-200 dark:bg-gray-700 rounded w-24 mb-2" />
              <div className="h-8 bg-gray-200 dark:bg-gray-700 rounded w-32" />
            </div>
          ))}
        </div>
      </section>
    );
  }

  if (error && !metrics) {
    return (
      <section
        data-testid="metrics-dashboard"
        className="py-8 px-4"
      >
        <h2 className="text-2xl font-bold text-gray-900 dark:text-white mb-6">
          Real-time Metrics
        </h2>
        <div
          data-testid="metrics-error"
          className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg p-4"
        >
          <p className="text-red-700 dark:text-red-400">{error}</p>
          <button
            onClick={refresh}
            className="mt-2 px-4 py-2 bg-red-600 text-white rounded hover:bg-red-700 transition-colors"
          >
            Retry
          </button>
        </div>
      </section>
    );
  }

  const metricsData = [
    {
      label: 'Keys in Memory',
      value: metrics ? formatNumber(metrics.total_keys_in_memory) : '0',
      testId: 'metric-keys',
    },
    {
      label: 'Disk Usage',
      value: metrics ? formatDiskUsage(metrics.total_disk_storage_mb) : '0 MB',
      testId: 'metric-disk',
    },
    {
      label: 'Active Compactions',
      value: metrics ? formatCompactions(metrics.active_compactions) : '0 active',
      testId: 'metric-compactions',
    },
    {
      label: 'Avg Latency',
      value: metrics ? formatLatency(metrics.request_latency_ms) : '0ms avg',
      testId: 'metric-latency',
    },
  ];

  return (
    <section
      data-testid="metrics-dashboard"
      className="py-8 px-4"
    >
      <div className="flex items-center justify-between mb-6">
        <h2 className="text-2xl font-bold text-gray-900 dark:text-white">
          Real-time Metrics
        </h2>
        <div className="flex items-center gap-2">
          <span
            data-testid="metrics-pulse"
            className="inline-block w-2 h-2 rounded-full bg-green-500 animate-pulse"
          />
          <span className="text-sm text-gray-500 dark:text-gray-400">
            Live
          </span>
        </div>
      </div>
      <div
        data-testid="metrics-grid"
        className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4"
      >
        {metricsData.map((metric) => (
          <MetricCard
            key={metric.testId}
            label={metric.label}
            value={metric.value}
            testId={metric.testId}
          />
        ))}
      </div>
    </section>
  );
};

export default MetricsDashboard;
