/**
 * Real-time metrics dashboard component
 *
 * Owner: Scenario 2 - Real-time System Metrics Dashboard
 *
 * Features:
 * - Displays total keys, memory usage, hit rate, connections
 * - Auto-updates every 5 seconds via polling
 * - Loading and error states
 */

import React from 'react';
import { MetricCard } from './MetricCard';
import { useMetrics } from '../../hooks/useMetrics';
import {
  formatNumber,
  formatBytes,
  formatPercent,
  formatDuration,
} from '../../utils/format';
import type { MetricsResponse } from '../../types/api';

export interface MetricsDashboardProps {
  /** Polling interval in milliseconds. Default: 5000 */
  pollingInterval?: number;
  /** Whether to enable auto-refresh. Default: true */
  autoRefresh?: boolean;
  /** Optional: pre-loaded metrics data (useful for testing) */
  initialData?: MetricsResponse;
  /** Custom class name */
  className?: string;
}

/**
 * Dashboard component displaying real-time system metrics.
 * Automatically polls for updates every 5 seconds.
 */
export function MetricsDashboard({
  pollingInterval = 5000,
  autoRefresh = true,
  initialData,
  className = '',
}: MetricsDashboardProps): React.ReactElement {
  const { data, loading, error, lastUpdated, refresh } = useMetrics({
    pollingInterval,
    autoRefresh,
  });

  // Use initialData if provided (for testing), otherwise use fetched data
  const metrics = initialData || data;

  if (loading && !metrics) {
    return (
      <div
        className={`metrics-dashboard metrics-dashboard--loading ${className}`}
        data-testid="metrics-dashboard-loading"
        style={{ padding: '2rem', textAlign: 'center' }}
      >
        <div className="loading-spinner" aria-label="Loading metrics">
          Loading metrics...
        </div>
      </div>
    );
  }

  if (error && !metrics) {
    return (
      <div
        className={`metrics-dashboard metrics-dashboard--error ${className}`}
        data-testid="metrics-dashboard-error"
        style={{ padding: '2rem', textAlign: 'center', color: '#dc2626' }}
      >
        <p>Failed to load metrics: {error}</p>
        <button
          onClick={refresh}
          style={{
            marginTop: '1rem',
            padding: '0.5rem 1rem',
            cursor: 'pointer',
          }}
        >
          Retry
        </button>
      </div>
    );
  }

  if (!metrics) {
    return (
      <div
        className={`metrics-dashboard metrics-dashboard--empty ${className}`}
        data-testid="metrics-dashboard-empty"
      >
        No metrics data available
      </div>
    );
  }

  return (
    <div
      className={`metrics-dashboard ${className}`}
      data-testid="metrics-dashboard"
      style={{ padding: '1.5rem' }}
    >
      <div
        className="metrics-dashboard__header"
        style={{
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          marginBottom: '1.5rem',
        }}
      >
        <h2 style={{ margin: 0, fontSize: '1.5rem' }}>System Metrics</h2>
        {lastUpdated && (
          <span
            className="metrics-dashboard__last-updated"
            style={{ fontSize: '0.875rem', color: 'var(--text-secondary, #666)' }}
          >
            Last updated: {lastUpdated.toLocaleTimeString()}
          </span>
        )}
      </div>

      <div
        className="metrics-dashboard__grid"
        data-testid="metrics-grid"
        style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))',
          gap: '1rem',
        }}
      >
        <MetricCard
          label="Total Keys"
          value={formatNumber(metrics.total_keys)}
          secondaryValue="keys"
          testId="metric-total-keys"
        />

        <MetricCard
          label="Memory Usage"
          value={formatBytes(metrics.memory_used_bytes)}
          secondaryValue={`/ ${formatBytes(metrics.memory_available_bytes)}`}
          testId="metric-memory"
        />

        <MetricCard
          label="Cache Hit Rate"
          value={formatPercent(metrics.cache_hit_rate)}
          secondaryValue="hit rate"
          testId="metric-hit-rate"
        />

        <MetricCard
          label="Active Connections"
          value={formatNumber(metrics.active_connections)}
          secondaryValue="connections"
          testId="metric-connections"
        />

        <MetricCard
          label="Uptime"
          value={formatDuration(metrics.uptime_seconds)}
          testId="metric-uptime"
        />
      </div>

      {error && (
        <div
          className="metrics-dashboard__stale-warning"
          style={{
            marginTop: '1rem',
            padding: '0.75rem',
            backgroundColor: '#fef3c7',
            borderRadius: '4px',
            fontSize: '0.875rem',
            color: '#92400e',
          }}
        >
          Warning: Failed to refresh metrics. Showing stale data.
        </div>
      )}
    </div>
  );
}

export default MetricsDashboard;
