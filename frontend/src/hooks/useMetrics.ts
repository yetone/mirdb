/**
 * Custom hook for real-time metrics
 *
 * Owner: Scenario 2 - Real-time System Metrics Dashboard
 *
 * Features:
 * - Fetches metrics from /api/metrics
 * - Polls every 5 seconds (configurable)
 * - Returns loading, error, and data states
 */

import { useState, useEffect, useCallback } from 'react';
import type { MetricsResponse } from '../types/api';
import { fetchMetrics } from '../api/metrics';

interface UseMetricsOptions {
  /** Polling interval in milliseconds. Default: 5000 (5 seconds) */
  pollingInterval?: number;
  /** Whether to enable auto-polling. Default: true */
  autoRefresh?: boolean;
}

interface UseMetricsResult {
  /** The current metrics data, or null if not yet loaded */
  data: MetricsResponse | null;
  /** Whether the initial load is in progress */
  loading: boolean;
  /** Error message if the last fetch failed */
  error: string | null;
  /** Manually trigger a refresh */
  refresh: () => void;
  /** Timestamp of the last successful fetch */
  lastUpdated: Date | null;
}

/**
 * Hook to fetch and poll for real-time system metrics.
 *
 * @param options - Configuration options
 * @returns Metrics data, loading state, error state, and refresh function
 */
export function useMetrics(options: UseMetricsOptions = {}): UseMetricsResult {
  const { pollingInterval = 5000, autoRefresh = true } = options;

  const [data, setData] = useState<MetricsResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);

  const refresh = useCallback(async () => {
    try {
      const metrics = await fetchMetrics();
      setData(metrics);
      setError(null);
      setLastUpdated(new Date());
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to fetch metrics');
    } finally {
      setLoading(false);
    }
  }, []);

  // Initial fetch
  useEffect(() => {
    refresh();
  }, [refresh]);

  // Set up polling interval
  useEffect(() => {
    if (!autoRefresh || pollingInterval <= 0) {
      return;
    }

    const intervalId = setInterval(refresh, pollingInterval);

    return () => {
      clearInterval(intervalId);
    };
  }, [refresh, autoRefresh, pollingInterval]);

  return {
    data,
    loading,
    error,
    refresh,
    lastUpdated,
  };
}
