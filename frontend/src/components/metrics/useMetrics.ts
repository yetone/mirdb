/**
 * Custom hook for polling metrics from /api/metrics.
 *
 * Responsibilities:
 * - Poll /api/metrics at METRICS_POLL_INTERVAL_MS (2000ms)
 * - Track loading state (only on initial load)
 * - Track error state
 * - Provide manual refresh capability
 * - Clean up intervals on unmount
 */

import { useState, useEffect, useRef, useCallback } from 'react';
import type { Metrics } from '../../types';
import { getMetrics } from '../../api/client';
import { METRICS_POLL_INTERVAL_MS } from '../../utils/constants';

export interface UseMetricsResult {
  metrics: Metrics | null;
  loading: boolean;
  error: string | null;
  isRefreshing: boolean;
  refresh: () => void;
}

export function useMetrics(): UseMetricsResult {
  const [metrics, setMetrics] = useState<Metrics | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const isInitialLoadRef = useRef(true);

  const fetchMetrics = useCallback(async (isManualRefresh = false) => {
    if (isManualRefresh) {
      setIsRefreshing(true);
    }

    try {
      const data = await getMetrics();
      setMetrics(data);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to fetch metrics');
    } finally {
      if (isInitialLoadRef.current) {
        setLoading(false);
        isInitialLoadRef.current = false;
      }
      if (isManualRefresh) {
        setIsRefreshing(false);
      }
    }
  }, []);

  const refresh = useCallback(() => {
    fetchMetrics(true);
  }, [fetchMetrics]);

  useEffect(() => {
    fetchMetrics();

    intervalRef.current = setInterval(() => {
      fetchMetrics(false);
    }, METRICS_POLL_INTERVAL_MS);

    return () => {
      if (intervalRef.current) {
        clearInterval(intervalRef.current);
        intervalRef.current = null;
      }
    };
  }, [fetchMetrics]);

  return { metrics, loading, error, isRefreshing, refresh };
}
