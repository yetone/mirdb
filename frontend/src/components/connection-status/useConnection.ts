/**
 * Connection status polling hook.
 * Owner: Scenario 5 - Connection Status Indicator
 *
 * Polls /api/health at regular intervals and manages connection state.
 * Covers REQ-13.
 */

import { useState, useEffect, useRef, useCallback } from 'react';
import type { ConnectionState, HealthStatus } from '../../types';
import { getHealth } from '../../api/client';
import { HEALTH_POLL_INTERVAL_MS } from '../../utils/constants';

export interface UseConnectionResult {
  state: ConnectionState;
  isLoading: boolean;
  error: Error | null;
}

export function useConnection(
  pollInterval: number = HEALTH_POLL_INTERVAL_MS
): UseConnectionResult {
  const [state, setState] = useState<ConnectionState>('connecting');
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);
  const timeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const isMountedRef = useRef(true);

  const pollHealth = useCallback(async () => {
    if (timeoutRef.current) {
      clearTimeout(timeoutRef.current);
      timeoutRef.current = null;
    }

    if (!isMountedRef.current) {
      return;
    }

    setIsLoading(true);

    try {
      const health: HealthStatus = await getHealth();

      if (!isMountedRef.current) {
        return;
      }

      if (health.status === 'healthy') {
        setState('connected');
        setError(null);
      } else {
        setState('disconnected');
        setError(new Error('Server reported unhealthy status'));
      }
    } catch (err) {
      if (!isMountedRef.current) {
        return;
      }

      setState('disconnected');
      setError(err instanceof Error ? err : new Error('Health check failed'));
    } finally {
      if (isMountedRef.current) {
        setIsLoading(false);
        timeoutRef.current = setTimeout(pollHealth, pollInterval);
      }
    }
  }, [pollInterval]);

  useEffect(() => {
    isMountedRef.current = true;
    setState('connecting');
    setIsLoading(true);
    setError(null);

    pollHealth();

    return () => {
      isMountedRef.current = false;
      if (timeoutRef.current) {
        clearTimeout(timeoutRef.current);
        timeoutRef.current = null;
      }
    };
  }, [pollHealth]);

  return { state, isLoading, error };
}
