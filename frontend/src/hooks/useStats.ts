/**
 * useStats Hook
 * Owner: Scenario 11 - Social Proof Elements
 *
 * Custom hook for fetching platform statistics.
 * Provides loading and error states with graceful fallback.
 */

import { useState, useEffect } from 'react';
import { PlatformStats } from '../types/homepage';

export interface StatsResult {
  stats: PlatformStats | null;
  isLoading: boolean;
  error: Error | null;
}

const API_BASE_URL = import.meta.env.VITE_API_URL || '/api';

export function useStats(): StatsResult {
  const [stats, setStats] = useState<PlatformStats | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  useEffect(() => {
    let isMounted = true;

    async function fetchStats() {
      try {
        setIsLoading(true);
        setError(null);

        const response = await fetch(`${API_BASE_URL}/stats`);

        if (!response.ok) {
          throw new Error(`Failed to fetch stats: ${response.status}`);
        }

        const data = await response.json();

        if (isMounted) {
          setStats({
            userCount: data.userCount ?? data.users ?? 0,
            linksCreated: data.linksCreated ?? data.links ?? 0,
          });
        }
      } catch (err) {
        if (isMounted) {
          setError(err instanceof Error ? err : new Error('Failed to fetch stats'));
        }
      } finally {
        if (isMounted) {
          setIsLoading(false);
        }
      }
    }

    fetchStats();

    return () => {
      isMounted = false;
    };
  }, []);

  return { stats, isLoading, error };
}
