/**
 * Hook for anonymous URL shortening
 * Owner: Scenario 2 - Inline URL Shortening
 *
 * Provides state management for anonymous URL shortening:
 * - shortenUrl(url: string): Promise<ShortenedUrlResult>
 * - isLoading: boolean
 * - error: string | null
 * - result: ShortenedUrlResult | null
 *
 * Uses existing API endpoint or creates anonymous-friendly endpoint
 *
 * Requirements: REQ-2, REQ-7, NFR-5
 */

import { useState, useCallback } from 'react';
import type { ShortenedUrlResult, UseAnonymousShortenReturn } from '../types/homepage';

const API_BASE_URL = import.meta.env.VITE_API_URL || '/api';

export function useAnonymousShorten(): UseAnonymousShortenReturn {
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<ShortenedUrlResult | null>(null);

  const shortenUrl = useCallback(async (url: string): Promise<ShortenedUrlResult> => {
    setIsLoading(true);
    setError(null);

    try {
      const response = await fetch(`${API_BASE_URL}/shorten`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ url }),
      });

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({}));
        throw new Error(errorData.message || 'Failed to shorten URL');
      }

      const data: ShortenedUrlResult = await response.json();
      setResult(data);
      return data;
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : 'An unexpected error occurred';
      setError(errorMessage);
      throw err;
    } finally {
      setIsLoading(false);
    }
  }, []);

  const reset = useCallback(() => {
    setIsLoading(false);
    setError(null);
    setResult(null);
  }, []);

  return {
    shortenUrl,
    isLoading,
    error,
    result,
    reset,
  };
}
