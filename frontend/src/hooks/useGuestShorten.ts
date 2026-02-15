/**
 * Guest Shorten Hook
 * Owner: Scenario 2 - Guest URL Shortening
 *
 * Custom React hook for guest URL shortening functionality.
 * Handles API calls to create short URLs without authentication.
 */

import { useState, useCallback } from 'react';
import { urlsApi } from '../api';
import type { ShortenResult, UseGuestShortenReturn } from '../types/home';

/**
 * Hook for guest URL shortening
 * Provides state management for the URL shortening process
 */
export function useGuestShorten(): UseGuestShortenReturn {
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<ShortenResult | null>(null);

  const shortenUrl = useCallback(async (url: string): Promise<ShortenResult> => {
    setIsLoading(true);
    setError(null);

    try {
      const response = await urlsApi.shorten({ original_url: url });

      const shortenResult: ShortenResult = {
        originalUrl: response.original_url,
        shortUrl: `${window.location.origin}/r/${response.short_code}`,
        shortCode: response.short_code,
      };

      setResult(shortenResult);
      return shortenResult;
    } catch (err) {
      const errorMessage =
        err instanceof Error
          ? err.message
          : 'Failed to shorten URL. Please try again.';
      setError(errorMessage);
      throw new Error(errorMessage);
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

export default useGuestShorten;
