/**
 * URL Shortener Hook
 * Owner: Scenario 1 - Hero Section URL Shortening
 *
 * Custom hook for managing URL shortening API calls.
 */

import { useState, useCallback } from 'react';
import { shortenUrlAnonymous, parseApiError } from '../api';
import { ShortenUrlResponse } from '../types/landing';

export interface UseUrlShortenerReturn {
  shortenUrl: (url: string) => Promise<ShortenUrlResponse | null>;
  isLoading: boolean;
  error: string | null;
  result: ShortenUrlResponse | null;
  reset: () => void;
}

/**
 * Custom hook for URL shortening operations
 *
 * Handles API calls, loading states, errors, and results
 */
export function useUrlShortener(): UseUrlShortenerReturn {
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<ShortenUrlResponse | null>(null);

  const reset = useCallback(() => {
    setIsLoading(false);
    setError(null);
    setResult(null);
  }, []);

  const shortenUrl = useCallback(async (url: string): Promise<ShortenUrlResponse | null> => {
    setIsLoading(true);
    setError(null);
    setResult(null);

    try {
      const response = await shortenUrlAnonymous(url);
      setResult(response);
      setIsLoading(false);
      return response;
    } catch (err) {
      const apiError = parseApiError(err);
      setError(apiError.message);
      setIsLoading(false);
      return null;
    }
  }, []);

  return {
    shortenUrl,
    isLoading,
    error,
    result,
    reset,
  };
}
