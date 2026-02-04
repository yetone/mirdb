/**
 * Custom hook for URL shortening functionality.
 * Owner: Scenario 2 - Anonymous URL Shortening
 *
 * Expected exports:
 * - useUrlShortener(): {
 *     shortenUrl: (url: string) => Promise<void>,
 *     shortUrl: string | null,
 *     isLoading: boolean,
 *     error: string | null,
 *     reset: () => void
 *   }
 *
 * Implementation notes:
 * - Uses existing API client pattern
 * - Calls POST /api/urls/shorten endpoint
 * - Handles loading and error states
 * - Returns full shortened URL (base URL + short code)
 */
import { useState, useCallback } from 'react';
import { shortenUrlAnonymous, getFullShortUrl } from '../api';

export interface UseUrlShortenerResult {
  shortenUrl: (url: string) => Promise<void>;
  shortUrl: string | null;
  isLoading: boolean;
  error: string | null;
  reset: () => void;
}

export function useUrlShortener(): UseUrlShortenerResult {
  const [shortUrl, setShortUrl] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const shortenUrl = useCallback(async (url: string) => {
    setIsLoading(true);
    setError(null);

    try {
      const response = await shortenUrlAnonymous(url);
      const fullShortUrl = getFullShortUrl(response.short_code);
      setShortUrl(fullShortUrl);
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : 'Failed to shorten URL';
      setError(errorMessage);
    } finally {
      setIsLoading(false);
    }
  }, []);

  const reset = useCallback(() => {
    setShortUrl(null);
    setError(null);
    setIsLoading(false);
  }, []);

  return {
    shortenUrl,
    shortUrl,
    isLoading,
    error,
    reset,
  };
}
