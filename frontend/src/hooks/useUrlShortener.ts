/**
 * URL Shortener Hook.
 * Owner: Scenario 2 - Guest URL Creation Flow
 *
 * Handles URL shortening logic:
 * - Form state management
 * - URL validation
 * - API call to create shortened URL
 * - Loading/error states
 * - Guest mode support (no authentication required)
 */
import { useState, useCallback } from 'react';
import type { ShortenedUrlResult } from '../types/home';

interface UseUrlShortenerReturn {
  shortenUrl: (url: string) => Promise<ShortenedUrlResult | null>;
  isLoading: boolean;
  error: string | null;
  result: ShortenedUrlResult | null;
  reset: () => void;
}

const URL_REGEX = /^(https?:\/\/)?([a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?\.)+[a-zA-Z]{2,}(\/[^\s]*)?$/;

function isValidUrl(url: string): boolean {
  if (!url || url.trim() === '') {
    return false;
  }
  return URL_REGEX.test(url.trim());
}

function normalizeUrl(url: string): string {
  const trimmed = url.trim();
  if (!trimmed.startsWith('http://') && !trimmed.startsWith('https://')) {
    return `https://${trimmed}`;
  }
  return trimmed;
}

export function useUrlShortener(): UseUrlShortenerReturn {
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<ShortenedUrlResult | null>(null);

  const reset = useCallback(() => {
    setError(null);
    setResult(null);
    setIsLoading(false);
  }, []);

  const shortenUrl = useCallback(async (url: string): Promise<ShortenedUrlResult | null> => {
    // Reset previous state
    setError(null);
    setResult(null);

    // Validation: empty URL
    if (!url || url.trim() === '') {
      setError('Please enter a URL');
      return null;
    }

    // Validation: invalid URL format
    if (!isValidUrl(url)) {
      setError('Please enter a valid URL');
      return null;
    }

    const normalizedUrl = normalizeUrl(url);

    setIsLoading(true);

    try {
      const response = await fetch('/api/urls/', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          original_url: normalizedUrl,
          guest: true,
        }),
      });

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({}));
        throw new Error(errorData.detail || `HTTP error ${response.status}`);
      }

      const data = await response.json();

      const shortenedResult: ShortenedUrlResult = {
        shortCode: data.short_code,
        shortUrl: `${window.location.origin}/r/${data.short_code}`,
        originalUrl: data.original_url,
        isGuest: true,
      };

      setResult(shortenedResult);
      return shortenedResult;
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : 'Failed to shorten URL';
      setError(errorMessage);
      return null;
    } finally {
      setIsLoading(false);
    }
  }, []);

  return { shortenUrl, isLoading, error, result, reset };
}
