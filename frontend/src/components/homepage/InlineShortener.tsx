/**
 * Inline URL Shortener Component
 * Owner: Scenario 2 - Inline URL Shortening
 *
 * Provides anonymous URL shortening functionality:
 * - URL input field with placeholder
 * - Shorten button
 * - Result display with copy button
 * - Registration prompt after success
 * - Loading states and error handling
 *
 * Requirements: REQ-2, REQ-7
 */

import { useState, useCallback, FormEvent } from 'react';
import { useAnonymousShorten } from '../../hooks/useAnonymousShorten';
import type { InlineShortenerProps, UrlValidationResult } from '../../types/homepage';

/**
 * Validates a URL string for proper format
 * @param url - The URL string to validate
 * @returns UrlValidationResult with isValid and optional error message
 */
export function validateUrl(url: string): UrlValidationResult {
  // Check for empty input
  if (!url || url.trim() === '') {
    return {
      isValid: false,
      error: 'URL is required',
    };
  }

  const trimmedUrl = url.trim();

  // Check for incomplete URLs (just protocol)
  if (/^https?:\/\/?$/i.test(trimmedUrl)) {
    return {
      isValid: false,
      error: 'Please enter a valid URL',
    };
  }

  // Try to parse the URL
  try {
    const parsedUrl = new URL(trimmedUrl);

    // Ensure the URL has a valid protocol
    if (!['http:', 'https:'].includes(parsedUrl.protocol)) {
      return {
        isValid: false,
        error: 'Please enter a valid URL',
      };
    }

    // Ensure the URL has a hostname
    if (!parsedUrl.hostname || parsedUrl.hostname.length === 0) {
      return {
        isValid: false,
        error: 'Please enter a valid URL',
      };
    }

    return { isValid: true };
  } catch {
    // URL constructor threw an error - invalid URL
    return {
      isValid: false,
      error: 'Please enter a valid URL',
    };
  }
}

/**
 * Encodes a URL ensuring special characters are properly handled
 * @param url - The URL string to encode
 * @returns The properly encoded URL
 */
export function encodeUrlForApi(url: string): string {
  try {
    // Parse and reconstruct to ensure proper encoding
    const parsed = new URL(url.trim());
    return parsed.href;
  } catch {
    // If parsing fails, return the original (validation should catch this)
    return url.trim();
  }
}

export function InlineShortener({ onSuccess, onError }: InlineShortenerProps) {
  const [inputUrl, setInputUrl] = useState('');
  const [validationError, setValidationError] = useState<string | null>(null);
  const [copied, setCopied] = useState(false);

  const { shortenUrl, isLoading, error: apiError, result, reset } = useAnonymousShorten();

  // Combined error from validation or API
  const displayError = validationError || apiError;

  const handleSubmit = useCallback(async (e: FormEvent) => {
    e.preventDefault();

    // Clear previous errors
    setValidationError(null);
    setCopied(false);
    reset();

    // Validate URL
    const validation = validateUrl(inputUrl);
    if (!validation.isValid) {
      setValidationError(validation.error || 'Please enter a valid URL');
      onError?.(validation.error || 'Please enter a valid URL');
      return;
    }

    // Encode URL for API
    const encodedUrl = encodeUrlForApi(inputUrl);

    try {
      const result = await shortenUrl(encodedUrl);
      onSuccess?.(result);
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : 'Failed to shorten URL. Please try again.';
      onError?.(errorMessage);
    }
  }, [inputUrl, shortenUrl, onSuccess, onError, reset]);

  const handleCopy = useCallback(async () => {
    if (result?.shortUrl) {
      try {
        await navigator.clipboard.writeText(result.shortUrl);
        setCopied(true);
        setTimeout(() => setCopied(false), 2000);
      } catch {
        // Fallback for older browsers
        const textArea = document.createElement('textarea');
        textArea.value = result.shortUrl;
        document.body.appendChild(textArea);
        textArea.select();
        document.execCommand('copy');
        document.body.removeChild(textArea);
        setCopied(true);
        setTimeout(() => setCopied(false), 2000);
      }
    }
  }, [result]);

  const handleRetry = useCallback(() => {
    setValidationError(null);
    reset();
  }, [reset]);

  return (
    <div className="w-full max-w-2xl mx-auto">
      <form onSubmit={handleSubmit} className="flex flex-col sm:flex-row gap-3">
        <div className="flex-1">
          <label htmlFor="url-input" className="sr-only">
            Enter URL to shorten
          </label>
          <input
            id="url-input"
            type="text"
            value={inputUrl}
            onChange={(e) => {
              setInputUrl(e.target.value);
              if (validationError) setValidationError(null);
            }}
            placeholder="Paste your long URL here..."
            className={`input input-bordered w-full ${displayError ? 'input-error' : ''}`}
            aria-describedby={displayError ? 'url-error' : undefined}
            aria-invalid={!!displayError}
            disabled={isLoading}
          />
          {displayError && (
            <p
              id="url-error"
              className="text-error text-sm mt-1"
              role="alert"
              data-testid="error-message"
            >
              {displayError}
            </p>
          )}
        </div>
        <button
          type="submit"
          className="btn btn-primary"
          disabled={isLoading}
          data-testid="shorten-button"
        >
          {isLoading ? (
            <>
              <span className="loading loading-spinner loading-sm"></span>
              Shortening...
            </>
          ) : (
            'Shorten'
          )}
        </button>
      </form>

      {result && (
        <div className="mt-4 p-4 bg-base-200 rounded-lg" data-testid="result-section">
          <div className="flex items-center gap-3">
            <a
              href={result.shortUrl}
              target="_blank"
              rel="noopener noreferrer"
              className="link link-primary flex-1 truncate"
              data-testid="short-url"
            >
              {result.shortUrl}
            </a>
            <button
              type="button"
              onClick={handleCopy}
              className="btn btn-sm btn-outline"
              data-testid="copy-button"
            >
              {copied ? 'Copied!' : 'Copy'}
            </button>
          </div>
          <p className="text-sm text-base-content/70 mt-2">
            <a href="/register" className="link link-secondary">
              Create an account
            </a>
            {' '}to track analytics for your links.
          </p>
        </div>
      )}

      {apiError && !validationError && (
        <div className="mt-4">
          <button
            type="button"
            onClick={handleRetry}
            className="btn btn-sm btn-ghost"
            data-testid="retry-button"
          >
            Try again
          </button>
        </div>
      )}
    </div>
  );
}

export default InlineShortener;
