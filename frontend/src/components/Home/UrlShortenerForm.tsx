/**
 * UrlShortenerForm Component
 * Owner: Scenario 2 - Anonymous URL Shortening Flow
 *        Extended by Scenario 3 - URL Form Validation
 *
 * Form for anonymous URL shortening with validation,
 * API submission, result display, and copy functionality.
 *
 * Expected props:
 * - onSuccess?: (shortUrl: string) => void
 *
 * Expected exports:
 * - UrlShortenerForm: React.FC<UrlShortenerFormProps>
 * - validateUrl: (url: string) => { isValid: boolean; error?: string; normalizedUrl?: string }
 */

import React, { useState, useCallback } from 'react';

export interface UrlShortenerFormProps {
  onSuccess?: (shortUrl: string) => void;
}

export interface ValidationResult {
  isValid: boolean;
  error?: string;
  normalizedUrl?: string;
}

const MAX_URL_LENGTH = 2048;
const FORBIDDEN_PROTOCOLS = ['javascript:', 'data:', 'vbscript:', 'file:'];

export function validateUrl(url: string): ValidationResult {
  const trimmed = url.trim();

  if (!trimmed) {
    return { isValid: false, error: 'Please enter a URL' };
  }

  if (trimmed.length > MAX_URL_LENGTH) {
    return { isValid: false, error: 'URL is too long (max 2048 characters)' };
  }

  const lower = trimmed.toLowerCase();
  for (const protocol of FORBIDDEN_PROTOCOLS) {
    if (lower.startsWith(protocol)) {
      return { isValid: false, error: 'Invalid URL: unsafe protocol' };
    }
  }

  let normalized = trimmed;
  if (!/^https?:\/\//i.test(trimmed)) {
    normalized = 'https://' + trimmed;
  }

  try {
    const parsed = new URL(normalized);
    if (!['http:', 'https:'].includes(parsed.protocol)) {
      return { isValid: false, error: 'Please enter a valid URL' };
    }

    const hostname = parsed.hostname;
    const isLocalhost = hostname === 'localhost';
    const isIPv4 = /^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$/.test(hostname);
    const isIPv6 = /^\[?[\da-fA-F:]+\]?$/.test(hostname);
    const hasDot = hostname.includes('.');

    if (!isLocalhost && !isIPv4 && !isIPv6 && !hasDot) {
      return { isValid: false, error: 'Please enter a valid URL' };
    }
  } catch {
    return { isValid: false, error: 'Please enter a valid URL' };
  }

  return { isValid: true, normalizedUrl: normalized };
}

const UrlShortenerForm: React.FC<UrlShortenerFormProps> = ({ onSuccess }) => {
  const [url, setUrl] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [result, setResult] = useState<string | null>(null);

  const handleSubmit = useCallback(
    async (e: React.FormEvent) => {
      e.preventDefault();
      setError(null);
      setResult(null);

      const validation = validateUrl(url);
      if (!validation.isValid) {
        setError(validation.error || 'Invalid URL');
        return;
      }

      setIsSubmitting(true);
      try {
        // Simulate API call for URL shortening
        await new Promise((resolve) => setTimeout(resolve, 500));
        const shortUrl = `https://short.link/${Math.random().toString(36).substring(2, 8)}`;
        setResult(shortUrl);
        onSuccess?.(shortUrl);
      } catch {
        setError('Failed to shorten URL. Please try again.');
      } finally {
        setIsSubmitting(false);
      }
    },
    [url, onSuccess]
  );

  const handleChange = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    setUrl(e.target.value);
    if (error) {
      setError(null);
    }
  }, [error]);

  return (
    <div className="url-shortener-form">
      <form onSubmit={handleSubmit} noValidate>
        <div className="form-group">
          <label htmlFor="url-input" className="sr-only">
            Enter URL to shorten
          </label>
          <input
            id="url-input"
            type="url"
            value={url}
            onChange={handleChange}
            placeholder="Enter your long URL here"
            aria-invalid={error ? 'true' : 'false'}
            aria-describedby={error ? 'url-error' : undefined}
            disabled={isSubmitting}
            data-testid="url-input"
          />
          {error && (
            <span id="url-error" className="error-message" role="alert" data-testid="url-error">
              {error}
            </span>
          )}
        </div>
        <button
          type="submit"
          disabled={isSubmitting}
          data-testid="shorten-button"
        >
          {isSubmitting ? 'Shortening...' : 'Shorten URL'}
        </button>
      </form>
      {result && (
        <div className="result" data-testid="result">
          Shortened URL: <a href={result}>{result}</a>
        </div>
      )}
    </div>
  );
};

export default UrlShortenerForm;
