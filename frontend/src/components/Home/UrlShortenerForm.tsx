/**
 * UrlShortenerForm Component
 * Owner: Scenario 2 - Anonymous URL Shortening Flow
 *        Extended by Scenario 3 - URL Form Validation
 *        Extended by Scenario 9 - Error Handling and API Failure Resilience
 *
 * Form for anonymous URL shortening with validation,
 * API submission, result display, and copy functionality.
 * Handles API errors gracefully with user-friendly messages.
 *
 * Expected props:
 * - onSuccess?: (shortUrl: string) => void
 *
 * Expected exports:
 * - UrlShortenerForm: React.FC<UrlShortenerFormProps>
 * - validateUrl: (url: string) => { isValid: boolean; error?: string; normalizedUrl?: string }
 */

import React, { useState, useCallback, useRef } from 'react';
import { shortenUrl, isApiError } from '../../api/index';
import { useTheme } from '../../contexts/ThemeContext';

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
  const [copied, setCopied] = useState(false);
  const [copyFallback, setCopyFallback] = useState(false);
  const resultUrlRef = useRef<HTMLAnchorElement>(null);
  const { theme } = useTheme();

  const handleSubmit = useCallback(
    async (e: React.FormEvent) => {
      e.preventDefault();
      setError(null);
      setResult(null);
      setCopied(false);
      setCopyFallback(false);

      const validation = validateUrl(url);
      if (!validation.isValid) {
        setError(validation.error || 'Invalid URL');
        return;
      }

      setIsSubmitting(true);
      try {
        const normalizedUrl = validation.normalizedUrl || url;
        const { shortUrl } = await shortenUrl(normalizedUrl);
        setResult(shortUrl);
        onSuccess?.(shortUrl);
      } catch (err) {
        if (isApiError(err)) {
          setError(err.message);
        } else {
          setError('Something went wrong. Please try again.');
        }
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

  const handleCopy = useCallback(async () => {
    if (!result) return;

    setCopied(false);
    setCopyFallback(false);

    if (navigator.clipboard && window.isSecureContext) {
      try {
        await navigator.clipboard.writeText(result);
        setCopied(true);
        setTimeout(() => setCopied(false), 2000);
      } catch {
        fallbackCopy();
      }
    } else {
      fallbackCopy();
    }
  }, [result]);

  const fallbackCopy = useCallback(() => {
    if (resultUrlRef.current) {
      const range = document.createRange();
      range.selectNode(resultUrlRef.current);
      const selection = window.getSelection();
      if (selection) {
        selection.removeAllRanges();
        selection.addRange(range);
        setCopyFallback(true);
        setTimeout(() => setCopyFallback(false), 3000);
      }
    }
  }, []);

  const getResultContainerClasses = () => {
    const base = 'result mt-4 p-4 rounded-lg border transition-colors';
    switch (theme) {
      case 'cyberpunk':
        return `${base} bg-yellow-900/20 border-yellow-400/50`;
      case 'synthwave':
        return `${base} bg-fuchsia-900/20 border-fuchsia-400/50`;
      case 'dark':
        return `${base} bg-green-900/20 border-green-700`;
      default:
        return `${base} bg-green-50 border-green-200`;
    }
  };

  const getResultLabelClasses = () => {
    switch (theme) {
      case 'cyberpunk':
        return 'text-yellow-400';
      case 'synthwave':
        return 'text-fuchsia-300';
      case 'dark':
        return 'text-green-300';
      default:
        return 'text-green-800';
    }
  };

  const getResultUrlClasses = () => {
    switch (theme) {
      case 'cyberpunk':
        return 'text-cyan-400 hover:text-cyan-300';
      case 'synthwave':
        return 'text-pink-400 hover:text-pink-300';
      case 'dark':
        return 'text-blue-400 hover:text-blue-300';
      default:
        return 'text-blue-700 hover:text-blue-800';
    }
  };

  const getCopyButtonClasses = () => {
    const base = 'copy-button ml-2 px-3 py-1.5 rounded-md text-sm font-medium transition-all duration-200 flex items-center gap-1.5 focus:outline-none focus:ring-2 focus:ring-offset-1';
    if (copied) {
      return `${base} bg-green-600 text-white focus:ring-green-500`;
    }
    switch (theme) {
      case 'cyberpunk':
        return `${base} bg-yellow-500/20 text-yellow-300 hover:bg-yellow-500/30 border border-yellow-400/50 focus:ring-yellow-400`;
      case 'synthwave':
        return `${base} bg-fuchsia-500/20 text-fuchsia-300 hover:bg-fuchsia-500/30 border border-fuchsia-400/50 focus:ring-fuchsia-400`;
      case 'dark':
        return `${base} bg-gray-700 text-gray-200 hover:bg-gray-600 border border-gray-600 focus:ring-gray-500`;
      default:
        return `${base} bg-blue-50 text-blue-700 hover:bg-blue-100 border border-blue-200 focus:ring-blue-500`;
    }
  };

  return (
    <div className="url-shortener-form bg-white/10 backdrop-blur-md rounded-2xl border border-white/20 shadow-lg p-4 sm:p-6 md:p-8">
      <form onSubmit={handleSubmit} noValidate className="flex flex-col sm:flex-row gap-3">
        <div className="form-group flex-1">
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
            className="w-full px-4 py-3 min-h-[44px] rounded-lg border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-base focus:outline-none focus:ring-2 focus:ring-blue-500"
          />
          {error && (
            <span id="url-error" className="error-message block mt-2 text-sm text-red-600" role="alert" data-testid="url-error">
              {error}
            </span>
          )}
        </div>
        <button
          type="submit"
          disabled={isSubmitting}
          data-testid="shorten-button"
          className="futuristic-button futuristic-btn-primary px-6 py-3 min-h-[44px] min-w-[44px] rounded-lg bg-blue-600 text-white font-medium hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors whitespace-nowrap focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2"
        >
          {isSubmitting ? 'Shortening...' : 'Shorten URL'}
        </button>
      </form>
      {result && (
        <div className={getResultContainerClasses()} data-testid="result" data-theme={theme}>
          <p className={`text-sm font-medium mb-2 ${getResultLabelClasses()}`} data-testid="result-label">
            Your shortened URL
          </p>
          <div className="flex items-center gap-2 flex-wrap">
            <a
              ref={resultUrlRef}
              href={result}
              className={`text-base font-semibold break-all underline-offset-2 hover:underline ${getResultUrlClasses()}`}
              data-testid="result-url"
            >
              {result}
            </a>
            <button
              type="button"
              onClick={handleCopy}
              className={getCopyButtonClasses()}
              data-testid="copy-button"
              aria-label={copied ? 'Copied to clipboard' : 'Copy shortened URL to clipboard'}
              aria-live="polite"
            >
              {copied ? (
                <>
                  <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" data-testid="copy-check-icon">
                    <polyline points="20 6 9 17 4 12" />
                  </svg>
                  <span>Copied!</span>
                </>
              ) : (
                <>
                  <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" data-testid="copy-icon">
                    <rect x="9" y="9" width="13" height="13" rx="2" ry="2" />
                    <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1" />
                  </svg>
                  <span>Copy</span>
                </>
              )}
            </button>
          </div>
          {copyFallback && (
            <p className="mt-2 text-sm text-amber-600 dark:text-amber-400" data-testid="copy-fallback-message" role="status">
              URL selected for manual copying. Press Ctrl+C to copy.
            </p>
          )}
        </div>
      )}
    </div>
  );
};

export default UrlShortenerForm;
