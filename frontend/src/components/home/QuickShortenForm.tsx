/**
 * Quick Shorten Form Component
 * Owner: Scenario 2 - Guest URL Shortening
 *
 * URL shortening form for guest users with:
 * - URL input field
 * - Shorten button
 * - Result display (original URL, short URL, copy button)
 * - Registration prompt
 */

import React, { useState, FormEvent } from 'react';
import { Link2, Copy, Check, Loader2, AlertCircle } from 'lucide-react';
import { useGuestShorten } from '../../hooks/useGuestShorten';
import { copyToClipboard } from '../../utils/clipboard';
import type { QuickShortenFormProps } from '../../types/home';

/**
 * Truncates a URL for display purposes
 */
function truncateUrl(url: string, maxLength: number = 50): string {
  if (url.length <= maxLength) return url;
  return `${url.substring(0, maxLength - 3)}...`;
}

/**
 * QuickShortenForm component
 * Allows guest users to shorten URLs without authentication
 */
export function QuickShortenForm({
  onSuccess,
  onError,
}: QuickShortenFormProps): React.ReactElement {
  const [url, setUrl] = useState('');
  const [copied, setCopied] = useState(false);
  const { shortenUrl, isLoading, error, result, reset } = useGuestShorten();

  const handleSubmit = async (e: FormEvent) => {
    e.preventDefault();

    if (!url.trim()) return;

    try {
      const shortenResult = await shortenUrl(url.trim());
      onSuccess?.(shortenResult.shortUrl);
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : 'An error occurred';
      onError?.(errorMessage);
    }
  };

  const handleCopy = async () => {
    if (!result) return;

    const success = await copyToClipboard(result.shortUrl);
    if (success) {
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    }
  };

  const handleReset = () => {
    setUrl('');
    setCopied(false);
    reset();
  };

  return (
    <div className="w-full max-w-2xl mx-auto">
      <form onSubmit={handleSubmit} className="space-y-4">
        <div className="flex flex-col sm:flex-row gap-3">
          <div className="flex-1 relative">
            <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
              <Link2 className="h-5 w-5 text-base-content/50" aria-hidden="true" />
            </div>
            <input
              type="url"
              value={url}
              onChange={(e) => setUrl(e.target.value)}
              placeholder="Paste your long URL here..."
              className="input input-bordered w-full pl-10 focus:input-primary"
              disabled={isLoading}
              aria-label="URL to shorten"
              required
            />
          </div>
          <button
            type="submit"
            disabled={isLoading || !url.trim()}
            className="btn btn-primary min-w-[120px]"
            aria-label="Shorten URL"
          >
            {isLoading ? (
              <>
                <Loader2 className="h-4 w-4 animate-spin mr-2" aria-hidden="true" />
                Shortening...
              </>
            ) : (
              'Shorten'
            )}
          </button>
        </div>

        {/* Error display */}
        {error && (
          <div
            className="alert alert-error"
            role="alert"
            aria-live="polite"
          >
            <AlertCircle className="h-5 w-5" aria-hidden="true" />
            <span>{error}</span>
          </div>
        )}
      </form>

      {/* Result display */}
      {result && (
        <div className="mt-6 p-4 bg-base-200 rounded-lg space-y-4" data-testid="shorten-result">
          {/* Original URL */}
          <div className="space-y-1">
            <p className="text-sm text-base-content/60">Original URL:</p>
            <p className="text-sm break-all" title={result.originalUrl}>
              {truncateUrl(result.originalUrl, 60)}
            </p>
          </div>

          {/* Shortened URL with copy button */}
          <div className="space-y-1">
            <p className="text-sm text-base-content/60">Short URL:</p>
            <div className="flex items-center gap-2">
              <code className="flex-1 p-2 bg-base-300 rounded text-primary font-mono text-sm break-all">
                {result.shortUrl}
              </code>
              <button
                type="button"
                onClick={handleCopy}
                className="btn btn-square btn-ghost"
                aria-label={copied ? 'Copied to clipboard' : 'Copy short URL to clipboard'}
                title={copied ? 'Copied!' : 'Copy to clipboard'}
              >
                {copied ? (
                  <Check className="h-5 w-5 text-success" aria-hidden="true" />
                ) : (
                  <Copy className="h-5 w-5" aria-hidden="true" />
                )}
              </button>
            </div>
            {copied && (
              <p className="text-sm text-success" role="status" aria-live="polite">
                Copied to clipboard!
              </p>
            )}
          </div>

          {/* Registration prompt */}
          <div
            className="mt-4 p-3 bg-primary/10 rounded-lg border border-primary/20"
            data-testid="registration-prompt"
          >
            <p className="text-sm">
              <strong>Want to track clicks and view detailed analytics?</strong>
            </p>
            <p className="text-sm text-base-content/70 mt-1">
              Create a free account to access click tracking, geo-location data,
              referrer analysis, and more.
            </p>
          </div>

          {/* Shorten another button */}
          <button
            type="button"
            onClick={handleReset}
            className="btn btn-outline btn-sm"
            aria-label="Shorten another URL"
          >
            Shorten Another URL
          </button>
        </div>
      )}
    </div>
  );
}

export default QuickShortenForm;
