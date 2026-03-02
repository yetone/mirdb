/**
 * URL Shortening Form Component.
 * Owner: Scenario 2 - Guest URL Creation Flow
 *
 * Provides the live URL shortening demo form in the hero section:
 * - Large URL input field
 * - "Shorten" submit button
 * - Loading spinner during submission
 * - Display of shortened URL with copy button
 * - Success/error message handling
 *
 * Uses: useUrlShortener hook, CopyButton component
 * Requirements: REQ-4, REQ-5
 */
import { useState, FormEvent } from 'react';
import { Loader2 } from 'lucide-react';
import { useUrlShortener } from '../../hooks/useUrlShortener';
import { CopyButton } from '../ui/CopyButton';

export function UrlShortenForm() {
  const [inputUrl, setInputUrl] = useState('');
  const { shortenUrl, isLoading, error, result, reset } = useUrlShortener();

  const handleSubmit = async (e: FormEvent) => {
    e.preventDefault();
    await shortenUrl(inputUrl);
  };

  const handleInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    setInputUrl(e.target.value);
    // Clear previous result when user starts typing new URL
    if (result) {
      reset();
    }
  };

  const handleNewUrl = () => {
    setInputUrl('');
    reset();
  };

  return (
    <div className="url-shorten-form w-full" data-testid="url-shorten-form">
      <form onSubmit={handleSubmit} className="w-full">
        <div className="flex flex-col sm:flex-row gap-2">
          <label htmlFor="url-input" className="sr-only">
            Enter URL to shorten
          </label>
          <input
            id="url-input"
            type="text"
            value={inputUrl}
            onChange={handleInputChange}
            placeholder="Paste your long URL here..."
            className={`input input-bordered input-lg flex-1 w-full ${error ? 'input-error' : ''}`}
            aria-label="URL to shorten"
            aria-describedby={error ? 'url-error' : undefined}
            aria-invalid={error ? 'true' : 'false'}
            disabled={isLoading}
            data-testid="url-input"
          />
          <button
            type="submit"
            className="btn btn-primary btn-lg min-w-[120px]"
            disabled={isLoading}
            aria-label={isLoading ? 'Shortening URL...' : 'Shorten URL'}
            data-testid="shorten-button"
          >
            {isLoading ? (
              <>
                <Loader2 className="w-5 h-5 animate-spin" aria-hidden="true" />
                <span className="sr-only">Loading...</span>
              </>
            ) : (
              'Shorten'
            )}
          </button>
        </div>
      </form>

      {/* Error Message */}
      {error && (
        <div
          id="url-error"
          className="mt-3 text-error text-sm text-center"
          role="alert"
          data-testid="error-message"
        >
          {error}
        </div>
      )}

      {/* Success Result */}
      {result && (
        <div
          className="mt-4 p-4 bg-base-200 rounded-lg"
          role="status"
          aria-live="polite"
          data-testid="success-result"
        >
          <div className="flex flex-col sm:flex-row items-center justify-between gap-3">
            <div className="flex-1 min-w-0">
              <p className="text-sm text-base-content/80 mb-1">Your shortened URL:</p>
              <a
                href={result.shortUrl}
                target="_blank"
                rel="noopener noreferrer"
                className="text-primary font-medium break-all hover:underline"
                data-testid="short-url"
              >
                {result.shortUrl}
              </a>
            </div>
            <div className="flex gap-2">
              <CopyButton text={result.shortUrl} />
              <button
                type="button"
                onClick={handleNewUrl}
                className="btn btn-sm btn-ghost"
                aria-label="Shorten another URL"
                data-testid="new-url-button"
              >
                New URL
              </button>
            </div>
          </div>
          {/* Guest mode hint - using /80 opacity for WCAG AA contrast compliance */}
          {result.isGuest && (
            <p className="mt-2 text-xs text-base-content/80 text-center">
              Create an account to save your links permanently
            </p>
          )}
        </div>
      )}
    </div>
  );
}
