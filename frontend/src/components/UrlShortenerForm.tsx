/**
 * URL shortening input form component.
 * Owner: Scenario 2 - Anonymous URL Shortening
 *
 * Props:
 * - onShorten: (url: string) => void - Callback when form submitted
 * - isLoading: boolean - Loading state
 * - error: string | null - Error message to display
 *
 * Expected behavior:
 * - Text input for URL with placeholder
 * - Submit button labeled "Shorten"
 * - Validates URL format before submission
 * - Shows inline error message on validation failure
 * - Supports Enter key submission
 * - Accessible with proper labels and ARIA
 */
import React, { useState, FormEvent, KeyboardEvent } from 'react';

export interface UrlShortenerFormProps {
  onShorten: (url: string) => void;
  isLoading: boolean;
  error: string | null;
}

export function UrlShortenerForm({
  onShorten,
  isLoading,
  error,
}: UrlShortenerFormProps): React.ReactElement {
  const [url, setUrl] = useState('');
  const [validationError, setValidationError] = useState<string | null>(null);

  const validateUrl = (value: string): boolean => {
    if (!value.trim()) {
      setValidationError('Please enter a URL');
      return false;
    }

    try {
      const urlObj = new URL(value);
      if (!['http:', 'https:'].includes(urlObj.protocol)) {
        setValidationError('URL must start with http:// or https://');
        return false;
      }
    } catch {
      setValidationError('Please enter a valid URL');
      return false;
    }

    setValidationError(null);
    return true;
  };

  const handleSubmit = (e: FormEvent) => {
    e.preventDefault();
    if (isLoading) return;

    if (validateUrl(url)) {
      onShorten(url);
    }
  };

  const handleKeyDown = (e: KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      if (!isLoading && validateUrl(url)) {
        onShorten(url);
      }
    }
  };

  const displayError = validationError || error;

  return (
    <form
      onSubmit={handleSubmit}
      className="w-full max-w-2xl mx-auto"
      aria-label="URL shortening form"
    >
      <div className="flex flex-col sm:flex-row gap-3">
        <div className="flex-1">
          <label htmlFor="url-input" className="sr-only">
            Enter URL to shorten
          </label>
          <input
            id="url-input"
            type="url"
            value={url}
            onChange={(e) => {
              setUrl(e.target.value);
              if (validationError) {
                setValidationError(null);
              }
            }}
            onKeyDown={handleKeyDown}
            placeholder="Enter your long URL here..."
            className={`input input-bordered w-full ${displayError ? 'input-error' : ''}`}
            aria-invalid={!!displayError}
            aria-describedby={displayError ? 'url-error' : undefined}
            disabled={isLoading}
          />
        </div>
        <button
          type="submit"
          className="btn btn-primary"
          disabled={isLoading}
          aria-busy={isLoading}
        >
          {isLoading ? (
            <>
              <span className="loading loading-spinner loading-sm" aria-hidden="true"></span>
              Shortening...
            </>
          ) : (
            'Shorten'
          )}
        </button>
      </div>
      {displayError && (
        <p id="url-error" className="text-error text-sm mt-2" role="alert">
          {displayError}
        </p>
      )}
    </form>
  );
}

export default UrlShortenerForm;
