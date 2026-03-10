/**
 * URL Shortener Form Component
 * Owner: Scenario 1 - Hero Section URL Shortening
 *
 * The form component for submitting URLs to shorten.
 * Handles input validation, loading states, and error display.
 */

import { useState } from 'react';
import type { FormEvent, KeyboardEvent } from 'react';
import { HERO_CONTENT } from '../../constants/landingContent';
import type { ShortenUrlResponse } from '../../types/landing';

export interface UrlShortenerFormProps {
  onSubmit: (url: string) => Promise<ShortenUrlResponse | null>;
  isLoading: boolean;
  error: string | null;
}

/**
 * Form component for URL shortening input and submission
 */
export function UrlShortenerForm({ onSubmit, isLoading, error }: UrlShortenerFormProps) {
  const [url, setUrl] = useState('');
  const [localError, setLocalError] = useState<string | null>(null);

  const displayError = error || localError;

  const validateUrl = (value: string): boolean => {
    if (!value.trim()) {
      setLocalError('Please enter a URL');
      return false;
    }
    setLocalError(null);
    return true;
  };

  const handleSubmit = async (e: FormEvent) => {
    e.preventDefault();

    if (!validateUrl(url)) {
      return;
    }

    await onSubmit(url);
  };

  const handleKeyDown = (e: KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      handleSubmit(e as unknown as FormEvent);
    }
  };

  return (
    <form onSubmit={handleSubmit} className="w-full max-w-2xl mx-auto" role="form">
      <div className="flex flex-col gap-4">
        <div className="form-control w-full">
          <label htmlFor="url-input" className="label">
            <span className="label-text sr-only">URL to shorten</span>
          </label>
          <div className="flex flex-col sm:flex-row gap-2">
            <input
              id="url-input"
              type="text"
              value={url}
              onChange={(e) => {
                setUrl(e.target.value);
                if (localError) setLocalError(null);
              }}
              onKeyDown={handleKeyDown}
              placeholder={HERO_CONTENT.inputPlaceholder}
              className={`input input-bordered flex-1 text-lg ${
                displayError ? 'input-error' : ''
              }`}
              aria-label="URL to shorten"
              aria-describedby={displayError ? 'url-error' : undefined}
              aria-invalid={!!displayError}
              disabled={isLoading}
              data-testid="url-input"
            />
            <button
              type="submit"
              className="btn btn-primary min-w-[140px]"
              disabled={isLoading}
              aria-label={isLoading ? 'Shortening URL...' : 'Shorten URL'}
              data-testid="shorten-button"
            >
              {isLoading ? (
                <>
                  <span
                    className="loading loading-spinner loading-sm"
                    aria-hidden="true"
                  />
                  <span>Shortening...</span>
                </>
              ) : (
                HERO_CONTENT.submitButton
              )}
            </button>
          </div>
        </div>

        {displayError && (
          <div
            id="url-error"
            className="alert alert-error"
            role="alert"
            data-testid="error-message"
          >
            <svg
              xmlns="http://www.w3.org/2000/svg"
              className="stroke-current shrink-0 h-6 w-6"
              fill="none"
              viewBox="0 0 24 24"
              aria-hidden="true"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth="2"
                d="M10 14l2-2m0 0l2-2m-2 2l-2-2m2 2l2 2m7-2a9 9 0 11-18 0 9 9 0 0118 0z"
              />
            </svg>
            <span>{displayError}</span>
          </div>
        )}
      </div>
    </form>
  );
}
