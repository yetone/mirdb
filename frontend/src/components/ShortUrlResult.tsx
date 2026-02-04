/**
 * Shortened URL result display component.
 * Owner: Scenario 2 - Anonymous URL Shortening
 *
 * Props:
 * - shortUrl: string - The full shortened URL
 * - onCopy: () => void - Callback when copy button clicked
 * - copied: boolean - Whether URL was recently copied
 *
 * Expected behavior:
 * - Displays shortened URL prominently
 * - Copy-to-clipboard button with visual feedback
 * - Tooltip showing "Copied!" on successful copy
 * - Accessible with proper ARIA live region
 */
import React from 'react';

export interface ShortUrlResultProps {
  shortUrl: string;
  onCopy: () => void;
  copied: boolean;
}

export function ShortUrlResult({
  shortUrl,
  onCopy,
  copied,
}: ShortUrlResultProps): React.ReactElement {
  return (
    <div
      className="w-full max-w-2xl mx-auto mt-6 p-4 bg-base-200 rounded-lg"
      role="region"
      aria-label="Shortened URL result"
    >
      <p className="text-sm text-base-content/70 mb-2">Your shortened URL:</p>
      <div className="flex flex-col sm:flex-row items-stretch sm:items-center gap-3">
        <div className="flex-1 bg-base-100 rounded-lg p-3 overflow-hidden">
          <a
            href={shortUrl}
            target="_blank"
            rel="noopener noreferrer"
            className="text-primary font-medium break-all hover:underline"
          >
            {shortUrl}
          </a>
        </div>
        <button
          type="button"
          onClick={onCopy}
          className={`btn ${copied ? 'btn-success' : 'btn-secondary'} min-w-[100px]`}
          aria-label={copied ? 'URL copied to clipboard' : 'Copy shortened URL to clipboard'}
        >
          {copied ? (
            <>
              <svg
                xmlns="http://www.w3.org/2000/svg"
                className="h-5 w-5"
                viewBox="0 0 20 20"
                fill="currentColor"
                aria-hidden="true"
              >
                <path
                  fillRule="evenodd"
                  d="M16.707 5.293a1 1 0 010 1.414l-8 8a1 1 0 01-1.414 0l-4-4a1 1 0 011.414-1.414L8 12.586l7.293-7.293a1 1 0 011.414 0z"
                  clipRule="evenodd"
                />
              </svg>
              Copied!
            </>
          ) : (
            <>
              <svg
                xmlns="http://www.w3.org/2000/svg"
                className="h-5 w-5"
                viewBox="0 0 20 20"
                fill="currentColor"
                aria-hidden="true"
              >
                <path d="M8 3a1 1 0 011-1h2a1 1 0 110 2H9a1 1 0 01-1-1z" />
                <path d="M6 3a2 2 0 00-2 2v11a2 2 0 002 2h8a2 2 0 002-2V5a2 2 0 00-2-2 3 3 0 01-3 3H9a3 3 0 01-3-3z" />
              </svg>
              Copy
            </>
          )}
        </button>
      </div>
      <div aria-live="polite" aria-atomic="true" className="sr-only">
        {copied && 'URL copied to clipboard'}
      </div>
    </div>
  );
}

export default ShortUrlResult;
