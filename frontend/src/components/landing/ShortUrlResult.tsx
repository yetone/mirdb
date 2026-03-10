/**
 * Short URL Result Component
 * Owner: Scenario 1 - Hero Section URL Shortening
 *
 * Displays the shortened URL with copy-to-clipboard functionality.
 */

import { useClipboard } from '../../hooks/useClipboard';

export interface ShortUrlResultProps {
  shortUrl: string;
}

/**
 * Component to display the shortened URL with a copy button
 */
export function ShortUrlResult({ shortUrl }: ShortUrlResultProps) {
  const { copy, copied } = useClipboard();

  const handleCopy = async () => {
    await copy(shortUrl);
  };

  return (
    <div
      className="w-full p-4 rounded-lg bg-base-200 border border-base-300"
      role="region"
      aria-label="Shortened URL result"
    >
      <div className="flex flex-col sm:flex-row items-center gap-3">
        <div className="flex-1 w-full">
          <label className="text-sm text-base-content/70 mb-1 block">
            Your shortened URL:
          </label>
          <div
            className="text-lg font-mono text-primary break-all"
            data-testid="short-url-display"
          >
            {shortUrl}
          </div>
        </div>
        <button
          type="button"
          onClick={handleCopy}
          className={`btn ${copied ? 'btn-success' : 'btn-primary'} min-w-[100px]`}
          aria-label={copied ? 'Copied to clipboard' : 'Copy short URL to clipboard'}
          data-testid="copy-button"
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
    </div>
  );
}
