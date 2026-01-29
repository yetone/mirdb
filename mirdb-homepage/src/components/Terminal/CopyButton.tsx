/**
 * Copy to Clipboard Button Component.
 * Owner: Scenario 3 - Interactive Terminal Component
 *
 * Expected features:
 * - Copy text to clipboard on click
 * - Visual feedback on successful copy
 * - Accessible button with aria-label
 * - Graceful degradation if Clipboard API unavailable
 */
import { useState, useEffect } from 'react';

interface CopyButtonProps {
  text: string;
  className?: string;
}

export function CopyButton({ text, className = '' }: CopyButtonProps) {
  const [copied, setCopied] = useState(false);
  const [isSupported, setIsSupported] = useState(true);

  useEffect(() => {
    // Check if Clipboard API is available
    setIsSupported(typeof navigator !== 'undefined' && !!navigator.clipboard);
  }, []);

  const handleCopy = async () => {
    if (!isSupported) return;

    try {
      await navigator.clipboard.writeText(text);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch (err) {
      console.error('Failed to copy text:', err);
    }
  };

  // Don't render button if clipboard API is not supported
  if (!isSupported) {
    return null;
  }

  return (
    <button
      type="button"
      onClick={handleCopy}
      className={`copy-button px-3 py-1.5 rounded text-sm font-mono transition-colors duration-200 ${
        copied
          ? 'bg-success text-background'
          : 'bg-surface text-text-secondary hover:bg-border hover:text-text-primary'
      } ${className}`}
      aria-label={copied ? 'Copied!' : 'Copy to clipboard'}
      data-testid="copy-button"
    >
      {copied ? (
        <span className="flex items-center gap-1" data-testid="copy-success">
          <svg
            xmlns="http://www.w3.org/2000/svg"
            className="h-4 w-4"
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
        </span>
      ) : (
        <span className="flex items-center gap-1">
          <svg
            xmlns="http://www.w3.org/2000/svg"
            className="h-4 w-4"
            viewBox="0 0 20 20"
            fill="currentColor"
            aria-hidden="true"
          >
            <path d="M8 3a1 1 0 011-1h2a1 1 0 110 2H9a1 1 0 01-1-1z" />
            <path d="M6 3a2 2 0 00-2 2v11a2 2 0 002 2h8a2 2 0 002-2V5a2 2 0 00-2-2 3 3 0 01-3 3H9a3 3 0 01-3-3z" />
          </svg>
          Copy
        </span>
      )}
    </button>
  );
}
