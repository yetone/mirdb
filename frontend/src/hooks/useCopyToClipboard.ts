/**
 * Copy to Clipboard Hook.
 * Owner: Scenario 2 - Guest URL Creation Flow
 *
 * Handles clipboard functionality:
 * - Copies text to clipboard
 * - Tracks copied state with auto-reset
 * - Error handling for clipboard API failures
 */
import { useState, useCallback, useRef, useEffect } from 'react';

interface UseCopyToClipboardReturn {
  copy: (text: string) => Promise<boolean>;
  isCopied: boolean;
  error: string | null;
  reset: () => void;
}

export function useCopyToClipboard(resetDelay = 2000): UseCopyToClipboardReturn {
  const [isCopied, setIsCopied] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const timeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const reset = useCallback(() => {
    setIsCopied(false);
    setError(null);
  }, []);

  const copy = useCallback(async (text: string): Promise<boolean> => {
    if (!text) {
      setError('No text to copy');
      return false;
    }

    // Clear any existing timeout
    if (timeoutRef.current) {
      clearTimeout(timeoutRef.current);
    }

    try {
      // Check if clipboard API is available
      if (!navigator.clipboard || !navigator.clipboard.writeText) {
        throw new Error('Clipboard API not available');
      }

      await navigator.clipboard.writeText(text);
      setIsCopied(true);
      setError(null);

      // Auto-reset after delay
      timeoutRef.current = setTimeout(() => {
        setIsCopied(false);
      }, resetDelay);

      return true;
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : 'Failed to copy to clipboard';
      setError(errorMessage);
      setIsCopied(false);
      return false;
    }
  }, [resetDelay]);

  // Cleanup timeout on unmount
  useEffect(() => {
    return () => {
      if (timeoutRef.current) {
        clearTimeout(timeoutRef.current);
      }
    };
  }, []);

  return { copy, isCopied, error, reset };
}
