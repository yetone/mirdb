/**
 * Clipboard Hook
 * Owner: Scenario 1 - Hero Section URL Shortening
 *
 * Custom hook for clipboard copy functionality with feedback.
 */

import { useState, useCallback, useRef, useEffect } from 'react';

export interface UseClipboardReturn {
  copy: (text: string) => Promise<boolean>;
  copied: boolean;
  reset: () => void;
}

/**
 * Custom hook for copying text to clipboard with visual feedback
 *
 * @param resetDelay - Time in ms before copied state resets (default: 2000)
 * @returns Object with copy function, copied state, and reset function
 */
export function useClipboard(resetDelay = 2000): UseClipboardReturn {
  const [copied, setCopied] = useState(false);
  const timeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const reset = useCallback(() => {
    setCopied(false);
    if (timeoutRef.current) {
      clearTimeout(timeoutRef.current);
      timeoutRef.current = null;
    }
  }, []);

  const copy = useCallback(
    async (text: string): Promise<boolean> => {
      // Clear any existing timeout
      if (timeoutRef.current) {
        clearTimeout(timeoutRef.current);
      }

      try {
        await navigator.clipboard.writeText(text);
        setCopied(true);

        // Auto-reset after delay
        timeoutRef.current = setTimeout(() => {
          setCopied(false);
          timeoutRef.current = null;
        }, resetDelay);

        return true;
      } catch (error) {
        console.error('Failed to copy to clipboard:', error);
        setCopied(false);
        return false;
      }
    },
    [resetDelay]
  );

  // Cleanup timeout on unmount
  useEffect(() => {
    return () => {
      if (timeoutRef.current) {
        clearTimeout(timeoutRef.current);
      }
    };
  }, []);

  return { copy, copied, reset };
}
