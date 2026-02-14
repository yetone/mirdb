/**
 * Custom hook for copy-to-clipboard functionality.
 * Owner: Scenario 6 - Code Examples and Quick Start
 *
 * Provides a copy function that uses the Clipboard API and
 * tracks the copied state for visual feedback.
 */
import { useState, useCallback, useRef, useEffect } from 'react';

interface UseCopyToClipboardResult {
  copy: (text: string) => Promise<boolean>;
  copied: boolean;
}

export function useCopyToClipboard(
  resetDelay: number = 2000
): UseCopyToClipboardResult {
  const [copied, setCopied] = useState(false);
  const timeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const copy = useCallback(
    async (text: string): Promise<boolean> => {
      if (!navigator?.clipboard) {
        console.warn('Clipboard API not available');
        return false;
      }

      try {
        await navigator.clipboard.writeText(text);
        setCopied(true);

        // Clear any existing timeout
        if (timeoutRef.current) {
          clearTimeout(timeoutRef.current);
        }

        // Reset copied state after delay
        timeoutRef.current = setTimeout(() => {
          setCopied(false);
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

  return { copy, copied };
}
