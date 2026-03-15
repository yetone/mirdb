/**
 * Copy to Clipboard hook.
 * Owner: Scenario 3 - Quick Start Section with Code Examples
 *
 * Returns:
 * - copy: (text: string) => Promise<boolean>
 * - copied: boolean
 * - error: Error | null
 *
 * Features:
 * - Uses Clipboard API
 * - Returns success/failure status
 * - Auto-resets copied state after timeout
 */

import { useState, useCallback, useRef, useEffect } from 'react';

interface UseCopyToClipboardReturn {
  copy: (text: string) => Promise<boolean>;
  copied: boolean;
  error: Error | null;
}

export function useCopyToClipboard(resetTimeout: number = 2000): UseCopyToClipboardReturn {
  const [copied, setCopied] = useState(false);
  const [error, setError] = useState<Error | null>(null);
  const timeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    return () => {
      if (timeoutRef.current) {
        clearTimeout(timeoutRef.current);
      }
    };
  }, []);

  const copy = useCallback(async (text: string): Promise<boolean> => {
    // Clear any existing timeout
    if (timeoutRef.current) {
      clearTimeout(timeoutRef.current);
    }

    try {
      await navigator.clipboard.writeText(text);
      setCopied(true);
      setError(null);

      // Reset copied state after timeout
      timeoutRef.current = setTimeout(() => {
        setCopied(false);
      }, resetTimeout);

      return true;
    } catch (err) {
      const error = err instanceof Error ? err : new Error('Failed to copy to clipboard');
      setError(error);
      setCopied(false);
      return false;
    }
  }, [resetTimeout]);

  return { copy, copied, error };
}
