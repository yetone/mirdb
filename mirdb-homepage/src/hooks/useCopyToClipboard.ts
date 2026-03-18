/**
 * Copy to Clipboard Hook.
 * Owner: Scenario 3 - Usage Examples and Code Blocks
 *
 * Returns:
 * - copy: (text: string) => Promise<boolean>
 * - copied: boolean
 * - error: Error | null
 */

import { useState, useCallback, useRef } from 'react';
import { copyToClipboard } from '../utils/clipboard';

export interface UseCopyToClipboardReturn {
  copy: (text: string) => Promise<boolean>;
  copied: boolean;
  error: Error | null;
}

export function useCopyToClipboard(resetTimeout = 2000): UseCopyToClipboardReturn {
  const [copied, setCopied] = useState(false);
  const [error, setError] = useState<Error | null>(null);
  const timeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const copy = useCallback(async (text: string): Promise<boolean> => {
    // Clear any existing timeout
    if (timeoutRef.current) {
      clearTimeout(timeoutRef.current);
    }

    try {
      const success = await copyToClipboard(text);
      if (success) {
        setCopied(true);
        setError(null);
        timeoutRef.current = setTimeout(() => {
          setCopied(false);
        }, resetTimeout);
        return true;
      } else {
        setError(new Error('Failed to copy to clipboard'));
        setCopied(false);
        return false;
      }
    } catch (err) {
      const error = err instanceof Error ? err : new Error('Failed to copy to clipboard');
      setError(error);
      setCopied(false);
      return false;
    }
  }, [resetTimeout]);

  return { copy, copied, error };
}
