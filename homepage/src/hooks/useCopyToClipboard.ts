/**
 * useCopyToClipboard hook.
 * Owner: Scenario 5 - Quick Start Section with Code Examples
 *
 * Returns:
 * - copy: (text: string) => Promise<boolean>
 * - copied: boolean
 * - reset: () => void
 *
 * Features:
 * - Clipboard API support
 * - Auto-reset after timeout
 *
 * Requirements: REQ-7
 */

'use client';

import { useState, useCallback, useRef, useEffect } from 'react';

interface UseCopyToClipboardReturn {
  copy: (text: string) => Promise<boolean>;
  copied: boolean;
  reset: () => void;
}

export function useCopyToClipboard(resetDelay = 2000): UseCopyToClipboardReturn {
  const [copied, setCopied] = useState(false);
  const timeoutRef = useRef<NodeJS.Timeout | null>(null);

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
      } catch {
        setCopied(false);
        return false;
      }
    },
    [resetDelay]
  );

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      if (timeoutRef.current) {
        clearTimeout(timeoutRef.current);
      }
    };
  }, []);

  return { copy, copied, reset };
}
