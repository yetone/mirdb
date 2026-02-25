/**
 * Clipboard Hook for copy-to-clipboard functionality.
 * Owner: Scenario 3 - Usage Demonstration
 */

import { useState, useCallback } from 'react';

export interface UseClipboardReturn {
  copy: (text: string) => Promise<boolean>;
  copied: boolean;
}

export function useClipboard(resetDelay = 2000): UseClipboardReturn {
  const [copied, setCopied] = useState(false);

  const copy = useCallback(
    async (text: string): Promise<boolean> => {
      if (!navigator?.clipboard) {
        console.error('Clipboard API not available');
        return false;
      }

      try {
        await navigator.clipboard.writeText(text);
        setCopied(true);

        setTimeout(() => {
          setCopied(false);
        }, resetDelay);

        return true;
      } catch (error) {
        console.error('Failed to copy text:', error);
        setCopied(false);
        return false;
      }
    },
    [resetDelay]
  );

  return { copy, copied };
}
