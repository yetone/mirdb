/**
 * Copy to Clipboard Hook
 * Owner: Scenario 4 - Getting Started Section
 *
 * Custom hook for copying text to clipboard with:
 * - Copy function
 * - Success/error state
 * - Reset functionality
 */

import { useState, useCallback } from 'react';

export type CopyState = 'idle' | 'success' | 'error';

export interface UseCopyToClipboardReturn {
  copy: (text: string) => Promise<void>;
  state: CopyState;
  reset: () => void;
}

export function useCopyToClipboard(resetDelay: number = 2000): UseCopyToClipboardReturn {
  const [state, setState] = useState<CopyState>('idle');

  const reset = useCallback(() => {
    setState('idle');
  }, []);

  const copy = useCallback(async (text: string) => {
    try {
      await navigator.clipboard.writeText(text);
      setState('success');

      // Auto-reset after delay
      setTimeout(() => {
        setState('idle');
      }, resetDelay);
    } catch (error) {
      setState('error');

      // Auto-reset after delay
      setTimeout(() => {
        setState('idle');
      }, resetDelay);
    }
  }, [resetDelay]);

  return { copy, state, reset };
}
