/**
 * Copy to Clipboard Hook
 * Owner: Scenario 5 - Quick Start Section
 *
 * Expected exports:
 * - useCopyToClipboard: Hook for copying text to clipboard
 *
 * Returns:
 * - copy: (text: string) => Promise<boolean>
 * - copied: boolean (reset after timeout)
 */

import { useState, useCallback } from 'react';

export function useCopyToClipboard(resetDelay: number = 2000) {
  const [copied, setCopied] = useState(false);

  const copy = useCallback(async (text: string): Promise<boolean> => {
    if (!navigator.clipboard) {
      // Fallback for older browsers
      try {
        const textArea = document.createElement('textarea');
        textArea.value = text;
        textArea.style.position = 'fixed';
        textArea.style.left = '-9999px';
        document.body.appendChild(textArea);
        textArea.select();
        document.execCommand('copy');
        document.body.removeChild(textArea);
        setCopied(true);
        setTimeout(() => setCopied(false), resetDelay);
        return true;
      } catch {
        return false;
      }
    }

    try {
      await navigator.clipboard.writeText(text);
      setCopied(true);
      setTimeout(() => setCopied(false), resetDelay);
      return true;
    } catch {
      return false;
    }
  }, [resetDelay]);

  return { copy, copied };
}
