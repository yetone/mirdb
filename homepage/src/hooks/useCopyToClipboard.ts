/**
 * Custom hook for copy-to-clipboard functionality.
 * Owner: Scenario 5 - Quick Start Section
 *
 * Features:
 * - Copy text to clipboard
 * - Track copied state for visual feedback
 * - Auto-reset copied state after delay
 */

import { useState, useCallback } from 'react'

interface UseCopyToClipboardReturn {
  copy: (text: string) => Promise<boolean>
  copied: boolean
}

export function useCopyToClipboard(resetDelay = 2000): UseCopyToClipboardReturn {
  const [copied, setCopied] = useState(false)

  const copy = useCallback(async (text: string): Promise<boolean> => {
    if (!navigator?.clipboard) {
      console.warn('Clipboard API not available')
      return false
    }

    try {
      await navigator.clipboard.writeText(text)
      setCopied(true)

      setTimeout(() => {
        setCopied(false)
      }, resetDelay)

      return true
    } catch (error) {
      console.error('Failed to copy to clipboard:', error)
      setCopied(false)
      return false
    }
  }, [resetDelay])

  return { copy, copied }
}
