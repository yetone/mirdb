/**
 * useCopyToClipboard Hook
 * Owner: Scenario 4 - Quick Start Section
 *
 * Custom hook for copying text to clipboard.
 * Returns copy function and copied state that auto-resets.
 */
import { useState, useCallback } from 'react'

interface UseCopyToClipboardReturn {
  copy: (text: string) => Promise<boolean>
  copied: boolean
  error: Error | null
}

export function useCopyToClipboard(resetDelay = 2000): UseCopyToClipboardReturn {
  const [copied, setCopied] = useState(false)
  const [error, setError] = useState<Error | null>(null)

  const copy = useCallback(async (text: string): Promise<boolean> => {
    if (!navigator?.clipboard) {
      const err = new Error('Clipboard API not available')
      setError(err)
      return false
    }

    try {
      await navigator.clipboard.writeText(text)
      setCopied(true)
      setError(null)

      setTimeout(() => {
        setCopied(false)
      }, resetDelay)

      return true
    } catch (err) {
      const error = err instanceof Error ? err : new Error('Failed to copy')
      setError(error)
      setCopied(false)
      return false
    }
  }, [resetDelay])

  return { copy, copied, error }
}
