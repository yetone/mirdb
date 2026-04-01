/**
 * Clipboard Hook.
 * Owner: Scenario 13 - Code Examples Copy Functionality
 * Contributor: Scenario 2 - Usage Demo Section
 *
 * Custom hook for copy-to-clipboard functionality.
 */

import { useState, useCallback } from 'react'

interface UseClipboardReturn {
  copy: (text: string) => Promise<void>
  copied: boolean
  error: Error | null
}

export function useClipboard(timeout = 2000): UseClipboardReturn {
  const [copied, setCopied] = useState(false)
  const [error, setError] = useState<Error | null>(null)

  const copy = useCallback(async (text: string): Promise<void> => {
    try {
      if (navigator.clipboard && navigator.clipboard.writeText) {
        await navigator.clipboard.writeText(text)
      } else {
        // Fallback for older browsers
        const textarea = document.createElement('textarea')
        textarea.value = text
        textarea.style.position = 'fixed'
        textarea.style.left = '-9999px'
        document.body.appendChild(textarea)
        textarea.select()
        document.execCommand('copy')
        document.body.removeChild(textarea)
      }
      setCopied(true)
      setError(null)
      setTimeout(() => setCopied(false), timeout)
    } catch (err) {
      setError(err instanceof Error ? err : new Error('Failed to copy to clipboard'))
      setCopied(false)
    }
  }, [timeout])

  return { copy, copied, error }
}
