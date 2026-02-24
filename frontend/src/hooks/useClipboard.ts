/**
 * Clipboard Hook
 * Owner: Scenario 5 - Copy to Clipboard Functionality
 *
 * Custom hook for clipboard operations with feedback state.
 *
 * Features:
 * - Copy text to clipboard
 * - Track copied state (auto-resets after 2 seconds)
 * - Handle clipboard API errors gracefully
 */

import { useState, useCallback, useRef, useEffect } from 'react'

interface UseClipboardReturn {
  /** Copy text to clipboard */
  copy: (text: string) => Promise<boolean>
  /** Whether text was recently copied (resets after 2 seconds) */
  copied: boolean
  /** Error from last copy attempt, if any */
  error: Error | null
}

/**
 * Custom hook for clipboard operations with visual feedback state
 * @returns Object containing copy function, copied state, and error state
 */
export function useClipboard(): UseClipboardReturn {
  const [copied, setCopied] = useState(false)
  const [error, setError] = useState<Error | null>(null)
  const timeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  // Cleanup timeout on unmount
  useEffect(() => {
    return () => {
      if (timeoutRef.current) {
        clearTimeout(timeoutRef.current)
      }
    }
  }, [])

  const copy = useCallback(async (text: string): Promise<boolean> => {
    // Clear any existing timeout
    if (timeoutRef.current) {
      clearTimeout(timeoutRef.current)
    }

    // Reset states
    setError(null)
    setCopied(false)

    try {
      // Check if clipboard API is available
      if (!navigator?.clipboard?.writeText) {
        throw new Error('Clipboard API not available')
      }

      await navigator.clipboard.writeText(text)
      setCopied(true)

      // Reset copied state after 2 seconds
      timeoutRef.current = setTimeout(() => {
        setCopied(false)
      }, 2000)

      return true
    } catch (err) {
      const copyError = err instanceof Error ? err : new Error('Failed to copy to clipboard')
      setError(copyError)
      return false
    }
  }, [])

  return { copy, copied, error }
}

export default useClipboard
