/**
 * Clipboard Utilities
 * Owner: Scenario 7 - Copy Short URL Functionality
 *
 * Provides clipboard copy functionality with fallback.
 *
 * Exports:
 * - copyToClipboard(text: string): Promise<CopyResult> - Copy text to clipboard
 * - useCopyToClipboard(): { copy: fn, copied: boolean, error: string | null }
 *
 * Requirements:
 * - Use Clipboard API when available
 * - Fallback for older browsers
 * - Return success/failure status
 * - Hook for React components with copied state
 */

import { useState, useCallback, useRef } from 'react'

export interface CopyResult {
  success: boolean
  error?: string
}

/**
 * Check if the Clipboard API is available
 */
export const isClipboardApiAvailable = (): boolean => {
  return !!(
    typeof navigator !== 'undefined' &&
    navigator.clipboard &&
    typeof navigator.clipboard.writeText === 'function'
  )
}

/**
 * Fallback copy method using execCommand (for older browsers)
 * @param text - Text to copy to clipboard
 * @returns CopyResult indicating success or failure
 */
const fallbackCopy = (text: string): CopyResult => {
  try {
    const textArea = document.createElement('textarea')
    textArea.value = text

    // Avoid scrolling to bottom
    textArea.style.top = '0'
    textArea.style.left = '0'
    textArea.style.position = 'fixed'
    textArea.style.opacity = '0'
    textArea.style.pointerEvents = 'none'

    document.body.appendChild(textArea)
    textArea.focus()
    textArea.select()

    const success = document.execCommand('copy')
    document.body.removeChild(textArea)

    if (success) {
      return { success: true }
    }
    return { success: false, error: 'Failed to copy using fallback method' }
  } catch (err) {
    return {
      success: false,
      error: err instanceof Error ? err.message : 'Fallback copy failed'
    }
  }
}

/**
 * Copy text to clipboard using modern Clipboard API with fallback
 * @param text - Text to copy to clipboard
 * @returns Promise<CopyResult> indicating success or failure
 */
export const copyToClipboard = async (text: string): Promise<CopyResult> => {
  // Try modern Clipboard API first
  if (isClipboardApiAvailable()) {
    try {
      await navigator.clipboard.writeText(text)
      return { success: true }
    } catch (err) {
      // If Clipboard API fails (e.g., permission denied), try fallback
      console.warn('Clipboard API failed, trying fallback:', err)
      return fallbackCopy(text)
    }
  }

  // Use fallback for browsers without Clipboard API
  return fallbackCopy(text)
}

export interface UseCopyToClipboardOptions {
  /**
   * Duration in milliseconds to show "copied" state
   * @default 2000
   */
  resetDelay?: number
  /**
   * Callback called after successful copy
   */
  onSuccess?: (text: string) => void
  /**
   * Callback called after failed copy
   */
  onError?: (error: string) => void
}

export interface UseCopyToClipboardReturn {
  /** Function to trigger copy */
  copy: (text: string) => Promise<boolean>
  /** Whether content was recently copied */
  copied: boolean
  /** Error message if copy failed, null otherwise */
  error: string | null
  /** Reset the copied/error state */
  reset: () => void
}

/**
 * React hook for clipboard copy functionality
 * @param options - Configuration options
 * @returns Object with copy function, copied state, and error
 */
export const useCopyToClipboard = (
  options: UseCopyToClipboardOptions = {}
): UseCopyToClipboardReturn => {
  const { resetDelay = 2000, onSuccess, onError } = options

  const [copied, setCopied] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const timeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  const reset = useCallback(() => {
    setCopied(false)
    setError(null)
    if (timeoutRef.current) {
      clearTimeout(timeoutRef.current)
      timeoutRef.current = null
    }
  }, [])

  const copy = useCallback(async (text: string): Promise<boolean> => {
    // Clear any existing timeout
    if (timeoutRef.current) {
      clearTimeout(timeoutRef.current)
    }

    const result = await copyToClipboard(text)

    if (result.success) {
      setCopied(true)
      setError(null)
      onSuccess?.(text)

      // Reset copied state after delay
      timeoutRef.current = setTimeout(() => {
        setCopied(false)
      }, resetDelay)

      return true
    } else {
      setCopied(false)
      setError(result.error || 'Copy failed')
      onError?.(result.error || 'Copy failed')

      // Reset error state after delay
      timeoutRef.current = setTimeout(() => {
        setError(null)
      }, resetDelay)

      return false
    }
  }, [resetDelay, onSuccess, onError])

  return { copy, copied, error, reset }
}
