/**
 * URL Shortener Hook
 * Owner: Scenario 2 - Inline URL Shortening Demo
 *
 * Custom hook for URL shortening functionality:
 * - shortenUrl(url: string): Promise<ShortenedUrlResponse>
 * - isLoading: boolean
 * - error: string | null
 * - shortenedUrl: string | null
 * - copyToClipboard(): void
 * - isCopied: boolean
 * - reset(): void
 *
 * Uses the existing API client from src/api/index.ts
 */

import { useState, useCallback } from 'react'
import { shortenUrl as apiShortenUrl, ShortenUrlResponse } from '../api'
import { validateUrlWithMessage, sanitizeUrl } from '../utils/validation'

export interface UseUrlShortenerState {
  isLoading: boolean
  error: string | null
  shortenedUrl: string | null
  originalUrl: string | null
  shortCode: string | null
  isCopied: boolean
}

export interface UseUrlShortenerReturn extends UseUrlShortenerState {
  shortenUrl: (url: string) => Promise<ShortenUrlResponse | null>
  copyToClipboard: () => Promise<void>
  reset: () => void
}

const initialState: UseUrlShortenerState = {
  isLoading: false,
  error: null,
  shortenedUrl: null,
  originalUrl: null,
  shortCode: null,
  isCopied: false,
}

export function useUrlShortener(): UseUrlShortenerReturn {
  const [state, setState] = useState<UseUrlShortenerState>(initialState)

  const shortenUrl = useCallback(async (url: string): Promise<ShortenUrlResponse | null> => {
    // Validate the URL first
    const validation = validateUrlWithMessage(url)
    if (!validation.isValid) {
      setState((prev) => ({
        ...prev,
        error: validation.error || 'Invalid URL',
        isLoading: false,
      }))
      return null
    }

    // Sanitize the URL
    const sanitizedUrl = sanitizeUrl(url)

    setState((prev) => ({
      ...prev,
      isLoading: true,
      error: null,
      isCopied: false,
    }))

    try {
      const response = await apiShortenUrl({ url: sanitizedUrl })

      setState((prev) => ({
        ...prev,
        isLoading: false,
        shortenedUrl: response.shortUrl,
        originalUrl: response.originalUrl,
        shortCode: response.shortCode,
        error: null,
      }))

      return response
    } catch (err) {
      const errorMessage =
        err instanceof Error ? err.message : 'Failed to shorten URL. Please try again.'

      setState((prev) => ({
        ...prev,
        isLoading: false,
        error: errorMessage,
      }))

      return null
    }
  }, [])

  const copyToClipboard = useCallback(async () => {
    if (!state.shortenedUrl) {
      return
    }

    try {
      await navigator.clipboard.writeText(state.shortenedUrl)
      setState((prev) => ({
        ...prev,
        isCopied: true,
      }))

      // Reset the copied state after 2 seconds
      setTimeout(() => {
        setState((prev) => ({
          ...prev,
          isCopied: false,
        }))
      }, 2000)
    } catch (err) {
      setState((prev) => ({
        ...prev,
        error: 'Failed to copy to clipboard',
      }))
    }
  }, [state.shortenedUrl])

  const reset = useCallback(() => {
    setState(initialState)
  }, [])

  return {
    ...state,
    shortenUrl,
    copyToClipboard,
    reset,
  }
}

export default useUrlShortener
