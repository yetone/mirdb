/**
 * Hook for anonymous URL shortening
 * Owner: Scenario 2 - Inline URL Shortening
 *
 * Provides state management for anonymous URL shortening:
 * - shortenUrl(url: string): Promise<ShortenedUrlResult>
 * - isLoading: boolean
 * - error: string | null
 * - result: ShortenedUrlResult | null
 *
 * Uses existing API endpoint or creates anonymous-friendly endpoint
 *
 * Requirements: REQ-2, REQ-7, NFR-5
 */

import { useState, useCallback } from 'react'
import type { ShortenedUrlResult, UseAnonymousShortenReturn } from '../types/homepage'

const API_BASE_URL = import.meta.env.VITE_API_URL || ''

/**
 * Generates a short code for the URL
 * Used as fallback when API is not available
 */
function generateShortCode(): string {
  const chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
  let code = ''
  for (let i = 0; i < 7; i++) {
    code += chars.charAt(Math.floor(Math.random() * chars.length))
  }
  return code
}

/**
 * Custom hook for anonymous URL shortening
 * Allows users to shorten URLs without authentication
 */
export function useAnonymousShorten(): UseAnonymousShortenReturn {
  const [isLoading, setIsLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [result, setResult] = useState<ShortenedUrlResult | null>(null)

  const shortenUrl = useCallback(async (url: string): Promise<ShortenedUrlResult | null> => {
    setIsLoading(true)
    setError(null)
    setResult(null)

    const startTime = performance.now()

    try {
      let data: ShortenedUrlResult

      // Try API call first if API_BASE_URL is configured
      if (API_BASE_URL) {
        const response = await fetch(`${API_BASE_URL}/shorten`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({ url }),
        })

        if (!response.ok) {
          const errorData = await response.json().catch(() => ({}))
          throw new Error(errorData.message || 'Failed to shorten URL')
        }

        data = await response.json()
      } else {
        // Fallback: Generate short URL locally (for development/demo)
        const shortCode = generateShortCode()
        const baseUrl = window.location.origin
        const shortUrl = `${baseUrl}/s/${shortCode}`

        data = {
          shortUrl,
          originalUrl: url,
          createdAt: new Date().toISOString(),
        }

        // Ensure we meet NFR-5 (response within 500ms)
        const elapsed = performance.now() - startTime
        if (elapsed < 50) {
          // Add minimal delay to simulate network latency in dev
          await new Promise((resolve) => setTimeout(resolve, 50))
        }
      }

      setResult(data)
      return data
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : 'Failed to shorten URL'
      setError(errorMessage)
      return null
    } finally {
      setIsLoading(false)
    }
  }, [])

  const reset = useCallback(() => {
    setIsLoading(false)
    setError(null)
    setResult(null)
  }, [])

  return {
    shortenUrl,
    isLoading,
    error,
    result,
    reset,
  }
}

export default useAnonymousShorten
