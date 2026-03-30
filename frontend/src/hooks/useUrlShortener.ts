/**
 * Custom hook for URL shortening from homepage.
 * Owner: Scenario 5 - Guest URL Shortening with Redirect Flow
 *
 * Handles:
 * - URL submission for authenticated and guest users
 * - Redirect to registration for guests (preserving URL)
 * - Redirect to dashboard for authenticated users
 * - Loading and error states
 */

import { useState, useCallback } from 'react'
import { useNavigate } from 'react-router-dom'
import { useAuth } from '@/contexts/AuthContext'
import { UrlShortenerState } from '@/types/homepage'

/** Key used to store pending URL in localStorage for guest users */
export const PENDING_URL_KEY = 'pendingUrl'

/**
 * Hook for handling URL shortening with guest redirect flow.
 *
 * For authenticated users:
 * - Stores URL and navigates to dashboard
 *
 * For guest users:
 * - Stores URL in localStorage
 * - Redirects to registration page with URL preserved
 *
 * @returns {UrlShortenerState} Object with shortenUrl function, isLoading, and error state
 */
export const useUrlShortener = (): UrlShortenerState => {
  const [isLoading, setIsLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const navigate = useNavigate()
  const { isAuthenticated } = useAuth()

  const shortenUrl = useCallback(async (url: string): Promise<void> => {
    setError(null)
    setIsLoading(true)

    try {
      // Validate URL is not empty
      const trimmedUrl = url.trim()
      if (!trimmedUrl) {
        throw new Error('Please enter a URL')
      }

      // Store URL in localStorage for later use
      localStorage.setItem(PENDING_URL_KEY, trimmedUrl)

      if (isAuthenticated) {
        // Authenticated user: navigate to dashboard with URL
        navigate('/dashboard', {
          state: { urlToShorten: trimmedUrl }
        })
      } else {
        // Guest user: redirect to registration with URL preserved
        // The URL is stored in localStorage and also passed as query param
        const params = new URLSearchParams({ returnUrl: trimmedUrl })
        navigate(`/register?${params.toString()}`, {
          state: { urlToShorten: trimmedUrl }
        })
      }
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : 'Failed to process URL'
      setError(errorMessage)
      throw err
    } finally {
      setIsLoading(false)
    }
  }, [isAuthenticated, navigate])

  return {
    shortenUrl,
    isLoading,
    error,
  }
}

/**
 * Retrieves and clears the pending URL from localStorage.
 * Call this after successful registration to get the URL the user wanted to shorten.
 *
 * @returns {string | null} The pending URL or null if none exists
 */
export const getPendingUrl = (): string | null => {
  return localStorage.getItem(PENDING_URL_KEY)
}

/**
 * Clears the pending URL from localStorage.
 * Call this after the URL has been processed.
 */
export const clearPendingUrl = (): void => {
  localStorage.removeItem(PENDING_URL_KEY)
}

export default useUrlShortener
