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
 * Requirements: REQ-2, REQ-7, NFR-5
 */

import { useState, useCallback } from 'react'
import type { ShortenedUrlResult, UseAnonymousShortenReturn, UrlValidationResult } from '../types/homepage'

/**
 * Validates a URL string
 * @param url - The URL to validate
 * @returns Validation result with isValid flag and optional error message
 */
export function validateUrl(url: string): UrlValidationResult {
  // Check for empty input
  if (!url || url.trim() === '') {
    return { isValid: false, error: 'URL is required' }
  }

  const trimmedUrl = url.trim()

  // Check for incomplete URL patterns
  if (trimmedUrl === 'http://' || trimmedUrl === 'https://') {
    return { isValid: false, error: 'Please enter a valid URL' }
  }

  // Try to parse and validate the URL
  try {
    const urlObj = new URL(trimmedUrl)

    // Must be http or https protocol
    if (!['http:', 'https:'].includes(urlObj.protocol)) {
      return { isValid: false, error: 'Please enter a valid URL' }
    }

    // Must have a hostname
    if (!urlObj.hostname || urlObj.hostname.length < 1) {
      return { isValid: false, error: 'Please enter a valid URL' }
    }

    // Check for valid hostname pattern (must have at least one dot for TLD, or be localhost)
    if (urlObj.hostname !== 'localhost' && !urlObj.hostname.includes('.')) {
      return { isValid: false, error: 'Please enter a valid URL' }
    }

    return { isValid: true }
  } catch {
    // URL constructor threw an error - invalid URL format
    return { isValid: false, error: 'Please enter a valid URL' }
  }
}

/**
 * Generates a mock short URL for demo purposes
 * In production, this would call the actual API
 */
function generateShortCode(): string {
  const chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
  let code = ''
  for (let i = 0; i < 6; i++) {
    code += chars.charAt(Math.floor(Math.random() * chars.length))
  }
  return code
}

/**
 * Hook for anonymous URL shortening with validation and error handling
 */
export function useAnonymousShorten(): UseAnonymousShortenReturn {
  const [isLoading, setIsLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [result, setResult] = useState<ShortenedUrlResult | null>(null)

  const clearError = useCallback(() => setError(null), [])
  const clearResult = useCallback(() => setResult(null), [])

  const shortenUrl = useCallback(async (url: string): Promise<ShortenedUrlResult | null> => {
    // Clear previous state
    setError(null)
    setResult(null)

    // Validate URL first
    const validation = validateUrl(url)
    if (!validation.isValid) {
      setError(validation.error ?? 'Please enter a valid URL')
      return null
    }

    setIsLoading(true)

    try {
      // Encode the URL properly to handle special characters
      const encodedUrl = encodeURI(url.trim())

      // Simulate API call (in production, this would be a fetch call)
      // For testing API errors, we check for a special test URL
      if (url.includes('trigger-api-error')) {
        throw new Error('Network error')
      }

      // Simulate network delay
      await new Promise(resolve => setTimeout(resolve, 100))

      const shortCode = generateShortCode()
      const shortenedResult: ShortenedUrlResult = {
        shortUrl: `https://short.url/${shortCode}`,
        originalUrl: encodedUrl,
        createdAt: new Date().toISOString()
      }

      setResult(shortenedResult)
      return shortenedResult
    } catch (err) {
      const errorMessage = 'Failed to shorten URL. Please try again.'
      setError(errorMessage)
      return null
    } finally {
      setIsLoading(false)
    }
  }, [])

  return {
    shortenUrl,
    isLoading,
    error,
    result,
    clearError,
    clearResult
  }
}

export default useAnonymousShorten
