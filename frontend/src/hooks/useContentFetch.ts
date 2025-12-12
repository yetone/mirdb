import { useState, useEffect, useCallback } from 'react'

/**
 * Custom hook for fetching content with error handling and retry capability
 * Handles network failures, API errors, and provides fallback data support
 */

export interface ContentFetchError {
  message: string
  status?: number
  statusText?: string
}

export interface ContentFetchOptions<T> {
  /** Fallback data to use when fetch fails */
  fallbackData?: T
  /** Request timeout in milliseconds */
  timeout?: number
  /** Whether to fetch on mount */
  fetchOnMount?: boolean
}

export interface ContentFetchResult<T> {
  /** The fetched data (or fallback data on error) */
  data: T | null
  /** Whether the fetch is in progress */
  isLoading: boolean
  /** Error object if fetch failed */
  error: ContentFetchError | null
  /** Whether fallback data is being used */
  isUsingFallback: boolean
  /** Function to retry the fetch */
  retry: () => void
}

export function useContentFetch<T>(
  url: string,
  options: ContentFetchOptions<T> = {}
): ContentFetchResult<T> {
  const {
    fallbackData,
    timeout = 10000,
    fetchOnMount = true,
  } = options

  const [data, setData] = useState<T | null>(null)
  const [isLoading, setIsLoading] = useState<boolean>(fetchOnMount)
  const [error, setError] = useState<ContentFetchError | null>(null)
  const [isUsingFallback, setIsUsingFallback] = useState<boolean>(false)
  const [fetchTrigger, setFetchTrigger] = useState<number>(0)

  const fetchData = useCallback(async () => {
    setIsLoading(true)
    setError(null)
    setIsUsingFallback(false)

    try {
      // Create abort controller for timeout
      const controller = new AbortController()
      const timeoutId = setTimeout(() => controller.abort(), timeout)

      const response = await fetch(url, {
        signal: controller.signal,
      })

      clearTimeout(timeoutId)

      if (!response.ok) {
        throw {
          message: `HTTP error: ${response.status} ${response.statusText}`,
          status: response.status,
          statusText: response.statusText,
        } as ContentFetchError
      }

      const result = await response.json()
      setData(result)
      setError(null)
    } catch (err) {
      let fetchError: ContentFetchError

      if (err instanceof Error) {
        if (err.name === 'AbortError') {
          fetchError = { message: 'Request timeout' }
        } else {
          fetchError = { message: err.message }
        }
      } else if (typeof err === 'object' && err !== null && 'message' in err) {
        fetchError = err as ContentFetchError
      } else {
        fetchError = { message: 'Unknown error occurred' }
      }

      setError(fetchError)

      // Use fallback data if provided
      if (fallbackData) {
        setData(fallbackData)
        setIsUsingFallback(true)
      } else {
        setData(null)
      }
    } finally {
      setIsLoading(false)
    }
  }, [url, timeout, fallbackData])

  const retry = useCallback(() => {
    setFetchTrigger(prev => prev + 1)
  }, [])

  useEffect(() => {
    if (fetchOnMount || fetchTrigger > 0) {
      fetchData()
    }
  }, [fetchData, fetchOnMount, fetchTrigger])

  return {
    data,
    isLoading,
    error,
    isUsingFallback,
    retry,
  }
}
