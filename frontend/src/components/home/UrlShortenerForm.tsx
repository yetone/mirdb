/**
 * URL Shortener Form Component
 * Owner: Scenario 3 - Anonymous URL Shortening
 *
 * Handles URL input, validation, API submission, and result display.
 * Works for both anonymous and authenticated users.
 *
 * Features:
 * - URL input with placeholder "Paste your long URL here..."
 * - Shorten button
 * - Validation feedback
 * - Result display with short URL
 * - Copy button integration
 */

import React, { useState, FormEvent, useCallback } from 'react'
import axios, { AxiosError } from 'axios'
import { ShortenResult, UrlShortenerFormProps, CreateUrlResponse, ApiErrorResponse } from '@/types/home'

/**
 * Create anonymous short URL - explicitly without authentication
 */
async function createAnonymousShortUrl(originalUrl: string): Promise<ShortenResult> {
  const response = await axios.post<CreateUrlResponse>(
    '/api/urls/anonymous',
    { original_url: originalUrl },
    { headers: { 'Content-Type': 'application/json' } }
  )

  const data = response.data
  const baseUrl = window.location.origin

  return {
    id: data.id,
    shortUrl: `${baseUrl}/r/${data.short_code}`,
    shortCode: data.short_code,
    shareToken: data.share_token,
    originalUrl: data.original_url,
    clickCount: data.click_count,
  }
}

export function UrlShortenerForm({ onSuccess, onError }: UrlShortenerFormProps) {
  const [url, setUrl] = useState('')
  const [isLoading, setIsLoading] = useState(false)
  const [result, setResult] = useState<ShortenResult | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [copied, setCopied] = useState(false)

  const handleSubmit = useCallback(async (e: FormEvent) => {
    e.preventDefault()

    const trimmedUrl = url.trim()
    if (!trimmedUrl) {
      setError('Please enter a URL')
      return
    }

    // Basic URL validation
    try {
      new URL(trimmedUrl)
    } catch {
      setError('Please enter a valid URL (including http:// or https://)')
      return
    }

    setIsLoading(true)
    setError(null)
    setResult(null)

    try {
      const shortenResult = await createAnonymousShortUrl(trimmedUrl)
      setResult(shortenResult)
      onSuccess?.(shortenResult)
    } catch (err) {
      const axiosError = err as AxiosError<ApiErrorResponse>
      const errorMessage = axiosError.response?.data?.detail || 'Failed to shorten URL. Please try again.'
      setError(errorMessage)
      onError?.(new Error(errorMessage))
    } finally {
      setIsLoading(false)
    }
  }, [url, onSuccess, onError])

  const handleCopy = useCallback(async () => {
    if (!result) return

    try {
      await navigator.clipboard.writeText(result.shortUrl)
      setCopied(true)
      setTimeout(() => setCopied(false), 2000)
    } catch {
      setError('Failed to copy to clipboard')
    }
  }, [result])

  return (
    <div className="w-full max-w-2xl mx-auto">
      <form onSubmit={handleSubmit} className="flex flex-col sm:flex-row gap-3">
        <div className="flex-1">
          <input
            type="text"
            value={url}
            onChange={(e) => setUrl(e.target.value)}
            placeholder="Paste your long URL here..."
            className="input input-bordered w-full bg-base-200 focus:outline-none focus:ring-2 focus:ring-primary"
            disabled={isLoading}
            aria-label="URL to shorten"
            aria-describedby={error ? 'url-error' : undefined}
          />
        </div>
        <button
          type="submit"
          className="btn btn-primary min-w-[120px]"
          disabled={isLoading}
          aria-label="Shorten URL"
        >
          {isLoading ? (
            <span className="loading loading-spinner loading-sm" />
          ) : (
            'Shorten'
          )}
        </button>
      </form>

      {error && (
        <div
          id="url-error"
          role="alert"
          className="alert alert-error mt-4"
        >
          <svg
            xmlns="http://www.w3.org/2000/svg"
            className="stroke-current shrink-0 h-6 w-6"
            fill="none"
            viewBox="0 0 24 24"
          >
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth="2"
              d="M10 14l2-2m0 0l2-2m-2 2l-2-2m2 2l2 2m7-2a9 9 0 11-18 0 9 9 0 0118 0z"
            />
          </svg>
          <span>{error}</span>
        </div>
      )}

      {result && (
        <div
          data-testid="shorten-result"
          className="mt-6 p-4 bg-base-200 rounded-lg"
        >
          <p className="text-sm text-base-content/70 mb-2">Your shortened URL:</p>
          <div className="flex flex-col sm:flex-row gap-3 items-start sm:items-center">
            <a
              href={result.shortUrl}
              target="_blank"
              rel="noopener noreferrer"
              className="text-primary font-mono text-lg break-all hover:underline"
            >
              {window.location.origin}/r/{result.shortCode}
            </a>
            <button
              type="button"
              onClick={handleCopy}
              className="btn btn-sm btn-outline"
              aria-label={copied ? 'Copied!' : 'Copy to clipboard'}
            >
              {copied ? (
                <>
                  <svg
                    xmlns="http://www.w3.org/2000/svg"
                    className="h-4 w-4"
                    viewBox="0 0 20 20"
                    fill="currentColor"
                  >
                    <path
                      fillRule="evenodd"
                      d="M16.707 5.293a1 1 0 010 1.414l-8 8a1 1 0 01-1.414 0l-4-4a1 1 0 011.414-1.414L8 12.586l7.293-7.293a1 1 0 011.414 0z"
                      clipRule="evenodd"
                    />
                  </svg>
                  Copied!
                </>
              ) : (
                <>
                  <svg
                    xmlns="http://www.w3.org/2000/svg"
                    className="h-4 w-4"
                    viewBox="0 0 20 20"
                    fill="currentColor"
                  >
                    <path d="M8 3a1 1 0 011-1h2a1 1 0 110 2H9a1 1 0 01-1-1z" />
                    <path d="M6 3a2 2 0 00-2 2v11a2 2 0 002 2h8a2 2 0 002-2V5a2 2 0 00-2-2 3 3 0 01-3 3H9a3 3 0 01-3-3z" />
                  </svg>
                  Copy
                </>
              )}
            </button>
          </div>
          {result.shareToken && (
            <p className="text-xs text-base-content/50 mt-3">
              Share token: <code className="bg-base-300 px-1 rounded">{result.shareToken}</code>
              <span className="ml-2">(Use this to access analytics)</span>
            </p>
          )}
        </div>
      )}
    </div>
  )
}
