/**
 * Inline URL Shortener Component
 * Owner: Scenario 2 - Inline URL Shortening, Scenario 3 - Validation
 *
 * Provides anonymous URL shortening functionality:
 * - URL input field with placeholder
 * - Shorten button
 * - Result display with copy button
 * - Registration prompt after success
 * - Loading states and error handling
 * - URL validation
 *
 * Requirements: REQ-2, REQ-7
 */

import { useState, useRef, useEffect, useCallback, FormEvent, KeyboardEvent } from 'react'
import { useAnonymousShorten } from '../../hooks/useAnonymousShorten'
import type { UrlValidationResult, ShortenedUrlResult } from '../../types/homepage'

export interface InlineShortenerProps {
  onShortenSuccess?: (shortUrl: string) => void
  onSuccess?: (result: ShortenedUrlResult) => void
  onError?: (error: string) => void
  autoFocus?: boolean
}

/**
 * Validates a URL string for proper format
 * @param url - The URL string to validate
 * @returns UrlValidationResult with isValid and optional error message
 */
export function validateUrl(url: string): UrlValidationResult {
  // Check for empty input
  if (!url || url.trim() === '') {
    return {
      isValid: false,
      error: 'URL is required',
    }
  }

  const trimmedUrl = url.trim()

  // Check for incomplete URLs (just protocol)
  if (/^https?:\/\/?$/i.test(trimmedUrl)) {
    return {
      isValid: false,
      error: 'Please enter a valid URL',
    }
  }

  // Try to parse the URL
  try {
    const parsedUrl = new URL(trimmedUrl)

    // Ensure the URL has a valid protocol
    if (!['http:', 'https:'].includes(parsedUrl.protocol)) {
      return {
        isValid: false,
        error: 'Please enter a valid URL',
      }
    }

    // Ensure the URL has a hostname
    if (!parsedUrl.hostname || parsedUrl.hostname.length === 0) {
      return {
        isValid: false,
        error: 'Please enter a valid URL',
      }
    }

    return { isValid: true }
  } catch {
    // URL constructor threw an error - invalid URL
    return {
      isValid: false,
      error: 'Please enter a valid URL',
    }
  }
}

/**
 * Encodes a URL ensuring special characters are properly handled
 * @param url - The URL string to encode
 * @returns The properly encoded URL
 */
export function encodeUrlForApi(url: string): string {
  try {
    // Parse and reconstruct to ensure proper encoding
    const parsed = new URL(url.trim())
    return parsed.href
  } catch {
    // If parsing fails, return the original (validation should catch this)
    return url.trim()
  }
}

export function InlineShortener({
  onShortenSuccess,
  onSuccess,
  onError,
  autoFocus = true,
}: InlineShortenerProps) {
  const [inputUrl, setInputUrl] = useState('')
  const [validationError, setValidationError] = useState<string | null>(null)
  const [copied, setCopied] = useState(false)
  const inputRef = useRef<HTMLInputElement>(null)
  const { shortenUrl, isLoading, error: apiError, result, reset } = useAnonymousShorten()

  // Combined error from validation or API
  const displayError = validationError || apiError

  // Auto-focus input on mount
  useEffect(() => {
    if (autoFocus && inputRef.current) {
      inputRef.current.focus()
    }
  }, [autoFocus])

  const handleSubmit = useCallback(async (e: FormEvent) => {
    e.preventDefault()

    // Clear previous errors
    setValidationError(null)
    setCopied(false)
    reset()

    // Validate URL
    const validation = validateUrl(inputUrl)
    if (!validation.isValid) {
      setValidationError(validation.error || 'Please enter a valid URL')
      onError?.(validation.error || 'Please enter a valid URL')
      return
    }

    // Encode URL for API
    const encodedUrl = encodeUrlForApi(inputUrl)

    try {
      const shortenedResult = await shortenUrl(encodedUrl)
      if (shortenedResult) {
        onShortenSuccess?.(shortenedResult.shortUrl)
        onSuccess?.(shortenedResult)
      }
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : 'Failed to shorten URL. Please try again.'
      onError?.(errorMessage)
    }
  }, [inputUrl, shortenUrl, onShortenSuccess, onSuccess, onError, reset])

  const handleKeyDown = (e: KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter' && !isLoading) {
      handleSubmit(e as unknown as FormEvent)
    }
  }

  const handleCopy = useCallback(async () => {
    if (result?.shortUrl) {
      try {
        await navigator.clipboard.writeText(result.shortUrl)
        setCopied(true)
        setTimeout(() => setCopied(false), 2000)
      } catch {
        // Fallback for older browsers
        const textArea = document.createElement('textarea')
        textArea.value = result.shortUrl
        document.body.appendChild(textArea)
        textArea.select()
        document.execCommand('copy')
        document.body.removeChild(textArea)
        setCopied(true)
        setTimeout(() => setCopied(false), 2000)
      }
    }
  }, [result])

  const handleRetry = useCallback(() => {
    setValidationError(null)
    reset()
  }, [reset])

  const handleReset = () => {
    setInputUrl('')
    setCopied(false)
    reset()
    inputRef.current?.focus()
  }

  return (
    <section
      data-testid="inline-shortener"
      className="w-full max-w-2xl mx-auto px-4 py-8"
    >
      <form onSubmit={handleSubmit} className="flex flex-col gap-4">
        <div className="join w-full flex flex-col sm:flex-row gap-2 sm:gap-0">
          <input
            ref={inputRef}
            type="text"
            data-testid="url-input"
            className={`input input-bordered join-item w-full sm:flex-1 ${displayError ? 'input-error' : ''}`}
            placeholder="Paste your long URL here..."
            value={inputUrl}
            onChange={(e) => {
              setInputUrl(e.target.value)
              if (validationError) setValidationError(null)
            }}
            onKeyDown={handleKeyDown}
            disabled={isLoading}
            aria-label="URL to shorten"
            aria-invalid={displayError ? 'true' : 'false'}
            aria-describedby={displayError ? 'url-error' : undefined}
          />
          <button
            type="submit"
            data-testid="shorten-button"
            className="btn btn-primary join-item"
            disabled={isLoading || !inputUrl.trim()}
            aria-busy={isLoading}
          >
            {isLoading ? (
              <>
                <span className="loading loading-spinner loading-sm" aria-hidden="true"></span>
                <span className="sr-only">Shortening...</span>
              </>
            ) : (
              'Shorten'
            )}
          </button>
        </div>

        {/* Error display */}
        {displayError && (
          <div
            id="url-error"
            data-testid="error-message"
            className="alert alert-error"
            role="alert"
          >
            <svg
              xmlns="http://www.w3.org/2000/svg"
              className="stroke-current shrink-0 h-6 w-6"
              fill="none"
              viewBox="0 0 24 24"
              aria-hidden="true"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth="2"
                d="M10 14l2-2m0 0l2-2m-2 2l-2-2m2 2l2 2m7-2a9 9 0 11-18 0 9 9 0 0118 0z"
              />
            </svg>
            <span>{displayError}</span>
          </div>
        )}

        {/* Retry button for API errors */}
        {apiError && !validationError && (
          <div>
            <button
              type="button"
              onClick={handleRetry}
              className="btn btn-sm btn-ghost"
              data-testid="retry-button"
            >
              Try again
            </button>
          </div>
        )}

        {/* Result display */}
        {result && (
          <div
            data-testid="result-area"
            className="bg-base-200 rounded-lg p-4 flex flex-col gap-4"
          >
            <div className="flex items-center gap-2 flex-wrap">
              <span className="text-sm text-base-content/70">Your short URL:</span>
              <a
                href={result.shortUrl}
                data-testid="short-url"
                className="link link-primary font-mono break-all"
                target="_blank"
                rel="noopener noreferrer"
              >
                {result.shortUrl}
              </a>
            </div>

            <div className="flex gap-2 flex-wrap">
              <button
                type="button"
                data-testid="copy-button"
                className={`btn btn-sm ${copied ? 'btn-success' : 'btn-outline'}`}
                onClick={handleCopy}
                aria-label={copied ? 'Copied to clipboard' : 'Copy short URL to clipboard'}
              >
                {copied ? (
                  <>
                    <svg
                      xmlns="http://www.w3.org/2000/svg"
                      className="h-4 w-4"
                      fill="none"
                      viewBox="0 0 24 24"
                      stroke="currentColor"
                      aria-hidden="true"
                    >
                      <path
                        strokeLinecap="round"
                        strokeLinejoin="round"
                        strokeWidth={2}
                        d="M5 13l4 4L19 7"
                      />
                    </svg>
                    <span data-testid="copied-feedback">Copied!</span>
                  </>
                ) : (
                  <>
                    <svg
                      xmlns="http://www.w3.org/2000/svg"
                      className="h-4 w-4"
                      fill="none"
                      viewBox="0 0 24 24"
                      stroke="currentColor"
                      aria-hidden="true"
                    >
                      <path
                        strokeLinecap="round"
                        strokeLinejoin="round"
                        strokeWidth={2}
                        d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z"
                      />
                    </svg>
                    Copy
                  </>
                )}
              </button>

              <button
                type="button"
                data-testid="shorten-another-button"
                className="btn btn-sm btn-ghost"
                onClick={handleReset}
              >
                Shorten another
              </button>
            </div>

            {/* Registration prompt after success */}
            <div
              data-testid="registration-prompt"
              className="border-t border-base-300 pt-4 mt-2"
            >
              <p className="text-sm text-base-content/70 mb-2">
                Want to track analytics for your links?
              </p>
              <a
                href="/register"
                className="btn btn-secondary btn-sm"
                data-testid="register-cta"
              >
                Create account to track analytics
              </a>
            </div>
          </div>
        )}
      </form>
    </section>
  )
}

export default InlineShortener
