/**
 * Guest URL Shortener Widget
 * Owner: Scenario 5 - Guest URL Shortening Success Flow
 *
 * Allows unauthenticated users to shorten URLs directly on homepage.
 *
 * States:
 * - idle: Input field ready for URL
 * - loading: API call in progress
 * - success: Short URL created, display result
 * - error: Validation or API error
 *
 * Requirements:
 * - URL input field with placeholder
 * - Shorten button (FuturisticButton)
 * - Loading state during API call
 * - Success state with short URL and copy button
 * - View Analytics link after success
 * - "Create account" prompt after success
 * - Error state with user-friendly messages
 */
import React, { useState } from 'react'
import { Link } from 'react-router-dom'
import { FuturisticButton } from './FuturisticButton'
import { GlassMorphismCard } from './GlassMorphismCard'
import { shortenUrl } from '../api'
import type { ShortenUrlResponse } from '../api'
import { validateUrl, parseApiError, type ApiErrorCode } from '../utils/validation'
import { useCopyToClipboard } from '../utils/clipboard'

type ShortenerState = 'idle' | 'loading' | 'success' | 'error'

const GuestShortener: React.FC = () => {
  const [url, setUrl] = useState('')
  const [state, setState] = useState<ShortenerState>('idle')
  const [result, setResult] = useState<ShortenUrlResponse | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [errorCode, setErrorCode] = useState<ApiErrorCode | null>(null)
  const [isRetryable, setIsRetryable] = useState(false)
  const [toastMessage, setToastMessage] = useState<string | null>(null)
  const [toastType, setToastType] = useState<'success' | 'error'>('success')

  const { copy, copied, error: copyError, reset: resetCopy } = useCopyToClipboard({
    resetDelay: 2000,
    onSuccess: () => {
      setToastMessage('URL copied to clipboard!')
      setToastType('success')
      setTimeout(() => setToastMessage(null), 2000)
    },
    onError: (err) => {
      setToastMessage(err || 'Failed to copy URL')
      setToastType('error')
      setTimeout(() => setToastMessage(null), 3000)
    }
  })

  const getShortUrl = (shortCode: string): string => {
    const baseUrl = window.location.origin
    return `${baseUrl}/r/${shortCode}`
  }

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()

    // Client-side validation
    const validation = validateUrl(url)
    if (!validation.valid) {
      setError(validation.error || 'Please enter a valid URL')
      setErrorCode('VALIDATION_ERROR')
      setIsRetryable(false)
      setState('error')
      return
    }

    setState('loading')
    setError(null)
    setErrorCode(null)
    setIsRetryable(false)

    try {
      const response = await shortenUrl(url)
      setResult(response)
      setState('success')
    } catch (err) {
      const apiError = parseApiError(err)
      setError(apiError.message)
      setErrorCode(apiError.code)
      setIsRetryable(apiError.retryable)
      setState('error')
    }
  }

  const handleRetry = () => {
    // Re-trigger the submission with the same URL
    const fakeEvent = { preventDefault: () => {} } as React.FormEvent
    handleSubmit(fakeEvent)
  }

  const handleCopy = async () => {
    if (!result) return

    const shortUrl = getShortUrl(result.short_code)
    await copy(shortUrl)
  }

  const handleReset = () => {
    setUrl('')
    setState('idle')
    setResult(null)
    setError(null)
    setErrorCode(null)
    setIsRetryable(false)
    resetCopy()
    setToastMessage(null)
  }

  return (
    <GlassMorphismCard className="w-full max-w-2xl mx-auto">
      <div className="text-center mb-6">
        <h2 className="text-2xl font-bold mb-2">Try It Now</h2>
        <p className="text-base-content/70">
          Shorten your first URL - no account required
        </p>
      </div>

      {state === 'success' && result ? (
        <div className="space-y-4">
          {/* Success state */}
          <div
            className="alert alert-success"
            role="status"
            aria-live="polite"
            data-testid="success-message"
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
                d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z"
              />
            </svg>
            <span>Your short URL is ready!</span>
          </div>

          {/* Short URL display with copy button */}
          <div className="flex items-center gap-2 bg-base-200 p-4 rounded-lg">
            <code
              className="flex-1 text-primary font-mono text-lg break-all"
              data-testid="short-url"
            >
              {getShortUrl(result.short_code)}
            </code>
            <button
              onClick={handleCopy}
              className="btn btn-ghost btn-sm"
              aria-label="Copy short URL to clipboard"
              data-testid="copy-button"
            >
              {copied ? (
                <svg
                  xmlns="http://www.w3.org/2000/svg"
                  className="h-5 w-5 text-success"
                  viewBox="0 0 20 20"
                  fill="currentColor"
                  aria-hidden="true"
                >
                  <path
                    fillRule="evenodd"
                    d="M16.707 5.293a1 1 0 010 1.414l-8 8a1 1 0 01-1.414 0l-4-4a1 1 0 011.414-1.414L8 12.586l7.293-7.293a1 1 0 011.414 0z"
                    clipRule="evenodd"
                  />
                </svg>
              ) : (
                <svg
                  xmlns="http://www.w3.org/2000/svg"
                  className="h-5 w-5"
                  viewBox="0 0 20 20"
                  fill="currentColor"
                  aria-hidden="true"
                >
                  <path d="M8 3a1 1 0 011-1h2a1 1 0 110 2H9a1 1 0 01-1-1z" />
                  <path d="M6 3a2 2 0 00-2 2v11a2 2 0 002 2h8a2 2 0 002-2V5a2 2 0 00-2-2 3 3 0 01-3 3H9a3 3 0 01-3-3z" />
                </svg>
              )}
            </button>
          </div>

          {/* View Analytics link */}
          <div className="flex flex-col sm:flex-row gap-2 justify-center">
            <Link
              to={`/stats/${result.short_code}`}
              className="btn btn-outline btn-sm"
              data-testid="view-analytics-link"
            >
              <svg
                xmlns="http://www.w3.org/2000/svg"
                className="h-4 w-4 mr-1"
                viewBox="0 0 20 20"
                fill="currentColor"
                aria-hidden="true"
              >
                <path d="M2 11a1 1 0 011-1h2a1 1 0 011 1v5a1 1 0 01-1 1H3a1 1 0 01-1-1v-5zM8 7a1 1 0 011-1h2a1 1 0 011 1v9a1 1 0 01-1 1H9a1 1 0 01-1-1V7zM14 4a1 1 0 011-1h2a1 1 0 011 1v12a1 1 0 01-1 1h-2a1 1 0 01-1-1V4z" />
              </svg>
              View Analytics
            </Link>
            <button
              onClick={handleReset}
              className="btn btn-ghost btn-sm"
            >
              Shorten Another URL
            </button>
          </div>

          {/* Account prompt */}
          <div
            className="divider"
            role="separator"
          />
          <div
            className="text-center"
            data-testid="account-prompt"
          >
            <p className="text-base-content/70 mb-2">
              Want to track all your URLs and access advanced analytics?
            </p>
            <Link
              to="/register"
              className="btn btn-primary btn-sm"
            >
              Create Free Account
            </Link>
          </div>
        </div>
      ) : (
        <form onSubmit={handleSubmit} className="space-y-4">
          {/* Error state */}
          {state === 'error' && error && (
            <div
              className="alert alert-error"
              role="alert"
              aria-live="assertive"
              data-testid="error-message"
              data-error-code={errorCode}
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
              <div className="flex-1">
                <span data-testid="error-text">{error}</span>
              </div>
              {isRetryable && (
                <button
                  type="button"
                  onClick={handleRetry}
                  className="btn btn-sm btn-ghost"
                  data-testid="retry-button"
                  aria-label="Retry shortening URL"
                >
                  <svg
                    xmlns="http://www.w3.org/2000/svg"
                    className="h-4 w-4 mr-1"
                    viewBox="0 0 20 20"
                    fill="currentColor"
                    aria-hidden="true"
                  >
                    <path
                      fillRule="evenodd"
                      d="M4 2a1 1 0 011 1v2.101a7.002 7.002 0 0111.601 2.566 1 1 0 11-1.885.666A5.002 5.002 0 005.999 7H9a1 1 0 010 2H4a1 1 0 01-1-1V3a1 1 0 011-1zm.008 9.057a1 1 0 011.276.61A5.002 5.002 0 0014.001 13H11a1 1 0 110-2h5a1 1 0 011 1v5a1 1 0 11-2 0v-2.101a7.002 7.002 0 01-11.601-2.566 1 1 0 01.61-1.276z"
                      clipRule="evenodd"
                    />
                  </svg>
                  Retry
                </button>
              )}
            </div>
          )}

          {/* URL Input */}
          <div className="form-control">
            <label htmlFor="url-input" className="label sr-only">
              <span className="label-text">URL to shorten</span>
            </label>
            <input
              id="url-input"
              type="url"
              value={url}
              onChange={(e) => setUrl(e.target.value)}
              placeholder="Enter your long URL here..."
              className="input input-bordered input-lg w-full min-h-[44px] touch-manipulation"
              disabled={state === 'loading'}
              aria-describedby={error ? 'url-error' : undefined}
              data-testid="url-input"
            />
          </div>

          {/* Shorten Button */}
          <FuturisticButton
            type="submit"
            variant="primary"
            loading={state === 'loading'}
            disabled={state === 'loading'}
            className="w-full btn-lg"
            aria-label={state === 'loading' ? 'Shortening URL...' : 'Shorten URL'}
          >
            {state === 'loading' ? 'Shortening...' : 'Shorten'}
          </FuturisticButton>
        </form>
      )}

      {/* Toast Notification */}
      {toastMessage && (
        <div
          className="toast toast-top toast-center"
          role="alert"
          aria-live="polite"
          data-testid="copy-toast"
        >
          <div className={`alert ${toastType === 'success' ? 'alert-success' : 'alert-error'}`}>
            {toastType === 'success' ? (
              <svg
                xmlns="http://www.w3.org/2000/svg"
                className="stroke-current shrink-0 h-5 w-5"
                fill="none"
                viewBox="0 0 24 24"
                aria-hidden="true"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth="2"
                  d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z"
                />
              </svg>
            ) : (
              <svg
                xmlns="http://www.w3.org/2000/svg"
                className="stroke-current shrink-0 h-5 w-5"
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
            )}
            <span data-testid="toast-message">{toastMessage}</span>
          </div>
        </div>
      )}
    </GlassMorphismCard>
  )
}

export default GuestShortener
