/**
 * Inline URL Shortener Component
 * Owner: Scenario 2 - Inline URL Shortening
 *
 * Provides anonymous URL shortening functionality:
 * - URL input field with placeholder
 * - Shorten button
 * - Result display with copy button
 * - Registration prompt after success
 * - Loading states and error handling
 *
 * Requirements: REQ-2, REQ-7
 */

import { useState, useCallback, FormEvent, KeyboardEvent } from 'react'
import { useAnonymousShorten, validateUrl } from '../../hooks/useAnonymousShorten'
import type { InlineShortenerProps } from '../../types/homepage'

export function InlineShortener({ onShortenSuccess, onRegisterClick }: InlineShortenerProps) {
  const [inputUrl, setInputUrl] = useState('')
  const [validationError, setValidationError] = useState<string | null>(null)
  const [copied, setCopied] = useState(false)

  const { shortenUrl, isLoading, error: apiError, result, clearResult } = useAnonymousShorten()

  // Combined error from validation or API
  const displayError = validationError || apiError

  const handleInputChange = useCallback((value: string) => {
    setInputUrl(value)
    // Clear validation error when user types
    if (validationError) {
      setValidationError(null)
    }
  }, [validationError])

  const handleSubmit = useCallback(async (e?: FormEvent) => {
    e?.preventDefault()

    // Reset states
    setCopied(false)
    setValidationError(null)

    // Validate before submission
    const validation = validateUrl(inputUrl)
    if (!validation.isValid) {
      setValidationError(validation.error ?? 'Please enter a valid URL')
      return
    }

    // Call the API
    const shortenedResult = await shortenUrl(inputUrl)

    if (shortenedResult && onShortenSuccess) {
      onShortenSuccess(shortenedResult)
    }
  }, [inputUrl, shortenUrl, onShortenSuccess])

  const handleKeyDown = useCallback((e: KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') {
      e.preventDefault()
      handleSubmit()
    }
  }, [handleSubmit])

  const handleCopy = useCallback(async () => {
    if (result?.shortUrl) {
      try {
        await navigator.clipboard.writeText(result.shortUrl)
        setCopied(true)
        // Reset copied state after 2 seconds
        setTimeout(() => setCopied(false), 2000)
      } catch {
        // Fallback for browsers without clipboard API
        console.error('Failed to copy to clipboard')
      }
    }
  }, [result])

  const handleReset = useCallback(() => {
    setInputUrl('')
    setCopied(false)
    setValidationError(null)
    clearResult()
  }, [clearResult])

  return (
    <section
      data-testid="inline-shortener"
      className="w-full max-w-2xl mx-auto px-4 py-8"
    >
      <form onSubmit={handleSubmit} noValidate>
        <div className="flex flex-col sm:flex-row gap-3">
          <div className="flex-1">
            <label htmlFor="url-input" className="sr-only">
              Enter URL to shorten
            </label>
            <input
              id="url-input"
              type="url"
              data-testid="url-input"
              value={inputUrl}
              onChange={(e) => handleInputChange(e.target.value)}
              onKeyDown={handleKeyDown}
              placeholder="Paste your long URL here..."
              className={`input input-bordered w-full ${displayError ? 'input-error' : ''}`}
              aria-invalid={!!displayError}
              aria-describedby={displayError ? 'url-error' : undefined}
              disabled={isLoading}
            />
          </div>

          <button
            type="submit"
            data-testid="shorten-button"
            className="btn btn-primary"
            disabled={isLoading}
          >
            {isLoading ? (
              <>
                <span className="loading loading-spinner loading-sm" />
                Shortening...
              </>
            ) : (
              'Shorten'
            )}
          </button>
        </div>

        {/* Error message - displayed inline */}
        {displayError && (
          <div
            id="url-error"
            data-testid="error-message"
            className="text-error text-sm mt-2"
            role="alert"
          >
            {displayError}
          </div>
        )}
      </form>

      {/* Result display */}
      {result && (
        <div
          data-testid="result-section"
          className="mt-6 p-4 bg-base-200 rounded-lg"
        >
          <div className="flex flex-col sm:flex-row items-start sm:items-center gap-3">
            <div className="flex-1 min-w-0">
              <p className="text-sm text-base-content/60 mb-1">Your shortened URL:</p>
              <p
                data-testid="short-url"
                className="font-mono text-primary truncate"
              >
                {result.shortUrl}
              </p>
            </div>

            <div className="flex gap-2">
              <button
                type="button"
                data-testid="copy-button"
                onClick={handleCopy}
                className="btn btn-sm btn-outline"
              >
                {copied ? 'Copied!' : 'Copy'}
              </button>

              <button
                type="button"
                data-testid="reset-button"
                onClick={handleReset}
                className="btn btn-sm btn-ghost"
              >
                Shorten Another
              </button>
            </div>
          </div>

          {/* Registration prompt */}
          <div
            data-testid="register-prompt"
            className="mt-4 pt-4 border-t border-base-300"
          >
            <p className="text-sm text-base-content/70 mb-2">
              Want to track clicks and analytics?
            </p>
            <button
              type="button"
              data-testid="register-cta"
              onClick={onRegisterClick}
              className="btn btn-sm btn-secondary"
            >
              Create Account for Analytics
            </button>
          </div>
        </div>
      )}
    </section>
  )
}

export default InlineShortener
