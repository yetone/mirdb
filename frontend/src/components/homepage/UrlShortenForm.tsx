/**
 * URL Shortening Form Component
 * Owner: Scenario 2 - URL Shortening Form Functionality
 *
 * Form for shortening URLs directly from homepage.
 * Does NOT require authentication.
 *
 * Features:
 * - URL input with validation
 * - Submit on button click or Enter key
 * - Display shortened URL result
 * - Copy to clipboard functionality
 * - Inline error messages
 *
 * API Integration:
 * - POST /api/urls/ (unauthenticated)
 */
import { useState, FormEvent, KeyboardEvent } from 'react'
import { shortenUrl } from '../../api'

export interface ShortenedUrl {
  shortCode: string
  originalUrl: string
  shortUrl: string
}

export interface UrlShortenFormProps {
  onSuccess?: (result: ShortenedUrl) => void
}

const URL_PATTERN = /^https?:\/\/.+/i

function isValidUrl(url: string): boolean {
  if (!url.trim()) return false
  return URL_PATTERN.test(url)
}

export function UrlShortenForm({ onSuccess }: UrlShortenFormProps) {
  const [url, setUrl] = useState('')
  const [error, setError] = useState<string | null>(null)
  const [isLoading, setIsLoading] = useState(false)
  const [result, setResult] = useState<ShortenedUrl | null>(null)
  const [copySuccess, setCopySuccess] = useState(false)

  const validateUrl = (value: string): string | null => {
    if (!value.trim()) {
      return 'Please enter a URL'
    }
    if (!isValidUrl(value)) {
      return 'Please enter a valid URL'
    }
    return null
  }

  const handleSubmit = async (e?: FormEvent) => {
    e?.preventDefault()

    const validationError = validateUrl(url)
    if (validationError) {
      setError(validationError)
      return
    }

    setError(null)
    setIsLoading(true)
    setResult(null)

    try {
      const response = await shortenUrl(url)
      const shortUrl = `${window.location.origin}/r/${response.short_code}`
      const shortened: ShortenedUrl = {
        shortCode: response.short_code,
        originalUrl: response.original_url,
        shortUrl,
      }
      setResult(shortened)
      onSuccess?.(shortened)
    } catch {
      setError('Failed to shorten URL. Please try again.')
    } finally {
      setIsLoading(false)
    }
  }

  const handleKeyDown = (e: KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') {
      handleSubmit()
    }
  }

  const handleCopy = async () => {
    if (!result) return

    try {
      await navigator.clipboard.writeText(result.shortUrl)
      setCopySuccess(true)
      setTimeout(() => setCopySuccess(false), 2000)
    } catch {
      setError('Failed to copy to clipboard')
    }
  }

  const handleReset = () => {
    setUrl('')
    setResult(null)
    setError(null)
    setCopySuccess(false)
  }

  return (
    <div className="w-full">
      <form onSubmit={handleSubmit} className="w-full" role="form" aria-label="URL shortening form">
        <div className="join w-full">
          <input
            type="text"
            placeholder="Enter your long URL here..."
            className={`input input-bordered join-item flex-1 ${error ? 'input-error' : ''}`}
            value={url}
            onChange={(e) => {
              setUrl(e.target.value)
              if (error) setError(null)
            }}
            onKeyDown={handleKeyDown}
            disabled={isLoading}
            aria-label="URL input"
            aria-invalid={!!error}
            aria-describedby={error ? 'url-error' : undefined}
            data-testid="url-input"
          />
          <button
            type="submit"
            className="btn btn-primary join-item"
            disabled={isLoading}
            aria-label="Shorten URL"
            data-testid="shorten-url-button"
          >
            {isLoading ? (
              <span className="loading loading-spinner loading-sm" aria-label="Loading"></span>
            ) : (
              'Shorten URL'
            )}
          </button>
        </div>

        {error && (
          <div id="url-error" className="text-error text-sm mt-2" role="alert">
            {error}
          </div>
        )}
      </form>

      {result && (
        <div className="mt-4 p-4 bg-base-200 rounded-lg" role="region" aria-label="Shortened URL result">
          <p className="text-sm text-base-content/80 mb-2">Your shortened URL:</p>
          <div className="flex items-center gap-2">
            <input
              type="text"
              readOnly
              value={result.shortUrl}
              className="input input-bordered flex-1 font-mono text-sm"
              aria-label="Shortened URL"
            />
            <button
              type="button"
              className={`btn ${copySuccess ? 'btn-success' : 'btn-secondary'}`}
              onClick={handleCopy}
              aria-label={copySuccess ? 'Copied!' : 'Copy to clipboard'}
            >
              {copySuccess ? (
                <>
                  <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5" viewBox="0 0 20 20" fill="currentColor">
                    <path fillRule="evenodd" d="M16.707 5.293a1 1 0 010 1.414l-8 8a1 1 0 01-1.414 0l-4-4a1 1 0 011.414-1.414L8 12.586l7.293-7.293a1 1 0 011.414 0z" clipRule="evenodd" />
                  </svg>
                  Copied!
                </>
              ) : (
                <>
                  <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5" viewBox="0 0 20 20" fill="currentColor">
                    <path d="M8 3a1 1 0 011-1h2a1 1 0 110 2H9a1 1 0 01-1-1z" />
                    <path d="M6 3a2 2 0 00-2 2v11a2 2 0 002 2h8a2 2 0 002-2V5a2 2 0 00-2-2 3 3 0 01-3 3H9a3 3 0 01-3-3z" />
                  </svg>
                  Copy
                </>
              )}
            </button>
          </div>
          <button
            type="button"
            className="btn btn-ghost btn-sm mt-3"
            onClick={handleReset}
            aria-label="Shorten another URL"
          >
            Shorten another URL
          </button>
        </div>
      )}
    </div>
  )
}

export default UrlShortenForm
