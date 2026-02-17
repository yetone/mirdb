/**
 * Interactive URL shortening demo section.
 * Owner: Scenario 5 - Interactive URL Shortening Demo
 *
 * Allows visitors to test URL shortening without registration:
 * - URL input field with validation
 * - Shorten button using FuturisticButton
 * - Result display with copy-to-clipboard
 * - Registration prompt after success
 */

import React, { useState, useCallback } from 'react'
import { FuturisticButton } from '../common'
import { GlassMorphismCard } from '../common'
import { isValidUrl, getUrlValidationError, sanitizeUrl } from '../../utils/validation'
import { copyToClipboard } from '../../utils/clipboard'
import { shortenUrl } from '../../api'

interface UrlDemoSectionProps {
  onRegisterPrompt?: () => void
}

export function UrlDemoSection({ onRegisterPrompt }: UrlDemoSectionProps) {
  const [url, setUrl] = useState('')
  const [shortUrl, setShortUrl] = useState('')
  const [isLoading, setIsLoading] = useState(false)
  const [error, setError] = useState('')
  const [copied, setCopied] = useState(false)
  const [showRegistrationPrompt, setShowRegistrationPrompt] = useState(false)

  const handleUrlChange = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    setUrl(e.target.value)
    // Clear error when user starts typing
    if (error) {
      setError('')
    }
  }, [error])

  const handleShorten = useCallback(async () => {
    // Reset states
    setError('')
    setCopied(false)
    setShortUrl('')
    setShowRegistrationPrompt(false)

    // Sanitize input
    const sanitizedUrl = sanitizeUrl(url)

    // Validate URL
    const validationError = getUrlValidationError(sanitizedUrl)
    if (validationError) {
      setError(validationError)
      return
    }

    // Additional validation for valid URL format
    if (!isValidUrl(sanitizedUrl)) {
      setError('Please enter a valid URL starting with http:// or https://')
      return
    }

    setIsLoading(true)

    try {
      // Call the API to shorten the URL
      const result = await shortenUrl(sanitizedUrl)
      setShortUrl(result.short_url)
      setShowRegistrationPrompt(true)
    } catch (err) {
      // Handle API errors gracefully
      console.error('Error shortening URL:', err)
      setError('Unable to shorten URL. Please try again.')
    } finally {
      setIsLoading(false)
    }
  }, [url])

  const handleCopy = useCallback(async () => {
    if (!shortUrl) return

    const success = await copyToClipboard(shortUrl)
    if (success) {
      setCopied(true)
      // Reset copied state after 2 seconds
      setTimeout(() => setCopied(false), 2000)
    }
  }, [shortUrl])

  const handleRegisterClick = useCallback(() => {
    if (onRegisterPrompt) {
      onRegisterPrompt()
    }
  }, [onRegisterPrompt])

  return (
    <section
      className="py-20"
      data-testid="url-demo-section"
      aria-labelledby="url-demo-heading"
    >
      <div className="container mx-auto px-4">
        <h2
          id="url-demo-heading"
          data-testid="url-demo-heading"
          className="text-3xl font-bold text-center mb-8"
        >
          Try It Now
        </h2>
        <p className="text-center text-base-content mb-8 max-w-xl mx-auto">
          Paste a long URL below and see how fast we can shorten it. No sign-up required!
        </p>
        <GlassMorphismCard className="max-w-2xl mx-auto">
          <div className="flex flex-col gap-4">
            <label htmlFor="url-input" className="sr-only">
              URL to shorten
            </label>
            <input
              id="url-input"
              type="url"
              placeholder="Enter your long URL here (e.g., https://example.com/very/long/path)"
              className={`input input-bordered w-full ${error ? 'input-error' : ''}`}
              value={url}
              onChange={handleUrlChange}
              aria-label="URL to shorten"
              aria-invalid={!!error}
              aria-describedby={error ? 'url-error' : undefined}
              data-testid="url-input"
            />
            {error && (
              <p
                id="url-error"
                className="text-error text-sm"
                role="alert"
                data-testid="url-error"
              >
                {error}
              </p>
            )}
            <FuturisticButton
              onClick={handleShorten}
              loading={isLoading}
              disabled={isLoading}
              data-testid="shorten-button"
              aria-label="Shorten URL"
            >
              Shorten URL
            </FuturisticButton>
            {shortUrl && (
              <div
                className="mt-4 p-4 bg-base-200 rounded-lg"
                data-testid="result-container"
              >
                <p className="text-sm text-base-content mb-2">Your shortened URL:</p>
                <p className="font-mono text-lg" data-testid="short-url">
                  {shortUrl}
                </p>
                <button
                  className={`btn btn-sm mt-2 ${copied ? 'btn-success' : 'btn-ghost'}`}
                  onClick={handleCopy}
                  data-testid="copy-button"
                  aria-label={copied ? 'Copied to clipboard' : 'Copy shortened URL to clipboard'}
                >
                  {copied ? (
                    <>
                      <svg
                        xmlns="http://www.w3.org/2000/svg"
                        className="h-4 w-4 mr-1"
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
                      Copied!
                    </>
                  ) : (
                    <>
                      <svg
                        xmlns="http://www.w3.org/2000/svg"
                        className="h-4 w-4 mr-1"
                        viewBox="0 0 20 20"
                        fill="currentColor"
                        aria-hidden="true"
                      >
                        <path d="M8 3a1 1 0 011-1h2a1 1 0 110 2H9a1 1 0 01-1-1z" />
                        <path d="M6 3a2 2 0 00-2 2v11a2 2 0 002 2h8a2 2 0 002-2V5a2 2 0 00-2-2 3 3 0 01-3 3H9a3 3 0 01-3-3z" />
                      </svg>
                      Copy to Clipboard
                    </>
                  )}
                </button>
                {showRegistrationPrompt && (
                  <p
                    className="text-sm mt-4 text-base-content"
                    data-testid="registration-prompt"
                  >
                    <button
                      onClick={handleRegisterClick}
                      className="link link-primary"
                      data-testid="register-link"
                    >
                      Create an account
                    </button>{' '}
                    to get full analytics and manage all your links.
                  </p>
                )}
              </div>
            )}
          </div>
        </GlassMorphismCard>
      </div>
    </section>
  )
}
