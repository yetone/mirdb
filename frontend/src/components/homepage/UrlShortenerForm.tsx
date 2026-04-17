/**
 * URL Shortener Form Component
 * Owner: Scenario 2 - Inline URL Shortening Demo
 *
 * Inline URL shortening form for unauthenticated users:
 * - URL input field with validation
 * - Shorten button with loading state
 * - Success state showing shortened URL
 * - Copy to clipboard functionality
 * - Error states for invalid URLs and API failures
 *
 * Related requirements: REQ-2, NFR-1
 */

import { useState, FormEvent, KeyboardEvent } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import { Link2, Copy, Check, Loader2, AlertCircle, RefreshCw } from 'lucide-react'
import { useUrlShortener } from '../../hooks/useUrlShortener'

export interface UrlShortenerFormProps {
  className?: string
  placeholder?: string
  onSuccess?: (shortUrl: string, originalUrl: string) => void
}

export function UrlShortenerForm({
  className = '',
  placeholder = 'Enter your long URL here...',
  onSuccess,
}: UrlShortenerFormProps) {
  const [inputUrl, setInputUrl] = useState('')
  const {
    isLoading,
    error,
    shortenedUrl,
    isCopied,
    shortenUrl,
    copyToClipboard,
    reset,
  } = useUrlShortener()

  const handleSubmit = async (e?: FormEvent) => {
    e?.preventDefault()

    if (isLoading || !inputUrl.trim()) {
      return
    }

    const result = await shortenUrl(inputUrl)
    if (result && onSuccess) {
      onSuccess(result.shortUrl, result.originalUrl)
    }
  }

  const handleKeyDown = (e: KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') {
      e.preventDefault()
      handleSubmit()
    }
  }

  const handleReset = () => {
    reset()
    setInputUrl('')
  }

  return (
    <div className={`w-full max-w-2xl mx-auto ${className}`} data-testid="url-shortener-form">
      <form onSubmit={handleSubmit} className="flex flex-col gap-4">
        {/* Input Row */}
        <div className="flex flex-col sm:flex-row gap-2">
          <div className="relative flex-1">
            <div className="absolute inset-y-0 left-0 flex items-center pl-3 pointer-events-none">
              <Link2 className="w-5 h-5 text-base-content/50" aria-hidden="true" />
            </div>
            <input
              type="text"
              value={inputUrl}
              onChange={(e) => setInputUrl(e.target.value)}
              onKeyDown={handleKeyDown}
              placeholder={placeholder}
              disabled={isLoading || !!shortenedUrl}
              className={`input input-bordered w-full pl-10 pr-4 py-3 text-base-content bg-base-200/50 placeholder:text-base-content/40 focus:outline-none focus:ring-2 focus:ring-primary focus:border-transparent ${
                error ? 'input-error border-error' : ''
              }`}
              data-testid="url-input"
              aria-label="URL to shorten"
              aria-invalid={!!error}
              aria-describedby={error ? 'url-error' : undefined}
            />
          </div>

          <button
            type="submit"
            disabled={isLoading || !inputUrl.trim() || !!shortenedUrl}
            className="btn btn-primary px-6 py-3 min-w-[140px]"
            data-testid="shorten-button"
            aria-label="Shorten URL"
          >
            {isLoading ? (
              <>
                <Loader2 className="w-5 h-5 animate-spin" aria-hidden="true" />
                <span className="ml-2">Shortening...</span>
              </>
            ) : (
              'Shorten URL'
            )}
          </button>
        </div>

        {/* Error Message */}
        <AnimatePresence>
          {error && (
            <motion.div
              initial={{ opacity: 0, y: -10 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -10 }}
              className="flex items-center gap-2 text-error"
              id="url-error"
              role="alert"
              data-testid="error-message"
            >
              <AlertCircle className="w-5 h-5 flex-shrink-0" aria-hidden="true" />
              <span className="text-sm">{error}</span>
            </motion.div>
          )}
        </AnimatePresence>

        {/* Success State */}
        <AnimatePresence>
          {shortenedUrl && (
            <motion.div
              initial={{ opacity: 0, y: -10, height: 0 }}
              animate={{ opacity: 1, y: 0, height: 'auto' }}
              exit={{ opacity: 0, y: -10, height: 0 }}
              className="flex flex-col sm:flex-row items-center gap-3 p-4 bg-success/10 border border-success/30 rounded-lg"
              data-testid="success-state"
              role="status"
              aria-live="polite"
            >
              <div className="flex-1 text-center sm:text-left">
                <p className="text-sm text-base-content/70 mb-1">Your shortened URL:</p>
                <a
                  href={shortenedUrl}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-lg font-semibold text-primary hover:underline break-all"
                  data-testid="shortened-url"
                >
                  {shortenedUrl}
                </a>
              </div>

              <div className="flex gap-2">
                <button
                  type="button"
                  onClick={copyToClipboard}
                  className={`btn btn-sm ${isCopied ? 'btn-success' : 'btn-outline btn-primary'}`}
                  data-testid="copy-button"
                  aria-label={isCopied ? 'Copied to clipboard' : 'Copy to clipboard'}
                >
                  {isCopied ? (
                    <>
                      <Check className="w-4 h-4" aria-hidden="true" />
                      <span>Copied!</span>
                    </>
                  ) : (
                    <>
                      <Copy className="w-4 h-4" aria-hidden="true" />
                      <span>Copy</span>
                    </>
                  )}
                </button>

                <button
                  type="button"
                  onClick={handleReset}
                  className="btn btn-sm btn-ghost"
                  data-testid="reset-button"
                  aria-label="Shorten another URL"
                >
                  <RefreshCw className="w-4 h-4" aria-hidden="true" />
                  <span className="hidden sm:inline ml-1">New URL</span>
                </button>
              </div>
            </motion.div>
          )}
        </AnimatePresence>
      </form>
    </div>
  )
}

export default UrlShortenerForm
