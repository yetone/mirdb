/**
 * URL input component for homepage hero section.
 * Owner: Scenario 2 - Hero Section URL Input and Shortening CTA
 *
 * Features:
 * - URL input field with placeholder
 * - Auto-focus on mount
 * - Enter key submission
 * - Validation feedback
 * - Loading state during submission
 */

import React, { useRef, useEffect, useState, FormEvent, KeyboardEvent } from 'react'
import { UrlInputProps } from '@/types/homepage'
import FuturisticButton from '@/components/FuturisticButton'

const UrlInput: React.FC<UrlInputProps> = ({
  onSubmit,
  isLoading = false,
  error = null,
}) => {
  const [url, setUrl] = useState('')
  const inputRef = useRef<HTMLInputElement>(null)

  useEffect(() => {
    // Auto-focus input on mount
    if (inputRef.current) {
      inputRef.current.focus()
    }
  }, [])

  const handleSubmit = (e: FormEvent) => {
    e.preventDefault()
    if (url.trim() && !isLoading) {
      onSubmit(url.trim())
    }
  }

  const handleKeyDown = (e: KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter' && url.trim() && !isLoading) {
      e.preventDefault()
      onSubmit(url.trim())
    }
  }

  return (
    <form
      onSubmit={handleSubmit}
      className="w-full max-w-2xl mx-auto"
      data-testid="url-input-form"
    >
      <div className="flex flex-col sm:flex-row gap-3">
        <div className="flex-1 relative">
          <input
            ref={inputRef}
            type="url"
            value={url}
            onChange={(e) => setUrl(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="Enter your long URL here..."
            className={`input input-bordered w-full input-lg ${
              error ? 'input-error' : ''
            }`}
            disabled={isLoading}
            aria-label="URL to shorten"
            aria-describedby={error ? 'url-error' : undefined}
            data-testid="url-input"
          />
        </div>
        <FuturisticButton
          type="submit"
          variant="primary"
          size="lg"
          disabled={isLoading || !url.trim()}
          data-testid="shorten-button"
        >
          {isLoading ? (
            <span className="loading loading-spinner loading-sm" />
          ) : (
            'Shorten URL'
          )}
        </FuturisticButton>
      </div>
      {error && (
        <p
          id="url-error"
          className="text-error text-sm mt-2 text-left"
          role="alert"
          data-testid="url-error"
        >
          {error}
        </p>
      )}
    </form>
  )
}

export default UrlInput
