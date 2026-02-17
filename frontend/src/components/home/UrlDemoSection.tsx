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

import React, { useState } from 'react'
import { FuturisticButton } from '../common'
import { GlassMorphismCard } from '../common'

interface UrlDemoSectionProps {
  onRegisterPrompt?: () => void
}

export function UrlDemoSection({ onRegisterPrompt }: UrlDemoSectionProps) {
  const [url, setUrl] = useState('')
  const [shortUrl, setShortUrl] = useState('')
  const [isLoading, setIsLoading] = useState(false)
  const [error, setError] = useState('')

  const handleShorten = async () => {
    if (!url) {
      setError('Please enter a URL')
      return
    }

    setIsLoading(true)
    setError('')

    // Simulated demo - in production would call API
    setTimeout(() => {
      setShortUrl(`https://urlshort.io/abc123`)
      setIsLoading(false)
    }, 1000)
  }

  return (
    <section className="py-20">
      <div className="container mx-auto px-4">
        <h2 className="text-3xl font-bold text-center mb-8">Try It Now</h2>
        <GlassMorphismCard className="max-w-2xl mx-auto">
          <div className="flex flex-col gap-4">
            <input
              type="url"
              placeholder="Enter your long URL here..."
              className="input input-bordered w-full"
              value={url}
              onChange={(e) => setUrl(e.target.value)}
              aria-label="URL to shorten"
            />
            {error && <p className="text-error text-sm">{error}</p>}
            <FuturisticButton onClick={handleShorten} loading={isLoading}>
              Shorten URL
            </FuturisticButton>
            {shortUrl && (
              <div className="mt-4 p-4 bg-base-200 rounded-lg">
                <p className="text-sm text-base-content/70 mb-2">Your shortened URL:</p>
                <p className="font-mono text-lg">{shortUrl}</p>
                <button
                  className="btn btn-sm btn-ghost mt-2"
                  onClick={() => navigator.clipboard.writeText(shortUrl)}
                >
                  Copy to Clipboard
                </button>
                <p className="text-sm mt-4 text-base-content/60">
                  <button onClick={onRegisterPrompt} className="link link-primary">
                    Create an account
                  </button>{' '}
                  to get analytics and manage all your links.
                </p>
              </div>
            )}
          </div>
        </GlassMorphismCard>
      </div>
    </section>
  )
}
