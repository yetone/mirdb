/**
 * Hero Section Component
 * Owner: Scenario 2 - Hero Section Display
 *
 * Displays the hero area with product name, tagline, and URL shortener form.
 * Should integrate with UrlShortenerForm component.
 *
 * Expected exports:
 * - HeroSection: React.FC - Hero section with heading, tagline, and URL input
 *
 * Props:
 * - onUrlShortened?: (shortUrl: string, shareToken: string) => void
 *
 * Content Requirements:
 * - Large heading with product name
 * - Tagline describing URL shortening value proposition
 * - URL shortener form integration
 */

import { useState } from 'react'
import { HeroSectionProps } from '../../types/home'

export function HeroSection({ onUrlShortened }: HeroSectionProps) {
  const [url, setUrl] = useState('')

  const handleShorten = () => {
    // This will be implemented by Scenario 3 - Anonymous URL Shortening
    // For now, just a placeholder that calls the callback if provided
    if (onUrlShortened && url) {
      onUrlShortened(`https://short.link/${url.substring(0, 6)}`, 'share-token')
    }
  }

  return (
    <section
      className="hero min-h-[60vh] flex flex-col items-center justify-center px-4 py-16"
      aria-label="Hero section"
    >
      <div className="hero-content text-center max-w-3xl">
        <div className="space-y-6">
          {/* Product Name Heading */}
          <h1 className="text-5xl md:text-6xl font-bold bg-gradient-to-r from-primary to-secondary bg-clip-text text-transparent">
            URL Shortener
          </h1>

          {/* Tagline / Value Proposition */}
          <p className="text-xl md:text-2xl text-base-content/80">
            Transform your long URLs into short, shareable links in seconds.
            Track clicks and analyze performance with powerful analytics.
          </p>

          {/* URL Input Form */}
          <div className="flex flex-col sm:flex-row gap-3 w-full max-w-2xl mx-auto mt-8">
            <input
              type="url"
              value={url}
              onChange={(e) => setUrl(e.target.value)}
              placeholder="Paste your long URL here..."
              className="input input-bordered input-lg flex-1 w-full"
              aria-label="URL to shorten"
            />
            <button
              type="button"
              onClick={handleShorten}
              className="btn btn-primary btn-lg"
              aria-label="Shorten URL"
            >
              Shorten
            </button>
          </div>
        </div>
      </div>
    </section>
  )
}

export default HeroSection
