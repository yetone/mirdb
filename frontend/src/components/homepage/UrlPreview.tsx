/**
 * URL Preview/Demo Component
 * Owner: Scenario 9 - URL Shortening Preview/Demo
 *
 * Visual demonstration of how URL shortening works.
 *
 * Expected exports:
 * - UrlPreview: React.FC - The URL preview component
 *
 * Requirements:
 * - Shows example of long URL -> short URL transformation
 * - May be static visual or interactive demo
 * - Helps users understand the service value
 * - Uses existing styling components
 */

import React from 'react'
import GlassMorphismCard from '../GlassMorphismCard'
import { ArrowRight, Link2, Zap } from 'lucide-react'

export const UrlPreview: React.FC = () => {
  const longUrl = 'https://example.com/articles/2024/03/how-to-build-amazing-web-applications-with-react-typescript-and-tailwind'
  const shortUrl = 'short.ly/abc123'

  return (
    <section
      className="url-preview py-16 px-4 max-w-4xl mx-auto"
      data-testid="url-preview"
      aria-label="URL shortening preview"
    >
      <GlassMorphismCard className="p-8">
        {/* Section Header */}
        <div className="text-center mb-8">
          <div className="flex items-center justify-center gap-2 mb-2">
            <Zap className="w-6 h-6 text-primary" aria-hidden="true" />
            <h2 className="text-2xl font-bold" data-testid="preview-title">
              See How It Works
            </h2>
          </div>
          <p className="text-base-content/70" data-testid="preview-description">
            Transform long, unwieldy URLs into short, memorable links in seconds
          </p>
        </div>

        {/* URL Transformation Demo */}
        <div
          className="url-transformation flex flex-col lg:flex-row items-center gap-4 lg:gap-6"
          data-testid="url-transformation"
        >
          {/* Long URL Box */}
          <div className="flex-1 w-full" data-testid="long-url-container">
            <label className="text-sm font-medium text-base-content/60 mb-2 block">
              Your Long URL
            </label>
            <div
              className="long-url bg-base-200/50 rounded-lg p-4 border border-base-300 font-mono text-sm break-all"
              data-testid="long-url"
            >
              <Link2 className="w-4 h-4 inline-block mr-2 text-error" aria-hidden="true" />
              <span className="text-base-content/80">{longUrl}</span>
            </div>
          </div>

          {/* Arrow Indicator */}
          <div
            className="flex-shrink-0 flex items-center justify-center"
            data-testid="transformation-arrow"
            aria-hidden="true"
          >
            <div className="bg-primary/20 rounded-full p-3">
              <ArrowRight className="w-6 h-6 text-primary transform lg:rotate-0 rotate-90" />
            </div>
          </div>

          {/* Short URL Box */}
          <div className="flex-1 w-full" data-testid="short-url-container">
            <label className="text-sm font-medium text-base-content/60 mb-2 block">
              Your Short Link
            </label>
            <div
              className="short-url bg-primary/10 rounded-lg p-4 border-2 border-primary font-mono text-lg font-semibold"
              data-testid="short-url"
            >
              <Link2 className="w-4 h-4 inline-block mr-2 text-primary" aria-hidden="true" />
              <span className="text-primary">{shortUrl}</span>
            </div>
          </div>
        </div>

        {/* Benefits Callout */}
        <div
          className="mt-8 text-center"
          data-testid="preview-benefits"
        >
          <p className="text-base-content/60 text-sm">
            <span className="font-semibold text-primary">67 characters</span> reduced to just{' '}
            <span className="font-semibold text-primary">16 characters</span> — easier to share, track, and remember!
          </p>
        </div>
      </GlassMorphismCard>
    </section>
  )
}

export default UrlPreview
