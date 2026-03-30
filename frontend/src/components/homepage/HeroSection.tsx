/**
 * Hero section component for homepage.
 * Owner: Scenario 2 - Hero Section URL Input and Shortening CTA
 *
 * Renders the main hero area with:
 * - Headline and sub-headline
 * - URL input component
 * - Primary CTA (Shorten URL)
 * - Secondary CTAs (Sign Up, Learn More)
 */

import React, { useState } from 'react'
import { Link } from 'react-router-dom'
import { HeroSectionProps } from '@/types/homepage'
import UrlInput from './UrlInput'
import FuturisticButton from '@/components/FuturisticButton'

const HeroSection: React.FC<HeroSectionProps> = ({
  onUrlSubmit,
  isAuthenticated = false,
}) => {
  const [isLoading, setIsLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const handleUrlSubmit = async (url: string) => {
    setError(null)
    setIsLoading(true)

    try {
      if (onUrlSubmit) {
        await onUrlSubmit(url)
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to shorten URL')
    } finally {
      setIsLoading(false)
    }
  }

  return (
    <section
      className="py-20 px-4 text-center"
      data-testid="hero-section"
      aria-label="Hero section"
    >
      <div className="max-w-4xl mx-auto">
        <h1
          className="text-4xl md:text-6xl font-bold mb-6"
          data-testid="hero-headline"
        >
          Shorten URLs, Track Insights
        </h1>
        <p
          className="text-xl text-base-content/70 mb-8"
          data-testid="hero-subheadline"
        >
          Transform long URLs into short, memorable links. Track clicks,
          analyze performance, and grow your reach.
        </p>

        {/* URL Input Form */}
        <div className="mb-8">
          <UrlInput
            onSubmit={handleUrlSubmit}
            isLoading={isLoading}
            error={error}
          />
        </div>

        {/* Secondary CTAs */}
        <div className="flex justify-center gap-4 flex-wrap">
          {!isAuthenticated && (
            <Link to="/register">
              <FuturisticButton variant="secondary" size="lg">
                Get Started
              </FuturisticButton>
            </Link>
          )}
          <a href="#features">
            <FuturisticButton variant="ghost" size="lg">
              Learn More
            </FuturisticButton>
          </a>
        </div>
      </div>
    </section>
  )
}

export default HeroSection
