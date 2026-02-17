/**
 * Hero section component for homepage.
 * Owner: Scenario 1 - Hero Section Display
 *
 * Displays:
 * - Headline with value proposition
 * - Sub-headline describing benefits
 * - Primary CTA (Get Started/Register)
 * - Secondary CTA (Login link)
 */

import React from 'react'
import { FuturisticButton } from '../common'

interface HeroSectionProps {
  onRegisterClick?: () => void
  onLoginClick?: () => void
}

export function HeroSection({ onRegisterClick, onLoginClick }: HeroSectionProps) {
  return (
    <section className="hero min-h-[60vh] py-20" data-testid="hero-section">
      <div className="hero-content text-center">
        <div className="max-w-2xl">
          <h1 className="text-5xl font-bold mb-6">
            Shorten URLs, Track Clicks, Grow Your Reach
          </h1>
          <p className="text-xl mb-8 text-base-content/70">
            Transform long, unwieldy URLs into short, memorable links.
            Get detailed analytics and manage all your links in one place.
          </p>
          <div className="flex gap-4 justify-center">
            <FuturisticButton
              variant="primary"
              onClick={onRegisterClick}
              data-testid="hero-primary-cta"
              aria-label="Get started with URL shortening - create a free account"
            >
              Get Started Free
            </FuturisticButton>
            <FuturisticButton
              variant="outline"
              onClick={onLoginClick}
              data-testid="hero-login-cta"
              aria-label="Log in to your URL shortening account"
            >
              Log In
            </FuturisticButton>
          </div>
        </div>
      </div>
    </section>
  )
}
