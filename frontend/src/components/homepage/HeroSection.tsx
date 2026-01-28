/**
 * Hero Section Component.
 * Owner: Scenario 1 - Hero Section Display
 *
 * Displays the main hero area with:
 * - Compelling headline communicating core value
 * - Supporting subheadline explaining the service
 * - Primary CTA button ("Get Started" / "Go to Dashboard")
 * - Secondary CTA button ("Log In" for guests)
 *
 * Props:
 * - isAuthenticated: boolean - Shows different CTAs based on auth state
 * - onGetStarted: () => void - Callback for primary CTA
 * - onLogin: () => void - Callback for login CTA
 *
 * Requirements:
 * - REQ-1: Display hero section with compelling headline
 * - REQ-2: Provide prominent CTA buttons
 */

import React from 'react'
import type { HeroProps } from '@/types/homepage'
import { FuturisticButton } from '@/components/FuturisticButton'

export function HeroSection({ isAuthenticated, onGetStarted, onLogin }: HeroProps) {
  return (
    <section className="hero min-h-[60vh] bg-base-200" aria-labelledby="hero-title">
      <div className="hero-content text-center">
        <div className="max-w-2xl">
          <h1 id="hero-title" className="text-5xl font-bold mb-6">
            Shorten URLs, Track Performance
          </h1>
          <p className="text-xl mb-8 text-base-content/70">
            Create short, memorable links and get detailed analytics on every click.
            Know your audience with geographic insights and referrer tracking.
          </p>
          <div className="flex flex-wrap gap-4 justify-center">
            {isAuthenticated ? (
              <FuturisticButton
                variant="primary"
                size="lg"
                onClick={onGetStarted}
                data-testid="hero-dashboard-btn"
              >
                Go to Dashboard
              </FuturisticButton>
            ) : (
              <>
                <FuturisticButton
                  variant="primary"
                  size="lg"
                  onClick={onGetStarted}
                  data-testid="hero-get-started-btn"
                >
                  Get Started
                </FuturisticButton>
                <FuturisticButton
                  variant="secondary"
                  size="lg"
                  onClick={onLogin}
                  data-testid="hero-login-btn"
                >
                  Log In
                </FuturisticButton>
              </>
            )}
          </div>
        </div>
      </div>
    </section>
  )
}
