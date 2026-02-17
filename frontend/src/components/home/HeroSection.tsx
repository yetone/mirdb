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
import { motion } from 'framer-motion'
import { FuturisticButton } from '../common'

export interface HeroSectionProps {
  onRegisterClick?: () => void
  onLoginClick?: () => void
}

export function HeroSection({ onRegisterClick, onLoginClick }: HeroSectionProps) {
  return (
    <section
      className="hero min-h-[70vh] bg-base-100"
      aria-labelledby="hero-headline"
      data-testid="hero-section"
    >
      <div className="hero-content text-center">
        <motion.div
          className="max-w-2xl"
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.5 }}
        >
          <h1
            id="hero-headline"
            className="text-4xl md:text-5xl lg:text-6xl font-bold leading-tight"
            data-testid="hero-headline"
          >
            Shorten Links, Amplify Your Reach
          </h1>
          <p
            className="py-6 text-lg md:text-xl text-base-content/80"
            data-testid="hero-subheadline"
          >
            Transform long URLs into short, memorable links. Track clicks, analyze performance,
            and share with confidence. Start free today.
          </p>
          <div className="flex flex-col sm:flex-row gap-4 justify-center items-center">
            <FuturisticButton
              variant="primary"
              onClick={onRegisterClick}
              className="btn-lg px-8"
              aria-label="Get started with URL shortening"
              data-testid="hero-primary-cta"
            >
              Get Started Free
            </FuturisticButton>
            <button
              onClick={onLoginClick}
              className="btn btn-ghost btn-lg"
              data-testid="hero-secondary-cta"
            >
              Already have an account? <span className="text-primary ml-1">Log in</span>
            </button>
          </div>
        </motion.div>
      </div>
    </section>
  )
}
