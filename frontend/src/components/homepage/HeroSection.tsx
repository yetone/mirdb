/**
 * Hero Section Component
 * Owner: Scenario 1 - Hero Section Display and Value Proposition
 *
 * This component renders the main hero section at the top of the homepage.
 * It includes the headline, subheadline, and primary CTA buttons.
 */

import { motion } from 'framer-motion'
import { FuturisticButton } from '../FuturisticButton'

interface HeroSectionProps {
  onGetStarted?: () => void
  onSignIn?: () => void
}

export function HeroSection({ onGetStarted, onSignIn }: HeroSectionProps) {
  return (
    <section
      id="hero"
      data-testid="hero-section"
      className="min-h-[calc(100vh-4rem)] flex items-center justify-center px-4 py-12"
      aria-labelledby="hero-heading"
    >
      <div className="container mx-auto max-w-4xl text-center">
        <motion.h1
          id="hero-heading"
          data-testid="hero-headline"
          initial={{ opacity: 0, y: 30 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6 }}
          className="text-4xl md:text-5xl lg:text-6xl font-bold mb-6 bg-gradient-to-r from-primary via-secondary to-accent bg-clip-text text-transparent"
        >
          Shorten. Share. Track.
        </motion.h1>

        <motion.p
          data-testid="hero-subheadline"
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6, delay: 0.2 }}
          className="text-lg md:text-xl text-base-content/80 mb-8 max-w-2xl mx-auto"
        >
          Transform your long URLs into memorable short links. Track clicks, analyze performance,
          and manage all your links in one powerful dashboard.
        </motion.p>

        <motion.div
          data-testid="hero-cta-container"
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6, delay: 0.4 }}
          className="flex flex-col sm:flex-row gap-4 justify-center items-center"
        >
          <FuturisticButton
            variant="primary"
            size="lg"
            onClick={onGetStarted}
            data-testid="get-started-button"
            aria-label="Get started with URL shortening for free"
          >
            Get Started Free
          </FuturisticButton>

          <FuturisticButton
            variant="outline"
            size="lg"
            onClick={onSignIn}
            data-testid="sign-in-button"
            aria-label="Sign in to your account"
          >
            Sign In
          </FuturisticButton>
        </motion.div>

        <motion.div
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          transition={{ duration: 0.6, delay: 0.6 }}
          className="mt-12 text-sm text-base-content/60"
        >
          <p>No credit card required • Free forever plan available</p>
        </motion.div>
      </div>
    </section>
  )
}
