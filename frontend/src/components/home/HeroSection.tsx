/**
 * Hero Section Component
 * Owner: Scenario 1 - Hero Section Display
 *
 * Purpose: Display the main hero section with value proposition,
 * tagline, and primary call-to-action.
 */

import { Link } from 'react-router-dom'
import { motion } from 'framer-motion'
import { BackgroundEffect } from '../BackgroundEffect'
import { FuturisticButton } from '../FuturisticButton'

export function HeroSection() {
  return (
    <section
      className="relative min-h-[80vh] flex items-center justify-center px-4"
      data-testid="hero-section"
    >
      <BackgroundEffect />

      <div className="max-w-4xl mx-auto text-center z-10">
        <motion.h1
          className="text-4xl md:text-5xl lg:text-6xl font-bold mb-6"
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6 }}
          data-testid="hero-headline"
        >
          Shorten URLs.{' '}
          <span className="text-primary">Track Performance.</span>{' '}
          <span className="text-secondary">Grow Smarter.</span>
        </motion.h1>

        <motion.p
          className="text-lg md:text-xl text-base-content/70 mb-8 max-w-2xl mx-auto"
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6, delay: 0.2 }}
          data-testid="hero-subheadline"
        >
          Transform long URLs into short, powerful links. Get detailed analytics,
          track clicks in real-time, and understand your audience better.
        </motion.p>

        <motion.div
          className="flex flex-col sm:flex-row gap-4 justify-center items-center"
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6, delay: 0.4 }}
        >
          <Link to="/register" data-testid="hero-cta-primary">
            <FuturisticButton variant="primary" size="lg">
              Get Started Free
            </FuturisticButton>
          </Link>

          <Link
            to="/login"
            className="text-base-content/70 hover:text-primary transition-colors"
            data-testid="hero-login-link"
          >
            Already have an account? <span className="underline">Log in</span>
          </Link>
        </motion.div>
      </div>
    </section>
  )
}
