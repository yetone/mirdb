/**
 * Hero Section Component
 * Owner: Scenario 1 - Hero Section Value Proposition
 *
 * Full-width hero section with:
 * - Gradient background compatible with theme system
 * - Main headline: "Shorten Links. Track Success."
 * - Descriptive subheadline
 * - Primary CTA: "Get Started Free" linking to /register
 *
 * Related requirements: REQ-1, REQ-3, NFR-5
 */

import { Link } from 'react-router-dom'
import { motion } from 'framer-motion'
import { ArrowRight } from 'lucide-react'

export interface HeroSectionProps {
  className?: string
}

export function HeroSection({ className = '' }: HeroSectionProps) {
  return (
    <section
      data-testid="hero-section"
      className={`relative min-h-[80vh] flex items-center justify-center overflow-hidden ${className}`}
    >
      {/* Gradient Background - Theme Compatible */}
      <div
        className="absolute inset-0 bg-gradient-to-br from-primary/20 via-base-100 to-secondary/20"
        data-testid="hero-gradient"
      />

      {/* Decorative Elements */}
      <div className="absolute inset-0 overflow-hidden">
        <div className="absolute -top-40 -right-40 w-80 h-80 bg-primary/10 rounded-full blur-3xl" />
        <div className="absolute -bottom-40 -left-40 w-80 h-80 bg-secondary/10 rounded-full blur-3xl" />
      </div>

      {/* Content Container */}
      <div className="relative z-10 container mx-auto px-4 py-16 text-center">
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6 }}
          className="max-w-4xl mx-auto"
        >
          {/* Main Headline */}
          <h1
            className="text-4xl md:text-5xl lg:text-6xl font-bold text-base-content mb-6"
            data-testid="hero-headline"
          >
            Shorten Links. Track Success.
          </h1>

          {/* Subheadline */}
          <p
            className="text-lg md:text-xl lg:text-2xl text-base-content/70 mb-8 max-w-2xl mx-auto"
            data-testid="hero-subheadline"
          >
            Transform long URLs into powerful short links. Get detailed analytics,
            track clicks in real-time, and understand your audience better.
          </p>

          {/* CTA Button */}
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.6, delay: 0.2 }}
          >
            <Link
              to="/register"
              className="btn btn-primary btn-lg gap-2"
              data-testid="hero-cta"
            >
              Get Started Free
              <ArrowRight className="w-5 h-5" />
            </Link>
          </motion.div>

          {/* Trust Indicator */}
          <motion.p
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            transition={{ duration: 0.6, delay: 0.4 }}
            className="mt-6 text-sm text-base-content/50"
          >
            No credit card required. Start shortening links in seconds.
          </motion.p>
        </motion.div>
      </div>
    </section>
  )
}

export default HeroSection
