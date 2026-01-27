/**
 * Hero Section Component
 * Owner: Scenario 1 - Hero Section Display
 *
 * Displays the main hero area with:
 * - Product tagline and value proposition headline
 * - Subheadline about analytics capabilities
 * - Primary CTA: "Get Started" (links to Register)
 * - Secondary CTA: "Learn More" (smooth scroll to features)
 * - BackgroundEffect integration
 */

import { Link } from 'react-router-dom'
import { motion } from 'framer-motion'
import { FuturisticButton } from '../FuturisticButton'
import type { HeroSectionProps } from '../../types/homepage'

export function HeroSection({ onLearnMoreClick }: HeroSectionProps) {
  return (
    <section
      aria-label="Hero"
      className="min-h-screen flex items-center justify-center px-4 sm:px-6 lg:px-8"
    >
      <div className="max-w-4xl mx-auto text-center">
        <motion.h1
          className="text-4xl sm:text-5xl lg:text-6xl font-bold mb-6"
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6 }}
        >
          Shorten Your Links,{' '}
          <span className="text-primary">Amplify Your Reach</span>
        </motion.h1>

        <motion.p
          className="text-lg sm:text-xl text-base-content mb-8 max-w-2xl mx-auto"
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6, delay: 0.2 }}
        >
          Transform long URLs into powerful short links. Track every click with
          detailed analytics and gain insights into your audience's behavior.
        </motion.p>

        <motion.div
          className="flex flex-col sm:flex-row gap-4 justify-center items-center"
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6, delay: 0.4 }}
        >
          <Link to="/register" className="w-full sm:w-auto">
            <FuturisticButton variant="primary" className="w-full sm:w-auto px-8 py-3 text-lg">
              Get Started
            </FuturisticButton>
          </Link>

          <FuturisticButton
            variant="ghost"
            onClick={onLearnMoreClick}
            className="w-full sm:w-auto px-8 py-3 text-lg"
          >
            Learn More
          </FuturisticButton>
        </motion.div>
      </div>
    </section>
  )
}

export default HeroSection
