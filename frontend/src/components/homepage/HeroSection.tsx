/**
 * Hero Section Component
 * Owner: Scenario 1 - Hero Section and Value Proposition
 *
 * Displays the main hero section of the homepage with:
 * - Large headline with value proposition
 * - Supporting subheadline
 * - Primary CTA button (Get Started / Sign Up)
 * - Secondary CTA button (Learn More)
 * - Background visual/illustration
 *
 * Requirements: REQ-1, REQ-2, US-1
 */

import { useNavigate } from 'react-router-dom'
import { motion } from 'framer-motion'

export interface HeroSectionProps {
  onCtaClick?: () => void
  ctaText?: string
  secondaryCtaText?: string
  onSecondaryCtaClick?: () => void
}

export function HeroSection({
  onCtaClick,
  ctaText = 'Get Started',
  secondaryCtaText = 'Learn More',
  onSecondaryCtaClick,
}: HeroSectionProps) {
  const navigate = useNavigate()

  const handlePrimaryClick = () => {
    if (onCtaClick) {
      onCtaClick()
    } else {
      navigate('/register')
    }
  }

  const handleSecondaryClick = () => {
    if (onSecondaryCtaClick) {
      onSecondaryCtaClick()
    } else {
      const featuresSection = document.getElementById('features')
      if (featuresSection) {
        featuresSection.scrollIntoView({ behavior: 'smooth' })
      }
    }
  }

  return (
    <section
      className="hero min-h-[80vh] relative overflow-hidden"
      aria-labelledby="hero-heading"
      data-testid="hero-section"
    >
      {/* Background decoration */}
      <div className="absolute inset-0 bg-gradient-to-br from-primary/10 via-transparent to-secondary/10 pointer-events-none" />

      <div className="hero-content text-center py-16 px-4 relative z-10">
        <motion.div
          className="max-w-2xl"
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6 }}
        >
          <h1
            id="hero-heading"
            className="text-4xl md:text-5xl lg:text-6xl font-bold mb-6 bg-gradient-to-r from-primary to-secondary bg-clip-text text-transparent"
            data-testid="hero-headline"
          >
            Shorten URLs, Track Every Click
          </h1>

          <p
            className="text-lg md:text-xl mb-8 text-base-content/80"
            data-testid="hero-subheadline"
          >
            Create short, memorable links in seconds. Track clicks, analyze your audience,
            and share performance insights with powerful analytics.
          </p>

          <div className="flex flex-col sm:flex-row gap-4 justify-center">
            <motion.button
              className="btn btn-primary btn-lg"
              onClick={handlePrimaryClick}
              data-testid="hero-cta-primary"
              whileHover={{ scale: 1.05 }}
              whileTap={{ scale: 0.95 }}
            >
              {ctaText}
            </motion.button>

            <motion.button
              className="btn btn-outline btn-lg"
              onClick={handleSecondaryClick}
              data-testid="hero-cta-secondary"
              whileHover={{ scale: 1.05 }}
              whileTap={{ scale: 0.95 }}
            >
              {secondaryCtaText}
            </motion.button>
          </div>
        </motion.div>
      </div>
    </section>
  )
}

export default HeroSection
