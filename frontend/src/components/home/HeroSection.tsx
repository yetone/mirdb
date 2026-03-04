/**
 * Hero Section Component
 * Owner: Scenario 1 - Hero Section Rendering
 *
 * Displays the main value proposition headline, supporting subheading,
 * and primary call-to-action button.
 *
 * Requirements:
 * - Display headline within first viewport
 * - Include supporting subheading
 * - Primary CTA using FuturisticButton
 * - Fade-in animation on load (0.5-1s)
 */

import { motion } from 'framer-motion'
import { Link } from 'react-router-dom'
import { FuturisticButton } from '../FuturisticButton'
import type { HeroSectionProps } from '../../types/home'

const defaultHeadline = 'Shorten URLs, Track Clicks, Understand Your Audience'
const defaultSubheading =
  'Create memorable short links from long URLs and get detailed analytics on who\'s clicking and where they\'re coming from.'
const defaultCtaText = 'Get Started Free'
const defaultCtaLink = '/register'

export const HeroSection: React.FC<HeroSectionProps> = ({
  headline = defaultHeadline,
  subheading = defaultSubheading,
  ctaText = defaultCtaText,
  ctaLink = defaultCtaLink,
}) => {
  return (
    <section
      className="hero min-h-[80vh] flex items-center justify-center"
      data-testid="hero-section"
      aria-label="Hero section"
    >
      <motion.div
        className="hero-content text-center max-w-4xl px-4"
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.8, ease: 'easeOut' }}
      >
        <div className="flex flex-col items-center gap-6">
          <h1
            className="text-4xl md:text-5xl lg:text-6xl font-bold leading-tight text-base-content"
            data-testid="hero-headline"
          >
            {headline}
          </h1>

          <p
            className="text-lg md:text-xl text-base-content/70 max-w-2xl"
            data-testid="hero-subheading"
          >
            {subheading}
          </p>

          <motion.div
            initial={{ opacity: 0, scale: 0.9 }}
            animate={{ opacity: 1, scale: 1 }}
            transition={{ delay: 0.3, duration: 0.5 }}
          >
            <Link to={ctaLink}>
              <FuturisticButton
                variant="primary"
                size="lg"
                data-testid="hero-cta-button"
                aria-label={ctaText}
              >
                {ctaText}
              </FuturisticButton>
            </Link>
          </motion.div>
        </div>
      </motion.div>
    </section>
  )
}
