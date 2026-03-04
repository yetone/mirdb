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
import { FuturisticButton } from '../FuturisticButton'
import type { HeroSectionProps } from '../../types/home'

const fadeInVariants = {
  hidden: { opacity: 0, y: 20 },
  visible: {
    opacity: 1,
    y: 0,
    transition: {
      duration: 0.8,
      ease: 'easeOut',
    },
  },
}

const staggerContainer = {
  hidden: { opacity: 0 },
  visible: {
    opacity: 1,
    transition: {
      staggerChildren: 0.2,
      delayChildren: 0.1,
    },
  },
}

export function HeroSection({
  headline = 'Shorten URLs, Track Clicks, Understand Your Audience',
  subheading = 'Create memorable short links from long URLs and get detailed analytics on who\'s clicking and where they\'re coming from.',
  ctaText = 'Get Started Free',
  ctaLink = '/register',
}: HeroSectionProps) {
  return (
    <section
      className="hero-section w-full px-4 sm:px-6 lg:px-8"
      data-testid="hero-section"
      aria-labelledby="hero-headline"
    >
      <motion.div
        className="flex flex-col items-center justify-center text-center max-w-4xl mx-auto"
        variants={staggerContainer}
        initial="hidden"
        animate="visible"
      >
        {/* Headline */}
        <motion.h1
          id="hero-headline"
          className="text-4xl sm:text-5xl lg:text-6xl font-bold mb-6 leading-tight"
          variants={fadeInVariants}
          data-testid="hero-headline"
        >
          <span className="gradient-text">{headline}</span>
        </motion.h1>

        {/* Subheading */}
        <motion.p
          className="text-lg sm:text-xl lg:text-2xl text-base-content/70 mb-10 max-w-2xl"
          variants={fadeInVariants}
          data-testid="hero-subheading"
        >
          {subheading}
        </motion.p>

        {/* Primary CTA Button */}
        <motion.div variants={fadeInVariants}>
          <FuturisticButton
            variant="primary"
            size="lg"
            href={ctaLink}
            data-testid="hero-cta"
            aria-label={ctaText}
          >
            {ctaText}
          </FuturisticButton>
        </motion.div>
      </motion.div>
    </section>
  )
}

export default HeroSection
