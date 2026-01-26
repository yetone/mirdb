/**
 * HeroSection - Above-the-fold landing section
 * Owner: Scenario 1 - Hero Section Rendering
 * Secondary Owner: Scenario 13 - BackgroundEffect Integration
 *
 * Displays the primary value proposition with CTAs.
 *
 * Features:
 * - Prominent headline with value proposition
 * - Subheadline explaining the service
 * - "Get Started Free" CTA button (links to /register)
 * - "Login" button (links to /login)
 * - BackgroundEffect integration for visual enhancement
 * - Framer Motion entrance animations
 */
import { motion } from 'framer-motion'
import { Link } from 'react-router-dom'
import BackgroundEffect from '../BackgroundEffect'
import FuturisticButton from '../FuturisticButton'

export function HeroSection() {
  return (
    <section
      className="relative min-h-[80vh] flex items-center justify-center overflow-hidden"
      data-testid="hero-section"
      aria-label="Hero section"
    >
      {/* BackgroundEffect positioned behind hero content */}
      <div
        className="absolute inset-0 -z-10"
        data-testid="hero-background-effect"
        aria-hidden="true"
      >
        <BackgroundEffect />
      </div>

      {/* Hero content positioned above background */}
      <div className="relative z-10 text-center px-4 max-w-4xl mx-auto">
        <motion.h1
          className="text-4xl md:text-5xl lg:text-6xl font-bold mb-6"
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6 }}
        >
          Shorten URLs. Track Results. Grow Smarter.
        </motion.h1>

        <motion.p
          className="text-lg md:text-xl text-base-content/70 mb-8 max-w-2xl mx-auto"
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6, delay: 0.2 }}
        >
          Transform your long URLs into powerful, trackable short links.
          Get detailed analytics on every click, geographic insights, and more.
        </motion.p>

        <motion.div
          className="flex flex-col sm:flex-row gap-4 justify-center items-center"
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6, delay: 0.4 }}
        >
          <Link to="/register">
            <FuturisticButton variant="primary" size="lg" data-testid="cta-get-started">
              Get Started Free
            </FuturisticButton>
          </Link>
          <Link to="/login">
            <FuturisticButton variant="secondary" size="lg" data-testid="cta-login">
              Login
            </FuturisticButton>
          </Link>
        </motion.div>
      </div>
    </section>
  )
}

export default HeroSection
