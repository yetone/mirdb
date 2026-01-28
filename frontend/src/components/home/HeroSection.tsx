/**
 * Hero Section Component
 * Owner: Scenario 1 - Hero Section Display
 *
 * Displays the main above-the-fold content with:
 * - Compelling headline about URL shortening
 * - Brief product description
 * - Primary CTA: "Get Started" → /register
 * - Secondary CTA: "Log In" → /login
 */
import { Link } from 'react-router-dom'
import { motion } from 'framer-motion'
import { FuturisticButton } from '../FuturisticButton'

export function HeroSection() {
  return (
    <section
      data-testid="hero-section"
      className="min-h-screen flex items-center justify-center px-4 py-16"
    >
      <div className="max-w-4xl mx-auto text-center">
        <motion.h1
          className="text-4xl md:text-6xl font-bold mb-6 bg-gradient-to-r from-primary to-secondary bg-clip-text text-transparent"
          initial={{ opacity: 0, y: 30 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6 }}
        >
          Shorten URLs, Amplify Your Reach
        </motion.h1>

        <motion.p
          className="text-lg md:text-xl text-base-content/80 mb-8 max-w-2xl mx-auto"
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6, delay: 0.2 }}
        >
          Transform long, unwieldy URLs into short, memorable links. Track clicks with
          powerful analytics and manage all your links in one place.
        </motion.p>

        <motion.div
          className="flex flex-col sm:flex-row gap-4 justify-center items-center"
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6, delay: 0.4 }}
        >
          <Link to="/register">
            <FuturisticButton variant="primary" size="lg">
              Get Started
            </FuturisticButton>
          </Link>
          <Link to="/login">
            <FuturisticButton variant="ghost" size="lg">
              Log In
            </FuturisticButton>
          </Link>
        </motion.div>
      </div>
    </section>
  )
}
