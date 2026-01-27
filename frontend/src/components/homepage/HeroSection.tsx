/**
 * Hero Section Component
 * Owner: Scenario 1 - Hero Section Display
 *
 * Displays the main hero section with:
 * - Product name and compelling headline
 * - Subheadline with supporting text
 * - Primary CTA button (Get Started -> /register)
 * - Secondary CTA button (Login -> /login)
 * - BackgroundEffect component for visual appeal
 */
import { motion } from 'framer-motion'
import { Link } from 'react-router-dom'
import FuturisticButton from '../FuturisticButton'
import BackgroundEffect from '../BackgroundEffect'

export default function HeroSection() {
  return (
    <section className="relative min-h-screen flex items-center justify-center overflow-hidden">
      <BackgroundEffect />

      <div className="relative z-10 container mx-auto px-4 py-16 text-center">
        <motion.div
          initial={{ opacity: 0, y: 30 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6 }}
        >
          {/* Product Name */}
          <motion.span
            className="inline-block text-primary font-bold text-lg mb-4 tracking-wider uppercase"
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            transition={{ delay: 0.2 }}
          >
            URL Shortener
          </motion.span>

          {/* Main Headline */}
          <h1 className="text-4xl md:text-5xl lg:text-6xl font-bold mb-6 leading-tight">
            Shorten URLs.{' '}
            <span className="text-transparent bg-clip-text bg-gradient-to-r from-primary to-secondary">
              Track Clicks.
            </span>{' '}
            Grow Your Reach.
          </h1>

          {/* Subheadline */}
          <p className="text-lg md:text-xl text-base-content/70 mb-10 max-w-2xl mx-auto" data-testid="subheadline">
            Create short, memorable links in seconds. Track performance with detailed analytics.
            Manage all your URLs in one powerful dashboard.
          </p>

          {/* CTA Buttons */}
          <div className="flex flex-col sm:flex-row gap-4 justify-center items-center">
            <Link to="/register">
              <FuturisticButton variant="primary" className="min-w-[180px]">
                Get Started
              </FuturisticButton>
            </Link>
            <Link to="/login">
              <FuturisticButton variant="outline" className="min-w-[180px]">
                Login
              </FuturisticButton>
            </Link>
          </div>
        </motion.div>
      </div>
    </section>
  )
}
