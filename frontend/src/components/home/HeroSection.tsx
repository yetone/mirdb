/**
 * Hero section component for homepage.
 * Owner: Scenario 1 - Hero Section Value Proposition Display
 *
 * Displays above-the-fold content with value proposition.
 */

import { Link } from 'react-router-dom'
import { motion } from 'framer-motion'

export default function HeroSection() {
  return (
    <section
      className="hero min-h-[80vh] bg-base-200"
      aria-label="Hero section"
    >
      <div className="hero-content text-center">
        <motion.div
          className="max-w-2xl"
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6 }}
        >
          <h1
            className="text-5xl md:text-6xl font-bold mb-6 bg-gradient-to-r from-primary to-secondary bg-clip-text text-transparent"
            role="heading"
            aria-level={1}
          >
            Shorten Links, Track Results
          </h1>
          <p className="text-lg md:text-xl text-base-content/80 mb-8 leading-relaxed">
            Create short, memorable URLs and gain powerful analytics insights.
            Track clicks, analyze visitor locations, and optimize your link
            sharing strategy with our comprehensive URL shortening service.
          </p>
          <div className="flex flex-col sm:flex-row gap-4 justify-center">
            <Link
              to="/register"
              className="btn btn-primary btn-lg"
              aria-label="Get started with registration"
            >
              Get Started Free
            </Link>
            <Link
              to="/login"
              className="btn btn-outline btn-lg"
              aria-label="Login to your account"
            >
              Login
            </Link>
          </div>
        </motion.div>
      </div>
    </section>
  )
}
