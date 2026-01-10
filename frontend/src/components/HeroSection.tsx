import { Link } from 'react-router-dom'
import { motion } from 'framer-motion'

export default function HeroSection() {
  return (
    <section
      data-testid="hero-section"
      className="relative min-h-screen flex items-center justify-center bg-gradient-to-br from-primary via-secondary to-accent overflow-hidden"
    >
      {/* Animated background elements */}
      <div className="absolute inset-0 overflow-hidden">
        <div className="absolute -top-40 -right-40 w-80 h-80 bg-primary/30 rounded-full blur-3xl animate-pulse" />
        <div className="absolute -bottom-40 -left-40 w-80 h-80 bg-secondary/30 rounded-full blur-3xl animate-pulse delay-1000" />
      </div>

      <div className="relative z-10 text-center px-4 sm:px-6 lg:px-8 max-w-4xl mx-auto">
        <motion.h1
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6 }}
          className="text-4xl sm:text-5xl md:text-6xl lg:text-7xl font-bold text-white mb-6"
        >
          <span className="block">Shorten, Share, Track</span>
        </motion.h1>

        <motion.p
          data-testid="hero-subheadline"
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6, delay: 0.2 }}
          className="text-lg sm:text-xl md:text-2xl text-white/90 mb-10 max-w-2xl mx-auto"
        >
          Transform your long URLs into short, memorable links and track every click with powerful analytics.
        </motion.p>

        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6, delay: 0.4 }}
          className="flex flex-col sm:flex-row gap-4 justify-center items-center"
        >
          <Link
            to="/register"
            className="btn btn-lg btn-primary bg-white text-primary hover:bg-white/90 border-none shadow-lg"
          >
            Get Started Free
          </Link>

          <Link
            to="/login"
            className="btn btn-lg btn-ghost text-white border-white/30 hover:bg-white/10"
          >
            Already have an account? Log in
          </Link>
        </motion.div>
      </div>
    </section>
  )
}
