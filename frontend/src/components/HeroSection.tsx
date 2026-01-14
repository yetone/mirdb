import { motion } from 'framer-motion'
import { Link } from 'react-router-dom'

const HeroSection = () => {
  return (
    <section
      className="min-h-screen flex items-center justify-center bg-gradient-to-br from-base-200 to-base-300 px-4"
      data-testid="hero-section"
    >
      <div className="max-w-4xl mx-auto text-center">
        <motion.h1
          className="text-4xl md:text-6xl font-bold mb-6 text-base-content"
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6 }}
        >
          Shorten URLs. Track Results.
        </motion.h1>

        <motion.p
          className="text-lg md:text-xl text-base-content/70 mb-8 max-w-2xl mx-auto"
          data-testid="hero-subheadline"
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6, delay: 0.2 }}
        >
          Create short, memorable links and gain insights into your audience with powerful analytics.
          Simple, fast, and free to get started.
        </motion.p>

        <motion.div
          className="flex flex-col sm:flex-row gap-4 justify-center items-center"
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6, delay: 0.4 }}
        >
          <Link
            to="/register"
            className="btn btn-primary btn-lg"
            data-testid="cta-get-started"
          >
            Get Started Free
          </Link>

          <Link
            to="/login"
            className="btn btn-outline btn-lg"
            data-testid="cta-login"
          >
            Login
          </Link>
        </motion.div>
      </div>
    </section>
  )
}

export default HeroSection
