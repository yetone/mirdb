import { motion } from 'framer-motion'
import { Link } from 'react-router-dom'

const HeroSection = () => {
  const scrollToFeatures = () => {
    const featuresSection = document.getElementById('features')
    if (featuresSection) {
      featuresSection.scrollIntoView({ behavior: 'smooth' })
    }
  }

  return (
    <section
      data-testid="hero-section"
      className="min-h-screen flex items-center justify-center bg-gradient-to-br from-primary/10 via-base-100 to-secondary/10 px-4"
    >
      <div className="container mx-auto flex flex-col lg:flex-row items-center justify-between gap-12">
        {/* Text Content */}
        <motion.div
          className="flex-1 text-center lg:text-left"
          initial={{ opacity: 0, x: -50 }}
          animate={{ opacity: 1, x: 0 }}
          transition={{ duration: 0.6 }}
        >
          <h1
            data-testid="hero-headline"
            className="text-4xl md:text-5xl lg:text-6xl font-bold mb-6 bg-gradient-to-r from-primary to-secondary bg-clip-text text-transparent"
          >
            Shorten. Share. Track.
          </h1>
          <p
            data-testid="hero-subheadline"
            className="text-lg md:text-xl text-base-content/70 mb-8 max-w-xl mx-auto lg:mx-0"
          >
            Create memorable short URLs and track every click with powerful analytics.
            Understand your audience with detailed insights and real-time statistics.
          </p>
          <div className="flex flex-col sm:flex-row gap-4 justify-center lg:justify-start">
            <Link
              to="/register"
              data-testid="hero-cta-primary"
              className="btn btn-primary btn-lg"
            >
              Get Started Free
            </Link>
            <button
              onClick={scrollToFeatures}
              data-testid="hero-cta-secondary"
              className="btn btn-outline btn-lg"
            >
              Learn More
            </button>
          </div>
        </motion.div>

        {/* Visual Element - Animated Illustration */}
        <motion.div
          className="flex-1 flex justify-center"
          initial={{ opacity: 0, x: 50 }}
          animate={{ opacity: 1, x: 0 }}
          transition={{ duration: 0.6, delay: 0.2 }}
        >
          <div
            data-testid="hero-visual"
            className="relative w-full max-w-lg"
          >
            {/* Animated Link Illustration */}
            <motion.svg
              viewBox="0 0 400 300"
              className="w-full h-auto"
              initial={{ scale: 0.8 }}
              animate={{ scale: 1 }}
              transition={{ duration: 0.5, delay: 0.3 }}
            >
              {/* Background circles */}
              <motion.circle
                cx="200"
                cy="150"
                r="120"
                fill="currentColor"
                className="text-primary/10"
                animate={{ scale: [1, 1.1, 1] }}
                transition={{ duration: 3, repeat: Infinity, ease: "easeInOut" }}
              />
              <motion.circle
                cx="200"
                cy="150"
                r="80"
                fill="currentColor"
                className="text-secondary/10"
                animate={{ scale: [1, 1.15, 1] }}
                transition={{ duration: 3, repeat: Infinity, ease: "easeInOut", delay: 0.5 }}
              />

              {/* Link chain icon */}
              <motion.g
                initial={{ rotate: 0 }}
                animate={{ rotate: [0, 5, -5, 0] }}
                transition={{ duration: 4, repeat: Infinity, ease: "easeInOut" }}
              >
                {/* First link */}
                <motion.rect
                  x="120"
                  y="130"
                  width="80"
                  height="40"
                  rx="20"
                  fill="none"
                  stroke="currentColor"
                  strokeWidth="8"
                  className="text-primary"
                  initial={{ pathLength: 0 }}
                  animate={{ pathLength: 1 }}
                  transition={{ duration: 1, delay: 0.5 }}
                />
                {/* Second link */}
                <motion.rect
                  x="200"
                  y="130"
                  width="80"
                  height="40"
                  rx="20"
                  fill="none"
                  stroke="currentColor"
                  strokeWidth="8"
                  className="text-secondary"
                  initial={{ pathLength: 0 }}
                  animate={{ pathLength: 1 }}
                  transition={{ duration: 1, delay: 0.7 }}
                />
              </motion.g>

              {/* Floating data points */}
              <motion.circle
                cx="100"
                cy="80"
                r="8"
                fill="currentColor"
                className="text-accent"
                animate={{ y: [0, -10, 0] }}
                transition={{ duration: 2, repeat: Infinity, ease: "easeInOut" }}
              />
              <motion.circle
                cx="300"
                cy="100"
                r="6"
                fill="currentColor"
                className="text-success"
                animate={{ y: [0, -15, 0] }}
                transition={{ duration: 2.5, repeat: Infinity, ease: "easeInOut", delay: 0.3 }}
              />
              <motion.circle
                cx="320"
                cy="200"
                r="10"
                fill="currentColor"
                className="text-warning"
                animate={{ y: [0, -8, 0] }}
                transition={{ duration: 1.8, repeat: Infinity, ease: "easeInOut", delay: 0.6 }}
              />

              {/* Analytics chart bars */}
              <motion.g transform="translate(140, 200)">
                <motion.rect
                  x="0"
                  y="40"
                  width="20"
                  height="0"
                  fill="currentColor"
                  className="text-primary"
                  animate={{ height: 40, y: 0 }}
                  transition={{ duration: 0.5, delay: 1 }}
                />
                <motion.rect
                  x="30"
                  y="40"
                  width="20"
                  height="0"
                  fill="currentColor"
                  className="text-secondary"
                  animate={{ height: 60, y: -20 }}
                  transition={{ duration: 0.5, delay: 1.2 }}
                />
                <motion.rect
                  x="60"
                  y="40"
                  width="20"
                  height="0"
                  fill="currentColor"
                  className="text-accent"
                  animate={{ height: 30, y: 10 }}
                  transition={{ duration: 0.5, delay: 1.4 }}
                />
                <motion.rect
                  x="90"
                  y="40"
                  width="20"
                  height="0"
                  fill="currentColor"
                  className="text-success"
                  animate={{ height: 50, y: -10 }}
                  transition={{ duration: 0.5, delay: 1.6 }}
                />
              </motion.g>
            </motion.svg>
          </div>
        </motion.div>
      </div>
    </section>
  )
}

export default HeroSection
