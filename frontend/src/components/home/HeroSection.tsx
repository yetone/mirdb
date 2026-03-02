/**
 * Hero Section Component.
 * Owner: Scenario 1 - Hero Section Display and Content
 *
 * Displays the main hero area with:
 * - Headline: "Shorten Links. Track Clicks. Grow Your Impact."
 * - Subheadline describing the service
 * - URL shortening form (imported from UrlShortenForm)
 * - Primary CTA: "Sign Up Free"
 * - Secondary CTA: "Try as Guest"
 * - Animated visual element
 *
 * Requirements: REQ-1, REQ-3, REQ-4
 * Min height: 600px
 *
 * Animation: Uses Framer Motion for entrance animations (fade + slide)
 * Duration: 0.5s with ease-out easing per PRD visual design guidelines
 */
import { Link } from 'react-router-dom';
import { motion } from 'framer-motion';
import { Link as LinkIcon, Scissors, BarChart3 } from 'lucide-react';

// Animation variants for hero content entrance animation
const containerVariants = {
  hidden: { opacity: 0 },
  visible: {
    opacity: 1,
    transition: {
      staggerChildren: 0.15,
      delayChildren: 0.1,
    },
  },
};

const itemVariants = {
  hidden: { opacity: 0, y: 20 },
  visible: {
    opacity: 1,
    y: 0,
    transition: {
      duration: 0.5,
      ease: 'easeOut',
    },
  },
};

export function HeroSection() {
  return (
    <section
      className="hero-section flex flex-col items-center justify-center px-4 py-16 md:py-24"
      style={{ minHeight: '600px' }}
      aria-labelledby="hero-headline"
      data-testid="hero-section"
    >
      <motion.div
        className="max-w-4xl mx-auto text-center"
        variants={containerVariants}
        initial="hidden"
        animate="visible"
        data-testid="hero-content"
      >
        {/* Headline */}
        <motion.h1
          id="hero-headline"
          className="text-4xl md:text-5xl lg:text-6xl font-bold mb-6 leading-tight"
          variants={itemVariants}
        >
          Shorten Links. Track Clicks. Grow Your Impact.
        </motion.h1>

        {/* Subheadline - using /80 opacity for WCAG AA contrast compliance */}
        <motion.p
          className="text-lg md:text-xl text-base-content/80 mb-8 max-w-2xl mx-auto"
          variants={itemVariants}
        >
          Free URL shortener with powerful analytics. No account required for basic use.
        </motion.p>

        {/* URL Shortening Form Placeholder - will be implemented by Scenario 2 */}
        <motion.div className="mb-8 max-w-xl mx-auto" variants={itemVariants}>
          <div className="flex flex-col sm:flex-row gap-2">
            <input
              type="url"
              placeholder="Paste your long URL here..."
              className="input input-bordered input-lg flex-1 w-full"
              aria-label="URL to shorten"
            />
            <button
              type="button"
              className="btn btn-primary btn-lg"
              aria-label="Shorten URL"
            >
              Shorten
            </button>
          </div>
        </motion.div>

        {/* CTA Buttons */}
        <motion.div
          className="flex flex-col sm:flex-row gap-4 justify-center items-center mb-12"
          variants={itemVariants}
        >
          <Link
            to="/register"
            className="btn btn-primary btn-lg"
            role="button"
          >
            Sign Up Free
          </Link>
          <button
            type="button"
            className="btn btn-outline btn-lg"
            role="button"
          >
            Try as Guest
          </button>
        </motion.div>

        {/* Visual Element - Animated Icon Transformation */}
        <motion.div
          className="flex items-center justify-center gap-4 text-base-content/50"
          aria-hidden="true"
          variants={itemVariants}
        >
          <div className="flex items-center gap-2">
            <LinkIcon className="w-8 h-8" />
            <span className="text-sm">Long URL</span>
          </div>
          <Scissors className="w-6 h-6 text-primary" />
          <div className="flex items-center gap-2">
            <span className="text-sm font-semibold text-primary">Short Link</span>
            <BarChart3 className="w-8 h-8 text-primary" />
          </div>
        </motion.div>
      </motion.div>
    </section>
  );
}
