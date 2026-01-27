/**
 * Hero Section Component
 * Owner: Scenario 1 - Hero Section Display and Value Proposition
 *
 * Requirements:
 * - Large, bold headline (e.g., "Shorten URLs. Track Clicks. Grow Your Reach.")
 * - Supporting subheadline with brief description
 * - Primary CTA button ("Get Started" / "Create Free Account") -> /register
 * - Secondary CTA for existing users ("Sign In") -> /login
 * - BackgroundEffect component integration
 * - Theme-aware styling
 */
import { Link } from 'react-router-dom';
import { motion } from 'framer-motion';
import BackgroundEffect from '../BackgroundEffect';
import FuturisticButton from '../FuturisticButton';

export default function HeroSection() {
  return (
    <section className="relative min-h-[80vh] flex items-center justify-center overflow-hidden">
      <BackgroundEffect />

      <div className="container mx-auto px-4 relative z-10">
        <motion.div
          initial={{ opacity: 0, y: 30 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6 }}
          className="text-center max-w-4xl mx-auto"
        >
          <motion.h1
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.6, delay: 0.1 }}
            className="text-4xl md:text-5xl lg:text-6xl font-bold mb-6 leading-tight"
          >
            Shorten URLs.{' '}
            <span className="text-primary">Track Clicks.</span>{' '}
            <span className="text-secondary">Grow Your Reach.</span>
          </motion.h1>

          <motion.p
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.6, delay: 0.2 }}
            className="text-lg md:text-xl text-base-content/70 mb-8 max-w-2xl mx-auto"
          >
            Transform your long URLs into short, memorable links. Get powerful analytics
            and insights to understand your audience and optimize your reach.
          </motion.p>

          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.6, delay: 0.3 }}
            className="flex flex-col sm:flex-row gap-4 justify-center items-center"
          >
            <Link to="/register">
              <FuturisticButton variant="primary" size="lg">
                Get Started
              </FuturisticButton>
            </Link>

            <Link to="/login">
              <FuturisticButton variant="outline" size="lg">
                Sign In
              </FuturisticButton>
            </Link>
          </motion.div>
        </motion.div>
      </div>
    </section>
  );
}
