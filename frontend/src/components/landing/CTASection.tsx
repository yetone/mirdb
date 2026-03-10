/**
 * Call-to-Action Section Component
 * Owner: Scenario 10 - Call-to-Action Section
 *
 * Section encouraging users to sign up for full analytics features.
 *
 * Features:
 * - Compelling headline about analytics
 * - Sign Up Free button linking to /register
 * - Visual prominence with gradient background styling
 * - Hover animation effects on button
 */

import { motion } from 'framer-motion';
import { Link } from 'react-router-dom';
import { CTA_CONTENT } from '../../constants/landingContent';

// Animation variants for entrance animation
const containerVariants = {
  hidden: { opacity: 0, y: 20 },
  visible: {
    opacity: 1,
    y: 0,
    transition: {
      duration: 0.6,
      ease: 'easeOut',
    },
  },
} as const;

// Button hover animation
const buttonVariants = {
  initial: { scale: 1 },
  hover: {
    scale: 1.05,
    boxShadow: '0 10px 30px rgba(0, 0, 0, 0.2)',
    transition: {
      type: 'spring',
      stiffness: 400,
      damping: 15,
    },
  },
  tap: { scale: 0.98 },
} as const;

/**
 * CTASection displays a call-to-action encouraging user registration.
 * Features visual prominence with gradient background and animated button.
 */
export function CTASection() {
  return (
    <section
      id="cta"
      className="py-16 px-4 sm:px-6 lg:px-8 bg-gradient-to-r from-primary/10 via-secondary/10 to-primary/10"
      aria-labelledby="cta-title"
      data-testid="cta-section"
    >
      <motion.div
        className="max-w-4xl mx-auto text-center"
        variants={containerVariants}
        initial="hidden"
        whileInView="visible"
        viewport={{ once: true, margin: '-50px' }}
      >
        {/* Headline */}
        <h2
          id="cta-title"
          className="text-3xl sm:text-4xl font-bold text-base-content mb-4"
          data-testid="cta-headline"
        >
          {CTA_CONTENT.headline}
        </h2>

        {/* Description */}
        <p
          className="text-lg text-base-content/70 mb-8 max-w-2xl mx-auto"
          data-testid="cta-description"
        >
          {CTA_CONTENT.description}
        </p>

        {/* Sign Up Button */}
        <motion.div
          variants={buttonVariants}
          initial="initial"
          whileHover="hover"
          whileTap="tap"
          className="inline-block"
        >
          <Link
            to="/register"
            className="btn btn-primary btn-lg px-8 py-3 text-lg font-semibold
                       shadow-lg hover:shadow-xl transition-shadow duration-300
                       hover:scale-105"
            data-testid="cta-signup-button"
          >
            {CTA_CONTENT.buttonText}
          </Link>
        </motion.div>
      </motion.div>
    </section>
  );
}
