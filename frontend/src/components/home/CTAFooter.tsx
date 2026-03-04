/**
 * CTA Footer Component
 * Owner: Scenario 2 - Navigation and CTA Buttons
 *
 * Final call-to-action section at the bottom of the homepage.
 *
 * Requirements:
 * - Secondary "Get Started" CTA button
 * - Use FuturisticButton component
 * - Links to /register
 * - Optional "Login" link for existing users
 */
import { motion } from 'framer-motion'
import { Link, useNavigate } from 'react-router-dom'
import { FuturisticButton } from '../FuturisticButton'

export interface CTAFooterProps {
  headline?: string
  subheading?: string
  ctaText?: string
  ctaLink?: string
  showLoginLink?: boolean
}

const fadeInUpVariants = {
  hidden: { opacity: 0, y: 30 },
  visible: {
    opacity: 1,
    y: 0,
    transition: {
      duration: 0.6,
      ease: 'easeOut',
    },
  },
}

const staggerContainer = {
  hidden: { opacity: 0 },
  visible: {
    opacity: 1,
    transition: {
      staggerChildren: 0.15,
    },
  },
}

export function CTAFooter({
  headline = 'Ready to Get Started?',
  subheading = 'Join thousands of users who trust us with their URL shortening needs.',
  ctaText = 'Get Started',
  ctaLink = '/register',
  showLoginLink = true,
}: CTAFooterProps) {
  const navigate = useNavigate()

  return (
    <section
      className="py-20 px-4 sm:px-6 lg:px-8 bg-gradient-to-b from-base-100 to-base-200"
      data-testid="cta-footer"
      aria-labelledby="cta-footer-headline"
    >
      <motion.div
        className="max-w-3xl mx-auto text-center"
        variants={staggerContainer}
        initial="hidden"
        whileInView="visible"
        viewport={{ once: true, margin: '-100px' }}
      >
        <motion.h2
          id="cta-footer-headline"
          className="text-3xl sm:text-4xl font-bold mb-4"
          variants={fadeInUpVariants}
          data-testid="cta-footer-headline"
        >
          {headline}
        </motion.h2>

        <motion.p
          className="text-lg text-base-content/70 mb-8"
          variants={fadeInUpVariants}
          data-testid="cta-footer-subheading"
        >
          {subheading}
        </motion.p>

        <motion.div
          className="flex flex-col sm:flex-row items-center justify-center gap-4"
          variants={fadeInUpVariants}
        >
          <FuturisticButton
            variant="primary"
            size="lg"
            onClick={() => navigate(ctaLink)}
            data-testid="cta-footer-button"
            aria-label={`${ctaText} - Create an account`}
          >
            {ctaText}
          </FuturisticButton>

          {showLoginLink && (
            <Link
              to="/login"
              className="text-base-content/70 hover:text-primary transition-colors underline underline-offset-4"
              data-testid="cta-footer-login"
              aria-label="Already have an account? Login"
            >
              Already have an account? Login
            </Link>
          )}
        </motion.div>
      </motion.div>
    </section>
  )
}

export default CTAFooter
