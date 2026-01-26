/**
 * Footer Component
 * Owner: Scenario 7 - Footer Section
 *
 * This component renders the page footer with legal links and copyright.
 *
 * Expected features:
 * - Copyright information with current year
 * - Privacy Policy link
 * - Terms of Service link
 * - Responsive layout
 */

import { motion } from 'framer-motion'

export function Footer() {
  const currentYear = new Date().getFullYear()

  return (
    <footer
      data-testid="footer-section"
      className="border-t border-base-content/10 py-8 mt-auto"
      role="contentinfo"
    >
      <div className="container mx-auto px-4">
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.5 }}
          viewport={{ once: true }}
          className="flex flex-col md:flex-row justify-between items-center gap-4"
        >
          <p
            data-testid="copyright-text"
            className="text-sm text-base-content/60"
          >
            © {currentYear} URL Shortener. All rights reserved.
          </p>

          <nav
            data-testid="footer-links"
            className="flex gap-6"
            aria-label="Footer navigation"
          >
            <a
              href="/privacy"
              data-testid="privacy-policy-link"
              className="text-sm text-base-content/60 hover:text-base-content transition-colors"
            >
              Privacy Policy
            </a>
            <a
              href="/terms"
              data-testid="terms-of-service-link"
              className="text-sm text-base-content/60 hover:text-base-content transition-colors"
            >
              Terms of Service
            </a>
          </nav>
        </motion.div>
      </div>
    </footer>
  )
}
