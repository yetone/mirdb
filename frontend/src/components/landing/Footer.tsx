/**
 * Footer Component
 * Owner: Scenario 9 - Footer Section
 *
 * Page footer with links and copyright.
 *
 * Links:
 * - Privacy Policy
 * - Terms of Service
 * - Contact
 * - Copyright notice with current year
 *
 * Features:
 * - Responsive layout (horizontal on desktop, stacked on mobile)
 * - Accessible navigation with proper ARIA labels
 * - Dynamic year display
 */

import { Link } from 'react-router-dom';
import { motion } from 'framer-motion';
import { FOOTER_CONTENT } from '../../constants/landingContent';

// Animation variants for entrance animation
const containerVariants = {
  hidden: { opacity: 0 },
  visible: {
    opacity: 1,
    transition: {
      duration: 0.5,
      ease: 'easeOut',
      staggerChildren: 0.1,
    },
  },
} as const;

const itemVariants = {
  hidden: { opacity: 0, y: 10 },
  visible: {
    opacity: 1,
    y: 0,
    transition: {
      duration: 0.3,
    },
  },
} as const;

/**
 * Footer displays page footer with navigation links and copyright notice.
 * Features responsive design and animated entrance.
 */
export function Footer() {
  const currentYear = new Date().getFullYear();

  return (
    <footer
      id="footer"
      className="py-8 px-4 sm:px-6 lg:px-8 bg-base-200 border-t border-base-300"
      role="contentinfo"
      data-testid="footer-section"
    >
      <motion.div
        className="max-w-6xl mx-auto"
        variants={containerVariants}
        initial="hidden"
        whileInView="visible"
        viewport={{ once: true }}
      >
        <div className="flex flex-col md:flex-row md:items-center md:justify-between gap-4">
          {/* Navigation Links */}
          <nav
            aria-label="Footer navigation"
            data-testid="footer-nav"
          >
            <ul className="flex flex-col sm:flex-row gap-2 sm:gap-6 items-center sm:items-start">
              {FOOTER_CONTENT.links.map((link) => (
                <motion.li key={link.label} variants={itemVariants}>
                  <Link
                    to={link.href}
                    className="text-base-content/70 hover:text-primary transition-colors duration-200
                               text-sm sm:text-base focus:outline-none focus:ring-2 focus:ring-primary
                               focus:ring-offset-2 focus:ring-offset-base-200 rounded px-2 py-1"
                    data-testid={`footer-link-${link.label.toLowerCase().replace(/\s+/g, '-')}`}
                  >
                    {link.label}
                  </Link>
                </motion.li>
              ))}
            </ul>
          </nav>

          {/* Copyright Notice */}
          <motion.div
            variants={itemVariants}
            className="text-center md:text-right"
          >
            <p
              className="text-base-content/60 text-sm"
              data-testid="footer-copyright"
            >
              &copy; {currentYear} {FOOTER_CONTENT.copyright}. All rights reserved.
            </p>
          </motion.div>
        </div>
      </motion.div>
    </footer>
  );
}
