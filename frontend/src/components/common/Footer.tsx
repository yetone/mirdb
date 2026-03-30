/**
 * Footer component for application pages.
 * Owner: Scenario 10 - Footer Content and Links
 *
 * Displays:
 * - Copyright notice with current year
 * - Terms of Service link
 * - Privacy Policy link
 */

import React from 'react'
import { Link } from 'react-router-dom'

export const Footer: React.FC = () => {
  const currentYear = new Date().getFullYear()

  return (
    <footer
      className="py-8 text-center text-base-content/60 border-t border-base-300"
      data-testid="footer"
    >
      <div className="container mx-auto px-4">
        <p className="mb-4" data-testid="copyright">
          &copy; {currentYear} URL Shortener. All rights reserved.
        </p>
        <nav className="flex justify-center gap-6" aria-label="Footer navigation">
          <Link
            to="/terms"
            className="hover:text-primary transition-colors"
            data-testid="terms-link"
          >
            Terms of Service
          </Link>
          <Link
            to="/privacy"
            className="hover:text-primary transition-colors"
            data-testid="privacy-link"
          >
            Privacy Policy
          </Link>
        </nav>
      </div>
    </footer>
  )
}

export default Footer
