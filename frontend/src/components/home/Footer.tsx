/**
 * Footer Component
 * Owner: Scenario 12 - Footer Display
 *
 * Homepage footer with product information and links.
 *
 * Expected exports:
 * - Footer: React.FC
 *
 * Content:
 * - Product branding/name
 * - Relevant navigation links
 * - Copyright or attribution
 *
 * Must use semantic <footer> HTML element.
 */

import { Link } from 'react-router-dom'

export function Footer() {
  const currentYear = new Date().getFullYear()

  return (
    <footer
      className="py-8 px-4 bg-base-200"
      aria-label="Footer"
      data-testid="footer-section"
    >
      <div className="max-w-6xl mx-auto">
        <div className="grid grid-cols-1 md:grid-cols-3 gap-8 mb-8">
          {/* Product Branding */}
          <div className="text-center md:text-left">
            <h3 className="text-lg font-bold text-base-content mb-2">
              URL Shortener
            </h3>
            <p className="text-base-content/70 text-sm">
              Shorten, share, and track your links with ease.
            </p>
          </div>

          {/* Navigation Links */}
          <div className="text-center">
            <h4 className="text-sm font-semibold text-base-content mb-3">
              Quick Links
            </h4>
            <nav aria-label="Footer navigation">
              <ul className="space-y-2">
                <li>
                  <Link
                    to="/"
                    className="text-base-content/70 hover:text-primary text-sm transition-colors"
                  >
                    Home
                  </Link>
                </li>
                <li>
                  <Link
                    to="/login"
                    className="text-base-content/70 hover:text-primary text-sm transition-colors"
                  >
                    Login
                  </Link>
                </li>
                <li>
                  <Link
                    to="/register"
                    className="text-base-content/70 hover:text-primary text-sm transition-colors"
                  >
                    Register
                  </Link>
                </li>
              </ul>
            </nav>
          </div>

          {/* Product Info */}
          <div className="text-center md:text-right">
            <h4 className="text-sm font-semibold text-base-content mb-3">
              Features
            </h4>
            <ul className="space-y-2 text-base-content/70 text-sm">
              <li>Analytics Tracking</li>
              <li>Dashboard Management</li>
              <li>Theme Support</li>
            </ul>
          </div>
        </div>

        {/* Copyright */}
        <div className="border-t border-base-300 pt-6 text-center">
          <p className="text-base-content/70 text-sm">
            &copy; {currentYear} URL Shortener. All rights reserved.
          </p>
        </div>
      </div>
    </footer>
  )
}

export default Footer
