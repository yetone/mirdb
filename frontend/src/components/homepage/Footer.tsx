/**
 * Homepage Footer Component
 * Owner: Scenario 9 - Footer Section Display
 *
 * Displays footer section with:
 * - Navigation links (Login, Register)
 * - Copyright notice
 * - ThemeToggle component
 *
 * Dependencies:
 * - ThemeToggle from ../ThemeToggle
 * - react-router-dom for Link components
 */
import { Link } from 'react-router-dom'
import ThemeToggle from '../ThemeToggle'

export default function Footer() {
  const currentYear = new Date().getFullYear()

  return (
    <footer
      className="bg-base-200 border-t border-base-content/10 py-8"
      data-testid="footer-section"
    >
      <div className="container mx-auto px-4">
        <div className="flex flex-col md:flex-row justify-between items-center gap-6">
          {/* Navigation Links */}
          <nav
            className="flex flex-wrap gap-6 justify-center"
            aria-label="Footer navigation"
          >
            <Link
              to="/login"
              className="text-base-content/70 hover:text-primary transition-colors"
              data-testid="footer-login-link"
            >
              Login
            </Link>
            <Link
              to="/register"
              className="text-base-content/70 hover:text-primary transition-colors"
              data-testid="footer-register-link"
            >
              Register
            </Link>
          </nav>

          {/* Copyright Notice */}
          <p
            className="text-base-content/60 text-sm text-center"
            data-testid="footer-copyright"
          >
            &copy; {currentYear} URL Shortener. All rights reserved.
          </p>

          {/* Theme Toggle */}
          <div
            className="flex items-center gap-2"
            data-testid="footer-theme-toggle"
          >
            <span className="text-base-content/60 text-sm">Theme:</span>
            <ThemeToggle />
          </div>
        </div>
      </div>
    </footer>
  )
}
