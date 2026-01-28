/**
 * Footer Component
 * Owner: Scenario 6 - Footer Navigation
 *
 * Homepage footer with:
 * - Navigation links: Home (/), Login (/login), Register (/register)
 * - Copyright information
 *
 * Uses semantic <footer> element and React Router Link for navigation.
 */
import { Link } from 'react-router-dom'

export function Footer() {
  const currentYear = new Date().getFullYear()

  return (
    <footer
      data-testid="footer"
      className="bg-base-200 py-8 px-4 mt-auto"
    >
      <div className="max-w-4xl mx-auto">
        <nav
          aria-label="Footer navigation"
          className="flex flex-wrap justify-center gap-6 mb-6"
        >
          <Link
            to="/"
            className="link link-hover text-base-content/70 hover:text-primary transition-colors"
          >
            Home
          </Link>
          <Link
            to="/login"
            className="link link-hover text-base-content/70 hover:text-primary transition-colors"
          >
            Login
          </Link>
          <Link
            to="/register"
            className="link link-hover text-base-content/70 hover:text-primary transition-colors"
          >
            Register
          </Link>
        </nav>

        <div className="text-center text-base-content/60 text-sm">
          <p>&copy; {currentYear} URL Shortener. All rights reserved.</p>
        </div>
      </div>
    </footer>
  )
}
