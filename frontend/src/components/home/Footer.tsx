/**
 * Footer - Page footer component
 * Owner: Scenario 10 - Footer Section
 *
 * Displays copyright and quick links.
 *
 * Expected exports:
 * - Footer: React.FC
 *
 * Content:
 * - Copyright information
 * - Quick links to Login/Register
 * - Optional: Social links, About information
 *
 * Styling:
 * - Theme-aware colors
 * - Responsive padding/margins
 */
import { Link } from 'react-router-dom'

export function Footer() {
  const currentYear = new Date().getFullYear()

  return (
    <footer
      className="bg-base-300 py-8 px-4 lg:px-8"
      role="contentinfo"
      aria-label="Site footer"
      data-testid="footer"
    >
      <div className="max-w-6xl mx-auto">
        <div className="flex flex-col md:flex-row justify-between items-center gap-6">
          {/* Copyright Section */}
          <div className="text-center md:text-left">
            <p className="text-base-content/70" data-testid="copyright">
              &copy; {currentYear} URL Shortener. All rights reserved.
            </p>
          </div>

          {/* Quick Links Section */}
          <nav aria-label="Footer navigation">
            <ul className="flex flex-wrap justify-center gap-6">
              <li>
                <Link
                  to="/login"
                  className="text-base-content/70 hover:text-primary transition-colors"
                  data-testid="footer-login-link"
                >
                  Login
                </Link>
              </li>
              <li>
                <Link
                  to="/register"
                  className="text-base-content/70 hover:text-primary transition-colors"
                  data-testid="footer-register-link"
                >
                  Register
                </Link>
              </li>
            </ul>
          </nav>
        </div>
      </div>
    </footer>
  )
}

export default Footer
