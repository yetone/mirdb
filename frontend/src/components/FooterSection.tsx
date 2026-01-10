import { Link } from 'react-router-dom'

export default function FooterSection() {
  const currentYear = new Date().getFullYear()

  return (
    <footer
      data-testid="footer-section"
      className="bg-base-200 py-8 px-4"
    >
      <div className="container mx-auto max-w-6xl">
        <div className="grid grid-cols-1 md:grid-cols-3 gap-8 mb-8">
          {/* Navigation Links */}
          <div data-testid="footer-nav-links">
            <h3 className="font-bold text-lg mb-4">Navigation</h3>
            <ul className="space-y-2">
              <li>
                <Link
                  to="/"
                  className="link link-hover text-base-content/70 hover:text-primary"
                >
                  Home
                </Link>
              </li>
              <li>
                <Link
                  to="/login"
                  className="link link-hover text-base-content/70 hover:text-primary"
                >
                  Login
                </Link>
              </li>
              <li>
                <Link
                  to="/register"
                  className="link link-hover text-base-content/70 hover:text-primary"
                >
                  Register
                </Link>
              </li>
            </ul>
          </div>

          {/* Legal Links */}
          <div data-testid="footer-legal-links">
            <h3 className="font-bold text-lg mb-4">Legal</h3>
            <ul className="space-y-2">
              <li>
                <Link
                  to="/privacy"
                  className="link link-hover text-base-content/70 hover:text-primary"
                >
                  Privacy Policy
                </Link>
              </li>
              <li>
                <Link
                  to="/terms"
                  className="link link-hover text-base-content/70 hover:text-primary"
                >
                  Terms of Service
                </Link>
              </li>
            </ul>
          </div>

          {/* Brand/About */}
          <div>
            <h3 className="font-bold text-lg mb-4">About</h3>
            <p className="text-base-content/70">
              Create short, memorable links and track every click with powerful analytics.
            </p>
          </div>
        </div>

        {/* Copyright Notice */}
        <div className="border-t border-base-300 pt-6 text-center">
          <p
            data-testid="footer-copyright"
            className="text-base-content/60 text-sm"
          >
            © {currentYear} URL Shortener. All rights reserved.
          </p>
        </div>
      </div>
    </footer>
  )
}
