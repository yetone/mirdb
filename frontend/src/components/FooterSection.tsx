import { Link } from 'react-router-dom'

export default function FooterSection() {
  const currentYear = new Date().getFullYear()

  return (
    <footer
      data-testid="footer-section"
      className="bg-base-200 py-8 px-4"
    >
      <div className="container mx-auto max-w-6xl">
        <div
          data-testid="footer-grid"
          className="grid grid-cols-1 md:grid-cols-3 gap-8 mb-8"
        >
          {/* Brand Section */}
          <div data-testid="footer-brand">
            <h3 className="text-lg font-bold mb-4">URL Shortener</h3>
            <p className="text-base-content/70">
              Transform your long URLs into short, memorable links and track every click.
            </p>
          </div>

          {/* Navigation Links */}
          <div data-testid="footer-nav">
            <h4 className="text-md font-semibold mb-4">Navigation</h4>
            <nav className="flex flex-col gap-2">
              <Link
                to="/"
                data-testid="footer-link-home"
                className="link link-hover text-base-content/70 min-h-[44px] flex items-center"
              >
                Home
              </Link>
              <Link
                to="/login"
                data-testid="footer-link-login"
                className="link link-hover text-base-content/70 min-h-[44px] flex items-center"
              >
                Login
              </Link>
              <Link
                to="/register"
                data-testid="footer-link-register"
                className="link link-hover text-base-content/70 min-h-[44px] flex items-center"
              >
                Register
              </Link>
            </nav>
          </div>

          {/* Legal Links */}
          <div data-testid="footer-legal">
            <h4 className="text-md font-semibold mb-4">Legal</h4>
            <nav className="flex flex-col gap-2">
              <Link
                to="/privacy"
                data-testid="footer-link-privacy"
                className="link link-hover text-base-content/70 min-h-[44px] flex items-center"
              >
                Privacy Policy
              </Link>
              <Link
                to="/terms"
                data-testid="footer-link-terms"
                className="link link-hover text-base-content/70 min-h-[44px] flex items-center"
              >
                Terms of Service
              </Link>
            </nav>
          </div>
        </div>

        {/* Copyright */}
        <div
          data-testid="footer-copyright"
          className="border-t border-base-content/10 pt-6 text-center text-base-content/60"
        >
          <p>&copy; {currentYear} URL Shortener. All rights reserved.</p>
        </div>
      </div>
    </footer>
  )
}
