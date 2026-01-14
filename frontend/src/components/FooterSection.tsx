import { Link } from 'react-router-dom'

const FooterSection = () => {
  const currentYear = new Date().getFullYear()

  return (
    <footer
      className="bg-base-300 py-12"
      data-testid="footer-section"
      role="contentinfo"
    >
      <div className="container max-w-6xl mx-auto px-4">
        <div className="flex flex-col md:flex-row justify-between items-center gap-8">
          {/* Navigation Links */}
          <nav
            className="flex flex-wrap justify-center gap-6"
            aria-label="Footer navigation"
          >
            <Link
              to="/"
              className="link link-hover text-base-content/80 hover:text-primary transition-colors"
            >
              Home
            </Link>
            <Link
              to="/login"
              className="link link-hover text-base-content/80 hover:text-primary transition-colors"
            >
              Login
            </Link>
            <Link
              to="/register"
              className="link link-hover text-base-content/80 hover:text-primary transition-colors"
            >
              Register
            </Link>
          </nav>

          {/* Legal Links */}
          <nav className="flex flex-wrap justify-center gap-6">
            <Link
              to="/privacy"
              className="link link-hover text-base-content/60 hover:text-primary text-sm transition-colors"
            >
              Privacy
            </Link>
            <Link
              to="/terms"
              className="link link-hover text-base-content/60 hover:text-primary text-sm transition-colors"
            >
              Terms
            </Link>
          </nav>
        </div>

        {/* Copyright Notice */}
        <div className="border-t border-base-content/10 mt-8 pt-8">
          <p
            className="text-center text-base-content/60 text-sm"
            data-testid="footer-copyright"
          >
            © {currentYear} URL Shortener. All rights reserved.
          </p>
        </div>
      </div>
    </footer>
  )
}

export default FooterSection
