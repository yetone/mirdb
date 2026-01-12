import { Link } from 'react-router-dom'

const Footer = () => {
  const currentYear = new Date().getFullYear()

  return (
    <footer
      data-testid="footer-section"
      className="bg-base-200 text-base-content py-10 px-4"
    >
      <div className="container mx-auto">
        <div className="flex flex-col md:flex-row justify-between items-center gap-8">
          {/* Logo */}
          <div data-testid="footer-logo" className="flex items-center gap-2">
            <svg
              className="w-8 h-8 text-primary"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              strokeWidth="2"
              strokeLinecap="round"
              strokeLinejoin="round"
            >
              <path d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71" />
              <path d="M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71" />
            </svg>
            <span className="text-xl font-bold">LinkShort</span>
          </div>

          {/* Quick Links */}
          <nav data-testid="footer-links" className="flex flex-wrap gap-6 justify-center">
            <Link to="/" className="link link-hover">
              Home
            </Link>
            <Link to="/register" className="link link-hover">
              Sign Up
            </Link>
            <Link to="/login" className="link link-hover">
              Login
            </Link>
          </nav>

          {/* Copyright */}
          <p data-testid="footer-copyright" className="text-sm text-base-content/60">
            Copyright {currentYear} LinkShort. All rights reserved.
          </p>
        </div>
      </div>
    </footer>
  )
}

export default Footer
