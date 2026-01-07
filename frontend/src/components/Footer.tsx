import { Link } from 'react-router-dom'

export default function Footer() {
  const currentYear = new Date().getFullYear()

  return (
    <footer
      data-testid="footer"
      className="bg-base-200 border-t border-base-300"
    >
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <div className="flex flex-col md:flex-row justify-between items-center gap-6">
          <nav aria-label="Footer navigation" className="flex gap-6">
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

          <p className="text-base-content/60 text-sm">
            © {currentYear} URL Shortener. All rights reserved.
          </p>
        </div>
      </div>
    </footer>
  )
}
