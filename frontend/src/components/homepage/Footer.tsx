/**
 * Footer Component
 * Owner: Scenario 6 - Footer Component
 *
 * Displays footer with:
 * - Copyright information (current year)
 * - Privacy Policy link
 * - Terms of Service link
 * - Support/documentation links (if available)
 *
 * Requirements: REQ-6
 */

export interface FooterProps {
  className?: string
}

export function Footer({ className = '' }: FooterProps) {
  const currentYear = new Date().getFullYear()

  return (
    <footer
      data-testid="footer"
      className={`bg-base-200 py-8 mt-auto ${className}`}
    >
      <div className="container mx-auto px-4">
        <div className="flex flex-col md:flex-row justify-between items-center gap-4">
          <div data-testid="copyright" className="text-base-content/70 text-sm">
            &copy; {currentYear} URL Shortener. All rights reserved.
          </div>

          <nav
            data-testid="footer-links"
            className="flex flex-wrap gap-4 md:gap-6"
            aria-label="Footer navigation"
          >
            <a
              href="/privacy"
              data-testid="privacy-link"
              className="link link-hover text-sm text-base-content/70 hover:text-primary"
            >
              Privacy Policy
            </a>
            <a
              href="/terms"
              data-testid="terms-link"
              className="link link-hover text-sm text-base-content/70 hover:text-primary"
            >
              Terms of Service
            </a>
          </nav>
        </div>
      </div>
    </footer>
  )
}

export default Footer
