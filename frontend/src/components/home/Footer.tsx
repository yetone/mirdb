/**
 * Footer Component
 * Owner: Scenario 7 - Footer Section
 *
 * Purpose: Page footer with navigation and legal links.
 *
 * Expected sections:
 * - Navigation links: Home, Dashboard, Login, Register
 * - Legal links: Privacy Policy, Terms of Service
 * - Copyright notice
 */

import { Link } from 'react-router-dom'
import type { FooterLink } from '../../types/home'

const navigationLinks: FooterLink[] = [
  { label: 'Home', href: '/' },
  { label: 'Dashboard', href: '/dashboard' },
  { label: 'Login', href: '/login' },
  { label: 'Register', href: '/register' },
]

const legalLinks: FooterLink[] = [
  { label: 'Privacy Policy', href: '/privacy' },
  { label: 'Terms of Service', href: '/terms' },
]

export function Footer() {
  const currentYear = new Date().getFullYear()

  return (
    <footer
      className="bg-base-300 py-8 px-4 sm:px-6 lg:px-8"
      data-testid="footer"
      role="contentinfo"
    >
      <div className="max-w-6xl mx-auto">
        <div className="grid grid-cols-1 md:grid-cols-3 gap-8">
          {/* Brand Section */}
          <div className="space-y-4">
            <h3 className="text-lg font-semibold text-base-content">
              URL Shortener
            </h3>
            <p className="text-base-content/70 text-sm">
              Shorten URLs, track clicks, and grow smarter with our powerful
              link management platform.
            </p>
          </div>

          {/* Navigation Links */}
          <nav aria-label="Footer navigation">
            <h4 className="text-base font-semibold text-base-content mb-4">
              Navigation
            </h4>
            <ul className="space-y-2" data-testid="footer-nav-links">
              {navigationLinks.map((link) => (
                <li key={link.href}>
                  <Link
                    to={link.href}
                    className="text-base-content/70 hover:text-primary transition-colors text-sm"
                    data-testid={`footer-link-${link.label.toLowerCase().replaceAll(' ', '-')}`}
                  >
                    {link.label}
                  </Link>
                </li>
              ))}
            </ul>
          </nav>

          {/* Legal Links */}
          <nav aria-label="Legal links">
            <h4 className="text-base font-semibold text-base-content mb-4">
              Legal
            </h4>
            <ul className="space-y-2" data-testid="footer-legal-links">
              {legalLinks.map((link) => (
                <li key={link.href}>
                  <Link
                    to={link.href}
                    className="text-base-content/70 hover:text-primary transition-colors text-sm"
                    data-testid={`footer-link-${link.label.toLowerCase().replaceAll(' ', '-')}`}
                  >
                    {link.label}
                  </Link>
                </li>
              ))}
            </ul>
          </nav>
        </div>

        {/* Copyright Notice */}
        <div className="border-t border-base-content/10 mt-8 pt-6 text-center">
          <p
            className="text-base-content/60 text-sm"
            data-testid="footer-copyright"
          >
            © {currentYear} URL Shortener. All rights reserved.
          </p>
        </div>
      </div>
    </footer>
  )
}

export default Footer
