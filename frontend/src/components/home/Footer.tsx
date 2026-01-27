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
      className="bg-base-200 py-8 px-4"
      data-testid="footer"
      role="contentinfo"
    >
      <div className="max-w-6xl mx-auto">
        <div className="grid grid-cols-1 md:grid-cols-3 gap-8">
          {/* Navigation Links */}
          <div>
            <h3 className="font-semibold text-base-content mb-4">Navigation</h3>
            <nav aria-label="Footer navigation">
              <ul className="space-y-2">
                {navigationLinks.map((link) => (
                  <li key={link.href}>
                    <Link
                      to={link.href}
                      className="text-base-content/70 hover:text-primary transition-colors"
                      data-testid={`footer-nav-${link.label.toLowerCase()}`}
                    >
                      {link.label}
                    </Link>
                  </li>
                ))}
              </ul>
            </nav>
          </div>

          {/* Legal Links */}
          <div>
            <h3 className="font-semibold text-base-content mb-4">Legal</h3>
            <nav aria-label="Legal links">
              <ul className="space-y-2">
                {legalLinks.map((link) => (
                  <li key={link.href}>
                    <Link
                      to={link.href}
                      className="text-base-content/70 hover:text-primary transition-colors"
                      data-testid={`footer-legal-${link.label.toLowerCase().replace(/\s+/g, '-')}`}
                    >
                      {link.label}
                    </Link>
                  </li>
                ))}
              </ul>
            </nav>
          </div>

          {/* Copyright */}
          <div className="md:text-right">
            <p
              className="text-base-content/60 text-sm"
              data-testid="footer-copyright"
            >
              © {currentYear} URL Shortener. All rights reserved.
            </p>
          </div>
        </div>
      </div>
    </footer>
  )
}

export default Footer
