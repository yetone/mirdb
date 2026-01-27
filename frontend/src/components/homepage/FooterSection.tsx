/**
 * Footer Section Component
 * Owner: Scenario 8 - Footer Section
 *
 * Displays footer with:
 * - Navigation links: Home, Login, Register, Privacy Policy
 * - Copyright notice with current year
 *
 * Accessibility:
 * - Semantic <footer> element
 * - ARIA labels for navigation
 * - Proper link focus states
 */

import { Link } from 'react-router-dom'
import type { FooterLink } from '../../types/homepage'

const navigationLinks: FooterLink[] = [
  { label: 'Home', href: '/' },
  { label: 'Login', href: '/login' },
  { label: 'Register', href: '/register' },
  { label: 'Privacy Policy', href: '/privacy' },
]

export function FooterSection() {
  const currentYear = new Date().getFullYear()

  return (
    <footer className="bg-base-200 py-8 px-4 sm:px-6 lg:px-8 mt-auto">
      <div className="max-w-6xl mx-auto">
        <nav aria-label="Footer navigation" className="mb-6">
          <ul className="flex flex-wrap justify-center gap-4 sm:gap-8">
            {navigationLinks.map((link) => (
              <li key={link.href}>
                {link.external ? (
                  <a
                    href={link.href}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="text-base-content/70 hover:text-primary transition-colors duration-200 focus:outline-none focus:ring-2 focus:ring-primary focus:ring-offset-2 rounded"
                  >
                    {link.label}
                  </a>
                ) : (
                  <Link
                    to={link.href}
                    className="text-base-content/70 hover:text-primary transition-colors duration-200 focus:outline-none focus:ring-2 focus:ring-primary focus:ring-offset-2 rounded"
                  >
                    {link.label}
                  </Link>
                )}
              </li>
            ))}
          </ul>
        </nav>

        <div className="text-center text-base-content/50 text-sm">
          <p>© {currentYear} URL Shortener. All rights reserved.</p>
        </div>
      </div>
    </footer>
  )
}

export default FooterSection
