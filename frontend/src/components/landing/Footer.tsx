/**
 * Footer Component
 * Owner: Scenario 9 - Footer Section
 *
 * Displays the page footer with:
 * - Navigation links (Home, Login, Register)
 * - Copyright notice with current year
 *
 * Requirements: REQ-9
 */

import React from 'react'
import { Link } from 'react-router-dom'
import { FooterProps, FooterLink } from '../../types/landing'

const DEFAULT_LINKS: FooterLink[] = [
  { label: 'Home', href: '/' },
  { label: 'Login', href: '/login' },
  { label: 'Register', href: '/register' },
]

const CURRENT_YEAR = new Date().getFullYear()
const PRODUCT_NAME = 'URL Shortener'

export function Footer({ links = DEFAULT_LINKS }: FooterProps) {
  return (
    <footer
      data-testid="footer"
      className="bg-base-300 text-base-content py-8 px-4"
      role="contentinfo"
    >
      <div className="max-w-6xl mx-auto">
        <div className="flex flex-col md:flex-row justify-between items-center gap-4">
          {/* Navigation Links */}
          <nav
            data-testid="footer-nav"
            aria-label="Footer navigation"
            className="flex flex-wrap justify-center gap-4 md:gap-6"
          >
            {links.map((link) => (
              <Link
                key={link.href}
                to={link.href}
                data-testid={`footer-link-${link.label.toLowerCase()}`}
                className="text-base-content hover:text-primary transition-colors duration-200"
              >
                {link.label}
              </Link>
            ))}
          </nav>

          {/* Copyright Notice */}
          <p
            data-testid="footer-copyright"
            className="text-sm text-base-content/70"
          >
            © {CURRENT_YEAR} {PRODUCT_NAME}. All rights reserved.
          </p>
        </div>
      </div>
    </footer>
  )
}

export default Footer
