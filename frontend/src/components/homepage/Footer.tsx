/**
 * Footer component.
 * Owner: Scenario 6 - Footer Rendering
 *
 * Requirements:
 * - Product branding/logo
 * - Legal links: Privacy Policy, Terms of Service, Contact
 * - Copyright notice with current year
 * - Semantic footer element
 */

import { Link } from 'react-router-dom'
import type { FooterLink } from '../../types/homepage'

export interface FooterProps {
  brandName?: string
  links?: FooterLink[]
  showSocialLinks?: boolean
}

const defaultLinks: FooterLink[] = [
  { label: 'Privacy Policy', href: '/privacy' },
  { label: 'Terms of Service', href: '/terms' },
  { label: 'Contact', href: '/contact' },
]

const defaultProps: Required<Omit<FooterProps, 'showSocialLinks'>> & { showSocialLinks: boolean } = {
  brandName: 'URL Shortener',
  links: defaultLinks,
  showSocialLinks: false,
}

export function Footer(props: FooterProps = {}) {
  const { brandName, links, showSocialLinks } = { ...defaultProps, ...props }
  const currentYear = new Date().getFullYear()

  return (
    <footer
      className="footer footer-center p-10 bg-base-200 text-base-content"
      data-testid="footer"
    >
      <div className="flex flex-col items-center gap-4">
        {/* Product Branding */}
        <div data-testid="footer-branding" className="flex items-center gap-2">
          <svg
            xmlns="http://www.w3.org/2000/svg"
            className="h-6 w-6"
            fill="none"
            viewBox="0 0 24 24"
            stroke="currentColor"
            aria-hidden="true"
          >
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth={2}
              d="M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101m-.758-4.899a4 4 0 005.656 0l4-4a4 4 0 00-5.656-5.656l-1.1 1.1"
            />
          </svg>
          <span className="text-lg font-bold">{brandName}</span>
        </div>

        {/* Legal Links */}
        <nav
          className="flex flex-wrap justify-center gap-4"
          aria-label="Footer navigation"
          data-testid="footer-links"
        >
          {links.map((link) => (
            <Link
              key={link.href}
              to={link.href}
              className="link link-hover"
              data-testid={`footer-link-${link.label.toLowerCase().replace(/\s+/g, '-')}`}
            >
              {link.label}
            </Link>
          ))}
        </nav>

        {/* Social Links (Optional) */}
        {showSocialLinks && (
          <div className="flex gap-4" data-testid="footer-social">
            <a
              href="https://twitter.com"
              target="_blank"
              rel="noopener noreferrer"
              aria-label="Twitter"
              className="link link-hover"
            >
              <svg
                xmlns="http://www.w3.org/2000/svg"
                className="h-6 w-6"
                fill="currentColor"
                viewBox="0 0 24 24"
              >
                <path d="M24 4.557c-.883.392-1.832.656-2.828.775 1.017-.609 1.798-1.574 2.165-2.724-.951.564-2.005.974-3.127 1.195-.897-.957-2.178-1.555-3.594-1.555-3.179 0-5.515 2.966-4.797 6.045-4.091-.205-7.719-2.165-10.148-5.144-1.29 2.213-.669 5.108 1.523 6.574-.806-.026-1.566-.247-2.229-.616-.054 2.281 1.581 4.415 3.949 4.89-.693.188-1.452.232-2.224.084.626 1.956 2.444 3.379 4.6 3.419-2.07 1.623-4.678 2.348-7.29 2.04 2.179 1.397 4.768 2.212 7.548 2.212 9.142 0 14.307-7.721 13.995-14.646.962-.695 1.797-1.562 2.457-2.549z" />
              </svg>
            </a>
            <a
              href="https://github.com"
              target="_blank"
              rel="noopener noreferrer"
              aria-label="GitHub"
              className="link link-hover"
            >
              <svg
                xmlns="http://www.w3.org/2000/svg"
                className="h-6 w-6"
                fill="currentColor"
                viewBox="0 0 24 24"
              >
                <path d="M12 0c-6.626 0-12 5.373-12 12 0 5.302 3.438 9.8 8.207 11.387.599.111.793-.261.793-.577v-2.234c-3.338.726-4.033-1.416-4.033-1.416-.546-1.387-1.333-1.756-1.333-1.756-1.089-.745.083-.729.083-.729 1.205.084 1.839 1.237 1.839 1.237 1.07 1.834 2.807 1.304 3.492.997.107-.775.418-1.305.762-1.604-2.665-.305-5.467-1.334-5.467-5.931 0-1.311.469-2.381 1.236-3.221-.124-.303-.535-1.524.117-3.176 0 0 1.008-.322 3.301 1.23.957-.266 1.983-.399 3.003-.404 1.02.005 2.047.138 3.006.404 2.291-1.552 3.297-1.23 3.297-1.23.653 1.653.242 2.874.118 3.176.77.84 1.235 1.911 1.235 3.221 0 4.609-2.807 5.624-5.479 5.921.43.372.823 1.102.823 2.222v3.293c0 .319.192.694.801.576 4.765-1.589 8.199-6.086 8.199-11.386 0-6.627-5.373-12-12-12z" />
              </svg>
            </a>
          </div>
        )}

        {/* Copyright Notice */}
        <p data-testid="footer-copyright" className="text-sm">
          Copyright © {currentYear} {brandName}. All rights reserved.
        </p>
      </div>
    </footer>
  )
}

export default Footer
