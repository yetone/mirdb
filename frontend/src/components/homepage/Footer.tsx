/**
 * Footer Component
 * Owner: Scenario 8 - Footer Section
 *
 * Displays the page footer with:
 * - Copyright notice with current year
 * - Optional: Social links, terms, privacy links
 * - Theme attribution if required
 *
 * Requirements covered:
 * - REQ-9: Footer with relevant links and information
 */

import { Link } from 'react-router-dom';

interface FooterLink {
  label: string;
  to: string;
  external?: boolean;
}

const footerLinks: FooterLink[] = [
  { label: 'Terms of Service', to: '/terms' },
  { label: 'Privacy Policy', to: '/privacy' },
  { label: 'Contact', to: '/contact' },
];

export function Footer() {
  const currentYear = new Date().getFullYear();

  return (
    <footer
      className="footer footer-center p-6 bg-base-200 text-base-content border-t border-base-300"
      data-testid="footer"
    >
      <nav className="flex flex-wrap justify-center gap-4 mb-4" aria-label="Footer navigation">
        {footerLinks.map((link) =>
          link.external ? (
            <a
              key={link.label}
              href={link.to}
              className="link link-hover text-base-content/70 hover:text-primary"
              target="_blank"
              rel="noopener noreferrer"
            >
              {link.label}
            </a>
          ) : (
            <Link
              key={link.label}
              to={link.to}
              className="link link-hover text-base-content/70 hover:text-primary"
            >
              {link.label}
            </Link>
          )
        )}
      </nav>
      <aside>
        <p className="text-base-content/60" data-testid="footer-copyright">
          Copyright © {currentYear} URL Shortener. All rights reserved.
        </p>
      </aside>
    </footer>
  );
}
