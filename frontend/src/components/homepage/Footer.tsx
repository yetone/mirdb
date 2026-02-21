/**
 * Footer Component.
 * Owner: Scenario 5 - CTA & Footer
 *
 * Expected behavior:
 * - Service name and tagline
 * - Links: Terms of Service, Privacy Policy, Contact, GitHub
 * - Copyright notice with current year
 * - Multi-column layout on desktop
 * - Proper external link attributes (noopener noreferrer)
 */

import { Link } from 'react-router-dom';
import { FOOTER_LINKS } from '../../utils/constants';

function Footer() {
  const currentYear = new Date().getFullYear();

  return (
    <footer
      className="bg-base-200 py-12 px-4"
      data-testid="footer"
    >
      <div className="container mx-auto max-w-7xl">
        <div className="flex flex-col md:flex-row justify-between items-center md:items-start gap-8">
          {/* Brand Section */}
          <div className="text-center md:text-left">
            <h3
              className="text-xl font-bold text-base-content mb-2"
              data-testid="footer-service-name"
            >
              URL Shortener
            </h3>
            <p
              className="text-base-content/80 max-w-xs"
              data-testid="footer-tagline"
            >
              Shorten links, track insights, and share your analytics with ease.
            </p>
          </div>

          {/* Links Section */}
          <nav
            className="flex flex-wrap justify-center md:justify-end gap-6"
            data-testid="footer-links"
            aria-label="Footer navigation"
          >
            {FOOTER_LINKS.map((link) =>
              link.isExternal ? (
                <a
                  key={link.label}
                  href={link.href}
                  className="text-base-content/80 hover:text-primary transition-colors"
                  target="_blank"
                  rel="noopener noreferrer"
                >
                  {link.label}
                </a>
              ) : (
                <Link
                  key={link.label}
                  to={link.href}
                  className="text-base-content/80 hover:text-primary transition-colors"
                >
                  {link.label}
                </Link>
              )
            )}
          </nav>
        </div>

        {/* Copyright Section */}
        <div className="mt-8 pt-8 border-t border-base-300 text-center">
          <p
            className="text-base-content/80 text-sm"
            data-testid="footer-copyright"
          >
            © {currentYear} URL Shortener. All rights reserved.
          </p>
        </div>
      </div>
    </footer>
  );
}

export default Footer;
