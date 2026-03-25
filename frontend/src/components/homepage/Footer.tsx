/**
 * Footer Component
 * Owner: Scenario 10 - Footer Section
 *
 * Page footer with:
 * - Terms of Service link
 * - Privacy Policy link
 * - Contact link
 * - Copyright notice
 *
 * Uses semantic <footer> element.
 */

import React from 'react';
import { Link } from 'react-router-dom';

export function Footer() {
  const currentYear = new Date().getFullYear();

  return (
    <footer
      className="bg-base-300 border-t border-base-content/10 py-8 px-4"
      data-testid="footer"
      role="contentinfo"
    >
      <div className="max-w-6xl mx-auto">
        <div className="flex flex-col md:flex-row justify-between items-center gap-6">
          <nav className="flex flex-wrap justify-center gap-6" aria-label="Footer navigation">
            <Link
              to="/terms"
              className="text-base-content/70 hover:text-primary transition-colors"
              data-testid="footer-terms-link"
            >
              Terms of Service
            </Link>
            <Link
              to="/privacy"
              className="text-base-content/70 hover:text-primary transition-colors"
              data-testid="footer-privacy-link"
            >
              Privacy Policy
            </Link>
            <Link
              to="/contact"
              className="text-base-content/70 hover:text-primary transition-colors"
              data-testid="footer-contact-link"
            >
              Contact
            </Link>
          </nav>
          <p
            className="text-base-content/70 text-sm"
            data-testid="footer-copyright"
          >
            &copy; {currentYear} URL Shortener. All rights reserved.
          </p>
        </div>
      </div>
    </footer>
  );
}
