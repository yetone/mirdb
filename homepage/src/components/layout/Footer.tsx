/**
 * Footer component for the MirDB homepage.
 * Owner: Scenario 7 - Footer Section
 *
 * Requirements:
 * - REQ-11: Links to GitHub, issue tracker, license
 * - NFR-6: External links with security attributes
 */

import { footerLinks } from '../../config/content'
import './Footer.css'

export function Footer() {
  const currentYear = new Date().getFullYear()

  return (
    <footer className="footer">
      <div className="footer__container container">
        <nav className="footer__links" aria-label="Footer navigation">
          {footerLinks.map((link) => (
            <a
              key={link.label}
              href={link.href}
              className="footer__link"
              target={link.external ? '_blank' : undefined}
              rel={link.external ? 'noopener noreferrer' : undefined}
            >
              {link.label}
            </a>
          ))}
        </nav>

        <div className="footer__copyright">
          <p>© {currentYear} MirDB. MIT License.</p>
        </div>
      </div>
    </footer>
  )
}
