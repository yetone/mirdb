import { Link } from 'react-router-dom'
import './Footer.css'

export interface FooterLink {
  label: string
  href: string
}

export interface SocialLink {
  label: string
  href: string
  icon: string
}

export const FOOTER_NAV_LINKS: FooterLink[] = [
  { label: 'Home', href: '/' },
  { label: 'Features', href: '/features' },
  { label: 'About', href: '/about' },
  { label: 'Contact', href: '/contact' },
]

export const LEGAL_LINKS: FooterLink[] = [
  { label: 'Privacy Policy', href: '/privacy' },
  { label: 'Terms of Service', href: '/terms' },
]

export const SOCIAL_LINKS: SocialLink[] = [
  { label: 'Twitter', href: 'https://twitter.com', icon: '𝕏' },
  { label: 'LinkedIn', href: 'https://linkedin.com', icon: 'in' },
  { label: 'GitHub', href: 'https://github.com', icon: 'GH' },
]

export const CONTACT_INFO = {
  email: 'contact@example.com',
  phone: '+1 (555) 123-4567',
  address: '123 Main Street, Suite 100, City, State 12345',
}

export const Footer: React.FC = () => {
  return (
    <footer className="footer" data-testid="footer">
      <div className="footer-content">
        {/* Site Map Section */}
        <div className="footer-section">
          <h3 className="footer-section-title">Site Map</h3>
          <nav aria-label="Footer navigation">
            <ul className="footer-links">
              {FOOTER_NAV_LINKS.map((link) => (
                <li key={link.href}>
                  <Link to={link.href} className="footer-link">
                    {link.label}
                  </Link>
                </li>
              ))}
            </ul>
          </nav>
        </div>

        {/* Legal Links Section */}
        <div className="footer-section">
          <h3 className="footer-section-title">Legal</h3>
          <ul className="footer-links">
            {LEGAL_LINKS.map((link) => (
              <li key={link.href}>
                <Link to={link.href} className="footer-link">
                  {link.label}
                </Link>
              </li>
            ))}
          </ul>
        </div>

        {/* Social Links Section */}
        <div className="footer-section">
          <h3 className="footer-section-title">Follow Us</h3>
          <ul className="footer-social-links">
            {SOCIAL_LINKS.map((link) => (
              <li key={link.href}>
                <a
                  href={link.href}
                  className="footer-social-link"
                  target="_blank"
                  rel="noopener noreferrer"
                  aria-label={link.label}
                >
                  <span className="social-icon" aria-hidden="true">
                    {link.icon}
                  </span>
                </a>
              </li>
            ))}
          </ul>
        </div>

        {/* Contact Info Section */}
        <div className="footer-section">
          <h3 className="footer-section-title">Contact Us</h3>
          <address className="footer-contact">
            <a href={`mailto:${CONTACT_INFO.email}`} className="footer-link">
              {CONTACT_INFO.email}
            </a>
            <a href={`tel:${CONTACT_INFO.phone.replace(/\D/g, '')}`} className="footer-link">
              {CONTACT_INFO.phone}
            </a>
            <p className="footer-address">{CONTACT_INFO.address}</p>
          </address>
        </div>
      </div>

      {/* Copyright Section */}
      <div className="footer-bottom">
        <p className="footer-copyright">
          &copy; {new Date().getFullYear()} Homepage Enhancement. All rights reserved.
        </p>
      </div>
    </footer>
  )
}
