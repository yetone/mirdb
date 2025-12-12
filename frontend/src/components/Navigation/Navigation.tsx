import { useState } from 'react'
import { Link } from 'react-router-dom'
import './Navigation.css'

export interface NavLink {
  label: string
  href: string
}

export const NAV_LINKS: NavLink[] = [
  { label: 'Home', href: '/' },
  { label: 'Features', href: '/features' },
  { label: 'About', href: '/about' },
  { label: 'Contact', href: '/contact' },
]

export const Navigation: React.FC = () => {
  const [isMenuOpen, setIsMenuOpen] = useState(false)

  const toggleMenu = () => {
    setIsMenuOpen(!isMenuOpen)
  }

  const closeMenu = () => {
    setIsMenuOpen(false)
  }

  return (
    <header className="navigation-header" data-testid="main-navigation">
      <nav className="navigation" data-testid="navigation" aria-label="Main navigation">
        <Link
          to="/"
          className="navigation-logo"
          data-testid="navigation-logo"
          onClick={closeMenu}
        >
          Homepage Enhancement
        </Link>

        <button
          className="mobile-menu-toggle"
          data-testid="mobile-menu-toggle"
          onClick={toggleMenu}
          aria-label={isMenuOpen ? 'Close navigation menu' : 'Open navigation menu'}
          aria-expanded={isMenuOpen}
          aria-controls="navigation-links"
        >
          <span className="hamburger-icon" aria-hidden="true">
            <span className={`hamburger-line ${isMenuOpen ? 'open' : ''}`}></span>
            <span className={`hamburger-line ${isMenuOpen ? 'open' : ''}`}></span>
            <span className={`hamburger-line ${isMenuOpen ? 'open' : ''}`}></span>
          </span>
        </button>

        <ul
          className={`navigation-links ${isMenuOpen ? 'open' : ''}`}
          data-testid="navigation-links"
          id="navigation-links"
        >
          {NAV_LINKS.map((link) => (
            <li key={link.href}>
              <Link
                to={link.href}
                className="navigation-link"
                data-testid={`navigation-link-${link.label.toLowerCase()}`}
                onClick={closeMenu}
              >
                {link.label}
              </Link>
            </li>
          ))}
        </ul>
      </nav>
    </header>
  )
}
