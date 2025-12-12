import { useState, useEffect, useRef, useCallback } from 'react'
import { Link } from 'react-router-dom'
import { useAuth } from '../../context'
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

export const AUTH_NAV_LINKS: NavLink[] = [
  { label: 'Dashboard', href: '/dashboard' },
  { label: 'Account', href: '/account' },
]

export interface NavigationProps {
  showAuthLinks?: boolean
}

export const Navigation: React.FC<NavigationProps> = ({ showAuthLinks = true }) => {
  const [isMenuOpen, setIsMenuOpen] = useState(false)
  const menuToggleRef = useRef<HTMLButtonElement>(null)

  // Try to use auth context, but handle case where it's not available
  let isAuthenticated = false
  try {
    const auth = useAuth()
    isAuthenticated = auth.isAuthenticated
  } catch {
    // Auth context not available, treat as unauthenticated
    isAuthenticated = false
  }

  const toggleMenu = () => {
    setIsMenuOpen(!isMenuOpen)
  }

  const closeMenu = useCallback(() => {
    setIsMenuOpen(false)
  }, [])

  const closeMenuAndFocusToggle = useCallback(() => {
    setIsMenuOpen(false)
    // Return focus to the toggle button for accessibility
    menuToggleRef.current?.focus()
  }, [])

  // Handle Escape key to close mobile menu
  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape' && isMenuOpen) {
        closeMenuAndFocusToggle()
      }
    }

    document.addEventListener('keydown', handleKeyDown)
    return () => {
      document.removeEventListener('keydown', handleKeyDown)
    }
  }, [isMenuOpen, closeMenuAndFocusToggle])

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
          ref={menuToggleRef}
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
          {showAuthLinks && isAuthenticated && AUTH_NAV_LINKS.map((link) => (
            <li key={link.href}>
              <Link
                to={link.href}
                className="navigation-link navigation-link-auth"
                data-testid="nav-link-auth"
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
