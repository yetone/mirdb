/**
 * Header component for the MirDB homepage.
 * Owner: Scenario 1 - Header and Navigation
 *
 * Requirements:
 * - REQ-1: Display MirDB logo prominently
 * - REQ-2: Navigation menu with Documentation, Examples, GitHub, About
 * - REQ-12: Theme toggle button
 */

import { useState } from 'react'
import { Navigation } from './Navigation'
import { MobileMenu, HamburgerButton } from './MobileMenu'
import { ThemeToggle } from '../ui/ThemeToggle'
import { navItems } from '../../config/content'
import './Header.css'

export function Header() {
  const [isMobileMenuOpen, setIsMobileMenuOpen] = useState(false)

  const toggleMobileMenu = () => {
    setIsMobileMenuOpen((prev) => !prev)
  }

  const closeMobileMenu = () => {
    setIsMobileMenuOpen(false)
  }

  return (
    <header className="header">
      <div className="header__container container">
        <a href="/" className="header__logo-link" aria-label="MirDB Home">
          <img
            src="/assets/logo.gif"
            alt="MirDB"
            className="header__logo"
          />
        </a>

        <div className="header__nav-wrapper">
          <Navigation items={navItems} />
        </div>

        <div className="header__actions">
          <ThemeToggle />
          <HamburgerButton isOpen={isMobileMenuOpen} onClick={toggleMobileMenu} />
        </div>
      </div>

      <MobileMenu
        items={navItems}
        isOpen={isMobileMenuOpen}
        onClose={closeMobileMenu}
      />
    </header>
  )
}
